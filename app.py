"""
Nimbus Admin Auto-ReSync — Production Web App
Deployable to Railway / Render / Docker / any WSGI host.

Environment variables:
  SECRET_KEY  — Flask session secret (set in production!)
  PORT        — HTTP port (default 5050)
  LOCAL_DEV   — set to "1" to auto-open browser on startup
"""
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests as req
from flask import Flask, Response, jsonify, request, session, send_from_directory

Path("logs").mkdir(exist_ok=True)
Path("static").mkdir(exist_ok=True)

BASE    = "https://admin.nimbusgroup.us"
PORT    = int(os.environ.get("PORT", 5050))
SECRET  = os.environ.get("SECRET_KEY", "nimbus-resync-2026-change-me")

app = Flask(__name__, static_folder="static")
app.secret_key = SECRET
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=os.environ.get("HTTPS") == "1",
)

# ── Per-session state store ───────────────────────────────────────────────────
_store: dict = {}
_store_lock = threading.Lock()


def _evict_old():
    """Remove sessions idle for more than 8 hours."""
    while True:
        time.sleep(1800)
        cutoff = datetime.now() - timedelta(hours=8)
        with _store_lock:
            dead = [k for k, v in _store.items()
                    if v.get("seen", datetime.now()) < cutoff]
            for k in dead:
                del _store[k]


threading.Thread(target=_evict_old, daemon=True).start()


def _sess() -> dict:
    sid = session.get("sid")
    if not sid:
        sid = str(uuid.uuid4())
        session["sid"] = sid
    with _store_lock:
        if sid not in _store:
            _store[sid] = {
                "token":    None,
                "username": "",
                "status":   "idle",   # idle | scanning | scanned | resyncing | done | error
                "ships":    [],        # [{sn, sid, failed, state, done, total}]
                "stats":    _empty_stats(),
                "q":        queue.Queue(maxsize=2000),
                "stop":     threading.Event(),
                "ship_stops": {},      # sn -> threading.Event for per-ship cancel
                "seen":     datetime.now(),
            }
        else:
            _store[sid]["seen"] = datetime.now()
    return _store[sid]


def _empty_stats() -> dict:
    return {"found": 0, "ok": 0, "fail": 0, "total_ev": 0, "done_ev": 0}

# ── Nimbus API helper ─────────────────────────────────────────────────────────

def _nimbus(token: str, path: str, payload: dict, timeout: int = 30) -> dict:
    r = req.post(
        f"{BASE}{path}",
        json=payload,
        timeout=timeout,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json, text/plain, */*",
            "Origin":        BASE,
            "Referer":       f"{BASE}/shipment-sync/shipment-sync",
        },
    )
    r.raise_for_status()
    return r.json()

# ── Session event helpers ─────────────────────────────────────────────────────

def _bc(s: dict, data: dict):
    try:
        s["q"].put_nowait(data)
    except queue.Full:
        pass


def _log(s: dict, msg: str, level: str = "info"):
    _bc(s, {"type": "log", "ts": datetime.now().strftime("%H:%M:%S"),
             "level": level, "message": msg})


def _push_status(s: dict, v: str):
    s["status"] = v
    _bc(s, {"type": "status", "value": v})


def _push_ship(s: dict, sn: str, failed, state: str, done: int = 0, total: int = 0, errors: list = None):
    for item in s["ships"]:
        if item["sn"] == sn:
            item["state"] = state
            item["done"]  = done
            item["total"] = total
            if errors is not None:
                item["errors"] = errors
            break
    _bc(s, {"type": "ship", "sn": sn, "failed": failed,
             "state": state, "done": done, "total": total,
             "errors": errors or []})


def _push_stat(s: dict):
    _bc(s, {"type": "stat", **s["stats"]})

# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/stream")
def api_stream():
    s = _sess()

    def _gen():
        while True:
            try:
                ev = s["q"].get(timeout=25)
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'

    return Response(
        _gen(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/me")
def api_me():
    s = _sess()
    return jsonify({
        "logged_in": bool(s["token"]),
        "username":  s["username"],
        "status":    s["status"],
        "stats":     s["stats"],
        "ships":     s["ships"],
    })


@app.route("/api/login", methods=["POST"])
def api_login():
    s    = _sess()
    data = request.json or {}
    user = (data.get("username") or "").strip()
    pw   = (data.get("password") or "").strip()

    if not user or not pw:
        return jsonify({"ok": False, "error": "账号和密码不能为空"}), 400

    # Primary login endpoint (confirmed from KPI dashboard project)
    LOGIN_ATTEMPTS = [
        ("https://nimbusgroup.us",       "/api/v2/userauth/auth/sign-in", {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/sign-in", {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/login",   {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/admin/auth/login",         {"username": user, "password": pw}),
    ]
    for base, path, body in LOGIN_ATTEMPTS:
        try:
            r = req.post(f"{base}{path}", json=body, timeout=10,
                         headers={"Content-Type": "application/json"})
            if r.ok:
                d   = r.json()
                tok = ((d.get("data") or {}).get("token") or
                       d.get("token") or d.get("access_token"))
                if tok:
                    s["token"]    = tok
                    s["username"] = user
                    _log(s, f"登录成功：{user}", "success")
                    return jsonify({"ok": True, "username": user})
        except Exception:
            pass

    return jsonify({"ok": False,
                    "error": "登录失败，请检查账号密码，或使用 Token 直接登录"}), 401


@app.route("/api/token", methods=["POST"])
def api_set_token():
    s   = _sess()
    tok = ((request.json or {}).get("token") or "").strip()
    if not tok:
        return jsonify({"ok": False, "error": "Token 不能为空"}), 400
    s["token"]    = tok
    s["username"] = "Token 用户"
    _log(s, "Token 设置成功", "success")
    return jsonify({"ok": True, "username": "Token 用户"})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    s = _sess()
    s["token"]    = None
    s["username"] = ""
    s["status"]   = "idle"
    s["ships"]    = []
    s["stats"]    = _empty_stats()
    return jsonify({"ok": True})


@app.route("/api/scan", methods=["POST"])
def api_scan():
    s = _sess()
    if not s["token"]:
        return jsonify({"ok": False, "error": "请先登录"}), 401
    s["ships"] = []
    s["stats"] = _empty_stats()
    _push_status(s, "scanning")
    threading.Thread(target=_scan_worker, args=(s,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/resync", methods=["POST"])
def api_resync():
    s = _sess()
    if not s["token"]:
        return jsonify({"ok": False, "error": "请先登录"}), 401
    if not s["ships"]:
        return jsonify({"ok": False, "error": "请先扫描"}), 400
    _push_status(s, "resyncing")
    threading.Thread(target=_resync_worker, args=(s,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/resync_one", methods=["POST"])
def api_resync_one():
    s  = _sess()
    if not s["token"]:
        return jsonify({"ok": False, "error": "请先登录"}), 401
    sn = ((request.json or {}).get("sn") or "").strip()
    ship = next((x for x in s["ships"] if x["sn"] == sn), None)
    if not ship:
        return jsonify({"ok": False, "error": f"找不到 {sn}"}), 404
    if s["status"] == "resyncing":
        return jsonify({"ok": False, "error": "全部 ReSync 正在进行中，请等待或先停止"}), 400
    threading.Thread(target=_resync_one_ship_standalone, args=(s, ship), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    s = _sess()
    s["stop"].set()
    _log(s, "操作已停止", "warning")
    return jsonify({"ok": True})


@app.route("/api/stop_one", methods=["POST"])
def api_stop_one():
    s  = _sess()
    sn = ((request.json or {}).get("sn") or "").strip()
    ev = s["ship_stops"].get(sn)
    if ev:
        ev.set()
        _log(s, f"{sn} 已取消", "warning")
    return jsonify({"ok": True})


@app.route("/api/add_manual", methods=["POST"])
def api_add_manual():
    s = _sess()
    if not s["token"]:
        return jsonify({"ok": False, "error": "请先登录"}), 401
    sn = ((request.json or {}).get("sn") or "").strip()
    if not sn:
        return jsonify({"ok": False, "error": "请输入 Shipment Number"})

    token = s["token"]
    found_sid = None
    error_count = 0

    # Attempt 1: query parcelTrack-event with shipmentNumber filter
    try:
        page, total_pages = 1, 9999
        while page <= total_pages:
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                           {"shipmentNumber": sn, "operationResult": "ERROR",
                            "current": page, "pageSize": 100})
            if not resp.get("success"):
                break
            total_pages = resp.get("totalPages") or 1
            items = resp.get("data") or []
            for item in items:
                if not found_sid:
                    found_sid = str(item.get("shipmentId") or "").strip()
                error_count += 1
            if not items:
                break
            page += 1
    except Exception:
        pass

    # Attempt 2: globalSearch → get shipmentId → query parcelTrack-event
    if not found_sid:
        try:
            page, total_pages = 1, 9999
            while page <= total_pages and not found_sid:
                resp = _nimbus(token,
                               "/api/admin/operate/tenant-sync/shipment-event/globalSearch",
                               {"shipmentNumber": sn, "current": page, "pageSize": 20})
                if not resp.get("success"):
                    break
                total_pages = resp.get("totalPages") or 1
                for item in (resp.get("data") or []):
                    if (item.get("shipmentNumber") or "").strip() == sn:
                        found_sid = str(item.get("shipmentId") or "").strip()
                        break
                page += 1

            if found_sid:
                page, total_pages, error_count = 1, 9999, 0
                while page <= total_pages:
                    resp = _nimbus(token,
                                   "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                                   {"shipmentId": found_sid, "operationResult": "ERROR",
                                    "current": page, "pageSize": 100})
                    if not resp.get("success"):
                        break
                    total_pages = resp.get("totalPages") or 1
                    items = resp.get("data") or []
                    error_count += len(items)
                    if not items:
                        break
                    page += 1
        except Exception:
            pass

    if not found_sid:
        return jsonify({"ok": False, "error": f"找不到 Shipment：{sn}"})
    if error_count == 0:
        return jsonify({"ok": False, "error": f"{sn} 没有 ERROR 记录"})

    ship = {"sn": sn, "sid": found_sid, "failed": error_count,
            "state": "found", "done": 0, "total": 0}
    idx = next((i for i, x in enumerate(s["ships"]) if x["sn"] == sn), -1)
    if idx >= 0:
        s["ships"][idx] = ship
    else:
        s["ships"].append(ship)

    s["stats"]["found"] = len(s["ships"])
    _bc(s, {"type": "ship", "sn": sn, "failed": error_count,
             "state": "found", "done": 0, "total": 0})
    _push_stat(s)
    _log(s, f"手动添加 {sn}：{error_count} 条 ERROR 事件", "warning")
    return jsonify({"ok": True, "sn": sn, "failed": error_count})


# ── Background: scan ──────────────────────────────────────────────────────────

def _scan_worker(s: dict):
    s["stop"].clear()
    token = s["token"]
    found_map = {}   # sid -> ship dict

    try:
        page = 1
        total_pages = 9999

        while page <= total_pages and not s["stop"].is_set():
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                           {"operationResult": "ERROR", "current": page, "pageSize": 100})

            if not resp.get("success"):
                _log(s, f"API 错误: {resp.get('msg')}", "error")
                break

            total_pages = resp.get("totalPages") or 1
            if page == 1:
                _log(s, f"共 {resp.get('total')} 个失败事件，{total_pages} 页，正在扫描…")

            for item in (resp.get("data") or []):
                sid = str(item.get("shipmentId") or "").strip()
                sn  = (item.get("shipmentNumber") or "").strip()
                if not sid:
                    continue
                if sid in found_map:
                    found_map[sid]["failed"] += 1
                else:
                    found_map[sid] = {"sn": sn or sid, "sid": sid, "failed": 1,
                                      "state": "found", "done": 0, "total": 0}

            _bc(s, {"type": "scan_progress", "page": page, "total_pages": total_pages,
                    "found": len(found_map)})
            page += 1

        # Send final ship list with accurate counts
        found = list(found_map.values())
        for ship in found:
            _bc(s, {"type": "ship", "sn": ship["sn"], "failed": ship["failed"],
                    "state": "found", "done": 0, "total": 0})

        s["ships"] = found
        s["stats"]["found"] = len(found)
        _push_stat(s)

        if found:
            _log(s, f"扫描完成：发现 {len(found)} 个 Shipment 存在 HAWB 失败", "warning")
        else:
            _log(s, "扫描完成：未发现 HAWB 失败的 Shipment ✓", "success")

        _push_status(s, "scanned")
        _bc(s, {"type": "scan_done", "count": len(found)})

    except Exception as exc:
        _log(s, f"扫描出错: {exc}", "error")
        _push_status(s, "error")

# ── Background: resync ────────────────────────────────────────────────────────

def _resync_one_ship_standalone(s: dict, ship: dict):
    sn    = ship["sn"]
    sid   = ship["sid"]
    token = s["token"]
    stop_ev = threading.Event()
    s["ship_stops"][sn] = stop_ev
    _push_ship(s, sn, ship["failed"], "running")
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("logs") / f"results_{sn}_{ts}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "shipment_number", "event_id",
                    "operate_action", "result", "message"])
        ok, fail, errs = _resync_one_ship(s, token, sn, sid, w, stop_ev)
    cancelled = stop_ev.is_set()
    s["ship_stops"].pop(sn, None)
    if cancelled:
        state = "cancelled"
        _log(s, f"{sn} 已取消  已完成: {ok}", "warning")
    elif fail == 0:
        state = "done"
        _log(s, f"{sn} 完成 ✓  成功: {ok}", "success")
    else:
        state = "partial" if ok > 0 else "error"
        _log(s, f"{sn} 完成  成功: {ok}  失败: {fail}", "warning")
    _push_ship(s, sn, ship["failed"], state, ok, ok + fail, errs)
    s["stats"]["ok"]   += ok
    s["stats"]["fail"] += fail
    _push_stat(s)


def _resync_worker(s: dict):
    s["stop"].clear()
    token = s["token"]

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("logs") / f"results_{ts}.csv"

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "shipment_number", "event_id",
                    "operate_action", "result", "message"])

        for ship in s["ships"]:
            if s["stop"].is_set():
                break
            sn  = ship["sn"]
            sid = ship["sid"]
            stop_ev = threading.Event()
            s["ship_stops"][sn] = stop_ev
            _push_ship(s, sn, ship["failed"], "running")

            ok, fail, errs = _resync_one_ship(s, token, sn, sid, w, stop_ev)
            cancelled = stop_ev.is_set()
            s["ship_stops"].pop(sn, None)

            state = ("cancelled" if cancelled
                     else "done" if fail == 0
                     else "partial" if ok > 0 else "error")
            _push_ship(s, sn, ship["failed"], state, ok, ok + fail, errs)
            s["stats"]["ok"]   += ok
            s["stats"]["fail"] += fail
            _push_stat(s)

    _log(s,
         f"全部完成 ✓  ReSync 成功: {s['stats']['ok']}  失败: {s['stats']['fail']}  "
         f"结果已保存: {csv_path}",
         "success")
    _push_status(s, "done")
    _bc(s, {"type": "done"})


def _resync_one_ship(s: dict, token: str, sn: str, sid: str, w,
                     ship_stop: "threading.Event | None" = None) -> tuple:
    ok = fail = 0
    err_msgs: list = []
    total_logged = False
    attempted_ids: set = set()   # guard against infinite loop on permanently-failing events

    def _stopped():
        return s["stop"].is_set() or (ship_stop is not None and ship_stop.is_set())

    while not _stopped():
        try:
            # Always fetch page 1 — replayed events leave the ERROR list,
            # so incrementing the page cursor would skip items.
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                           {"shipmentId": sid, "operationResult": "ERROR",
                            "current": 1, "pageSize": 100})
        except Exception as exc:
            _log(s, f"{sn} 获取事件失败: {exc}", "error")
            break

        if not resp.get("success"):
            _log(s, f"{sn} 错误: {resp.get('msg')}", "error")
            break

        if not total_logged:
            tc = resp.get("total") or 0
            _log(s, f"  {sn}: {tc} 条失败事件")
            s["stats"]["total_ev"] += tc
            _push_stat(s)
            total_logged = True

        events = resp.get("data") or []
        if not events:
            break   # no more ERROR events

        # Stop if every event in this page was already attempted (all permanently failing)
        new_events = [ev for ev in events if (ev.get("id") or 0) not in attempted_ids]
        if not new_events:
            break
        for ev in new_events:
            attempted_ids.add(ev.get("id") or 0)

        if _stopped():
            return ok, fail, err_msgs

        with ThreadPoolExecutor(max_workers=15) as pool:
            future_map = {
                pool.submit(_replay, token, ev.get("id") or 0): ev
                for ev in new_events
            }
            for fut in as_completed(future_map):
                ev      = future_map[fut]
                ev_id   = ev.get("eventId") or ""
                action  = ev.get("operateAction") or ""
                success, msg = fut.result()
                w.writerow([datetime.now().isoformat(timespec="seconds"),
                            sn, ev_id, action, "ok" if success else "fail", msg])
                if success:
                    ok += 1
                else:
                    fail += 1
                    if msg and len(err_msgs) < 5:
                        err_msgs.append(f"{action}: {msg}")
                s["stats"]["done_ev"] += 1
                _push_stat(s)

    return ok, fail, err_msgs


def _replay(token: str, ev_num: int) -> tuple:
    try:
        resp = _nimbus(token,
                       "/api/admin/operate/tenant-sync/parcelTrack-event/replay",
                       {"id": ev_num}, timeout=15)
        msg = resp.get("msg") or ""
        return (resp.get("success") is True or resp.get("code") == 0), msg
    except Exception as exc:
        return False, str(exc)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if os.environ.get("LOCAL_DEV") == "1":
        import webbrowser, threading as _t
        _t.Timer(1.2, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
