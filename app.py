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
import re
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
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
)

# ── Per-session state store ───────────────────────────────────────────────────
_store: dict = {}
_store_lock = threading.Lock()

# ── Global operation log (shared across all sessions) ─────────────────────────
_OP_LOG_FILE = Path("logs") / "op_log.jsonl"
_op_log: list = []
_op_log_lock  = threading.Lock()

def _load_op_log():
    if _OP_LOG_FILE.exists():
        with open(_OP_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        _op_log.append(json.loads(line))
                    except Exception:
                        pass

_load_op_log()

def _append_op_log(entry: dict):
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    with _op_log_lock:
        _op_log.append(entry)
        # Purge entries older than 30 days (ts format: "YYYY-MM-DD HH:MM:SS")
        kept = [e for e in _op_log if (e.get("ts") or "") >= cutoff]
        if len(kept) != len(_op_log):
            _op_log[:] = kept
            try:
                with open(_OP_LOG_FILE, "w", encoding="utf-8") as f:
                    for e in kept:
                        f.write(json.dumps(e, ensure_ascii=False) + "\n")
            except Exception:
                pass
        else:
            try:
                with open(_OP_LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            except Exception:
                pass


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
    session.permanent = True
    with _store_lock:
        if sid not in _store:
            # Restore token/username from signed cookie so server restarts don't log users out
            _store[sid] = {
                "token":    session.get("_tok"),
                "username": session.get("_usr", ""),
                "status":   "idle",
                "ships":    [],
                "stats":    _empty_stats(),
                "q":        queue.Queue(maxsize=8000),
                "stop":     threading.Event(),
                "ship_stops": {},
                "seen":     datetime.now(),
                "_perm_ok": False,  # needs re-check after cookie restore
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
    if r.status_code == 401:
        raise PermissionError("Token 已过期，请重新登录")
    r.raise_for_status()
    return r.json()

def _check_permission(tok: str) -> str:
    """Return an error message if the token lacks replay permission, else empty string."""
    hdrs = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    results: dict = {}

    def _check_read():
        try:
            r = req.post(f"{BASE}/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                         json={"current": 1, "pageSize": 1}, timeout=6, headers=hdrs)
            results["read"] = r.status_code
        except Exception:
            results["read"] = None

    def _check_write():
        try:
            r = req.post(f"{BASE}/api/admin/operate/tenant-sync/parcelTrack-event/replay",
                         json={"id": 0}, timeout=6, headers=hdrs)
            results["write"] = r.status_code
        except Exception:
            results["write"] = None

    t1 = threading.Thread(target=_check_read, daemon=True)
    t2 = threading.Thread(target=_check_write, daemon=True)
    t1.start(); t2.start()
    t1.join(); t2.join()

    if results.get("read") == 401:
        return "Token 已过期或无效，请重新登录"
    if results.get("read") == 403:
        return "该账号没有 Nimbus Admin 操作权限，请使用有管理员权限的账号登录"
    if results.get("write") == 403:
        return "该账号没有 ReSync 操作权限，请使用有管理员权限的账号登录"
    return ""


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


def _push_stat(s: dict, force: bool = False):
    now = time.time()
    if not force and now - s.get("_last_stat_t", 0) < 0.4:
        return
    s["_last_stat_t"] = now
    _bc(s, {"type": "stat", **s["stats"]})

# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/stream")
def api_stream():
    s = _sess()
    # Assign a unique ID to this connection; supersedes any prior connection.
    conn_id = time.time()
    s["_sse_conn"] = conn_id

    def _gen():
        # Send current status so the UI indicator is correct on reconnect.
        cur = s.get("status", "idle")
        if cur != "idle":
            yield f"data: {json.dumps({'type':'status','value':cur}, ensure_ascii=False)}\n\n"

        while s.get("_sse_conn") == conn_id:
            try:
                ev = s["q"].get(timeout=20)
                # If we've been superseded, discard the event and stop.
                if s.get("_sse_conn") != conn_id:
                    break
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
            except queue.Empty:
                if s.get("_sse_conn") != conn_id:
                    break
                yield 'data: {"type":"ping"}\n\n'

    return Response(
        _gen(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/me")
def api_me():
    s = _sess()
    # Re-validate permission once after cookie restore (runs only on first /api/me per session)
    if s.get("token") and not s.get("_perm_ok"):
        err = _check_permission(s["token"])
        if err:
            s["token"] = None
            s["username"] = ""
            session.pop("_tok", None)
            session.pop("_usr", None)
        s["_perm_ok"] = True
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

    LOGIN_ATTEMPTS = [
        ("https://nimbusgroup.us",       "/api/v2/userauth/auth/sign-in", {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/sign-in", {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/login",   {"email": user, "password": pw}),
        ("https://admin.nimbusgroup.us", "/api/admin/auth/login",         {"username": user, "password": pw}),
    ]

    token_found: list = []   # [tok] once any attempt succeeds

    def _try_login(base, path, body):
        try:
            r = req.post(f"{base}{path}", json=body, timeout=6,
                         headers={"Content-Type": "application/json"})
            if r.ok:
                d   = r.json()
                tok = ((d.get("data") or {}).get("token") or
                       d.get("token") or d.get("access_token"))
                if tok and not token_found:
                    token_found.append(tok)
        except Exception:
            pass

    threads = [threading.Thread(target=_try_login, args=(b, p, bd), daemon=True)
               for b, p, bd in LOGIN_ATTEMPTS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if not token_found:
        return jsonify({"ok": False, "error": "账号或密码错误，请重试"}), 401

    tok = token_found[0]
    perm_err = _check_permission(tok)
    if perm_err:
        return jsonify({"ok": False, "error": perm_err}), 403
    s["token"]    = tok
    s["username"] = user
    s["_perm_ok"] = True
    session["_tok"] = tok
    session["_usr"] = user
    _log(s, f"登录成功：{user}", "success")
    return jsonify({"ok": True, "username": user})


@app.route("/api/token", methods=["POST"])
def api_set_token():
    s   = _sess()
    tok = ((request.json or {}).get("token") or "").strip()
    if not tok:
        return jsonify({"ok": False, "error": "Token 不能为空"}), 400
    perm_err = _check_permission(tok)
    if perm_err:
        return jsonify({"ok": False, "error": perm_err}), 403
    s["token"]    = tok
    s["username"] = "Token 用户"
    s["_perm_ok"] = True
    session["_tok"] = tok
    session["_usr"] = "Token 用户"
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
    session.pop("_tok", None)
    session.pop("_usr", None)
    return jsonify({"ok": True})


@app.route("/api/scan", methods=["POST"])
def api_scan():
    s = _sess()
    if not s["token"]:
        return jsonify({"ok": False, "error": "请先登录"}), 401
    # Keep manually-added ships; scan results will merge in
    s["ships"] = [sh for sh in s["ships"] if sh.get("manual")]
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
            "state": "found", "done": 0, "total": 0, "manual": True}
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

def _get_error_count(token: str, sid: str) -> int:
    """Return the number of current ERROR events for a shipment."""
    try:
        resp = _nimbus(token,
                       "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                       {"shipmentId": sid, "operationResult": "ERROR",
                        "current": 1, "pageSize": 1})
        if resp.get("success"):
            return resp.get("total") or 0
    except Exception:
        pass
    return 0


def _parse_nested(val):
    """Return val as dict if it's already a dict or a JSON string."""
    if isinstance(val, dict):
        return val
    if isinstance(val, str) and val.strip().startswith("{"):
        try:
            return json.loads(val)
        except Exception:
            pass
    return None


def _find_error_raw(ev: dict) -> str:
    """Extract error message text from a parcelTrack event."""
    # 1. Explicit top-level field names
    for field in ("errorMessage", "syncErrorMessage", "errorMsg", "syncError",
                  "errorContent", "errorDetail", "failureMessage", "failReason",
                  "errorInfo", "errorDesc", "message", "description"):
        v = ev.get(field)
        if isinstance(v, str) and len(v) > 8:
            return v
    # 2. Inside sourceTenantData / targetTenantData (JSON string or dict)
    for key in ("sourceTenantData", "targetTenantData", "targetTenantReq"):
        obj = _parse_nested(ev.get(key))
        if obj:
            for field in ("errorMessage", "errorMsg", "msg", "message",
                          "error", "remark", "failReason"):
                vv = obj.get(field)
                if isinstance(vv, str) and len(vv) > 8:
                    return vv
    # 3. Any top-level string that looks like an HTTP error
    for v in ev.values():
        if not isinstance(v, str) or len(v) < 20:
            continue
        vl = v.lower()
        if any(kw in vl for kw in ("[500]", "[400]", "[post]", "[get]",
                                    "exception", "internal server", "timeout",
                                    "connection refused")):
            return v
    return ""


def _extract_context(ev: dict) -> str:
    """Pull operation context (entry number, house, etc.) from event fields."""
    # targetTenantReq at top level or inside sourceTenantData
    for key in ("targetTenantReq",):
        obj = _parse_nested(ev.get(key))
        if obj:
            en = obj.get("entryNumber") or obj.get("entry_number")
            if en:
                return f"Entry# {en}"
            hn = obj.get("houseNumber") or obj.get("houseExtId")
            if hn:
                return f"House {str(hn)[:16]}"

    # customsChangeDetail inside sourceTenantData
    std = _parse_nested(ev.get("sourceTenantData"))
    if std:
        ccd = _parse_nested(std.get("customsChangeDetail")) or std.get("customsChangeDetail")
        if isinstance(ccd, dict):
            en = ccd.get("entryNumber") or ccd.get("entry_number")
            if en:
                return f"Entry# {en}"
        # targetTenantReq inside sourceTenantData
        req = _parse_nested(std.get("targetTenantReq"))
        if isinstance(req, dict):
            en = req.get("entryNumber")
            if en:
                return f"Entry# {en}"
    return ""


def _extract_error_key(raw: str) -> str:
    """Shorten a raw error string to its key description."""
    if not raw:
        return ""
    m = re.search(r'"msg"\s*:\s*"([^"]{2,120})"', raw)
    if m and m.group(1).lower() not in ("null", "none", ""):
        return m.group(1)
    m2 = re.search(r'\[(\d{3})\].*?/(\w{4,})\]', raw)
    if m2:
        return f"HTTP {m2.group(1)} — {m2.group(2)}"
    return raw.strip()[:100]


@app.route("/api/ship_reasons", methods=["POST"])
def api_ship_reasons():
    s = _sess()
    if not s.get("token"):
        return jsonify({"ok": False, "error": "未登录"}), 401
    data = request.get_json() or {}
    sn   = (data.get("sn") or "").strip()
    ship = next((sh for sh in s["ships"] if sh["sn"] == sn), None)
    if not ship:
        return jsonify({"ok": False, "error": "找不到"}), 404

    token  = s["token"]
    sid    = ship["sid"]
    # action -> {count, err_counts, ctx_counts}
    groups: dict = {}
    _debug_logged = False

    for page in range(1, 3):   # sample up to 200 events
        try:
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                           {"shipmentId": sid, "operationResult": "ERROR",
                            "current": page, "pageSize": 100})
            if not resp.get("success"):
                break
            events = resp.get("data") or []
            if not events:
                break
            # Log first event's keys once so we can identify error field names
            if not _debug_logged and events:
                first = events[0]
                print(f"[DEBUG ship_reasons] keys={list(first.keys())}", flush=True)
                for k, v in first.items():
                    if v is not None and v != "" and v != []:
                        print(f"  {k}: {str(v)[:120]}", flush=True)
                _debug_logged = True
            for ev in events:
                action    = (ev.get("operateAction") or "unknown").strip()
                raw_err   = _find_error_raw(ev)
                short_err = _extract_error_key(raw_err)
                ctx       = _extract_context(ev)
                if action not in groups:
                    groups[action] = {"count": 0, "err_counts": {}, "ctx_counts": {}}
                groups[action]["count"] += 1
                if short_err:
                    ec = groups[action]["err_counts"]
                    ec[short_err] = ec.get(short_err, 0) + 1
                if ctx:
                    cc = groups[action]["ctx_counts"]
                    cc[ctx] = cc.get(ctx, 0) + 1
        except Exception:
            break

    summary = []
    for action, info in sorted(groups.items(), key=lambda x: -x[1]["count"])[:3]:
        err_counts = info["err_counts"]
        ctx_counts = info["ctx_counts"]
        top_err = max(err_counts, key=err_counts.get) if err_counts else ""
        top_ctx = max(ctx_counts, key=ctx_counts.get) if ctx_counts else ""
        # Combine: "Entry# N123 — Internal Server Error" or fallback
        if top_err and top_ctx:
            msg = f"{top_ctx} — {top_err}"
        elif top_err:
            msg = top_err
        elif top_ctx:
            msg = top_ctx
        else:
            msg = ""
        summary.append({"action": action, "count": info["count"], "msg": msg})

    return jsonify({"ok": True, "summary": summary})


# ── SN info cache (clientName / branchCode) ──────────────────────────────────
_SN_CACHE_FILE = Path("logs") / "sn_info_cache.json"
_sn_cache: dict = {}          # sn -> {"clientName": str, "branchCode": str}
_sn_cache_lock = threading.Lock()
_dash_backfill_lock = threading.Lock()   # prevents overlapping background client-info backfills

def _load_sn_cache():
    if _SN_CACHE_FILE.exists():
        try:
            with open(_SN_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # keep entries that have at least created_at or clientName
            _sn_cache.update({k: v for k, v in data.items()
                               if v.get("clientName") or v.get("created_at")})
        except Exception:
            pass

_load_sn_cache()

def _save_sn_cache():
    try:
        with _sn_cache_lock:
            data = dict(_sn_cache)
        with open(_SN_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def _get_basic_info(token: str, sn: str) -> dict:
    try:
        r = req.get(
            f"{BASE}/api/admin/operate/shipment-basic/getShipmentBasicInfo",
            params={"shipmentNumber": sn},
            timeout=10,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{BASE}/shipment/basic-info",
            }
        )
        if r.ok:
            d = r.json().get("data") or {}
            if not d.get("clientName"):
                print(f"[dash-info] {sn}: HTTP {r.status_code} but no clientName in response data={d}", flush=True)
            return {
                "clientName": d.get("clientName") or "",
                "branchCode": d.get("branchCode") or "",
            }
        print(f"[dash-info] {sn}: HTTP {r.status_code} {r.text[:200]}", flush=True)
    except Exception as exc:
        print(f"[dash-info] {sn}: exception {exc!r}", flush=True)
    return {"clientName": "", "branchCode": ""}


@app.route("/api/dashboard_data")
def api_dashboard_data():
    s = _sess()
    if not s.get("token"):
        return jsonify({"ok": False, "error": "未登录"}), 401
    token = s["token"]

    # 3-month cutoff
    now = datetime.now()
    m, y = now.month - 2, now.year
    if m <= 0:
        m += 12
        y -= 1
    cutoff = f"{y:04d}-{m:02d}-01"

    changed = False
    try:
        page = 1
        total_pages = 9999
        while page <= total_pages:
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/shipment-event/globalSearch",
                           {"current": page, "pageSize": 50})
            if not resp.get("success"):
                break
            total_pages = resp.get("totalPages") or 1
            items = resp.get("data") or []
            if not items:
                break

            page_has_new = False
            hit_cutoff = False
            for item in items:
                sn = (item.get("shipmentNumber") or "").strip()
                et = item.get("eventTime") or ""
                ca = et.replace("T", " ")[:19] if "T" in et else et
                if not sn:
                    continue
                if ca and ca < cutoff:
                    # globalSearch returns newest-first: once we're past the
                    # dashboard's 3-month window, everything after is older still
                    hit_cutoff = True
                    break
                with _sn_cache_lock:
                    entry = _sn_cache.get(sn)
                    if entry is None:
                        # Brand new SN
                        _sn_cache[sn] = {"clientName": "", "branchCode": "", "created_at": ca}
                        changed = True
                        page_has_new = True
                    elif not entry.get("created_at"):
                        # Old cache entry missing created_at (pre-migration format)
                        _sn_cache[sn]["created_at"] = ca
                        changed = True
                        page_has_new = True
                    # else: fully known (has created_at) — skip

            # Stop once a full page is all known, or we've paged past the window
            if hit_cutoff or not page_has_new:
                break

            page += 1

    except PermissionError:
        return jsonify({"ok": False, "error": "token_expired"}), 401
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    # Clean entries older than 3-month cutoff
    with _sn_cache_lock:
        stale = [sn for sn, v in _sn_cache.items()
                 if v.get("created_at") and v["created_at"] < cutoff]
        if stale:
            for sn in stale:
                del _sn_cache[sn]
            changed = True

    if changed:
        _save_sn_cache()

    # Return 3-month data directly from cache. The daily chart only needs
    # created_at, so respond immediately instead of waiting on clientName/
    # branchCode lookups below (those trickle in on later refreshes).
    with _sn_cache_lock:
        cache_snap = dict(_sn_cache)

    result = [
        {
            "sn":         sn,
            "created_at": v["created_at"],
            "clientName": v.get("clientName", ""),
            "branchCode": v.get("branchCode", ""),
        }
        for sn, v in cache_snap.items()
        if v.get("created_at") and v["created_at"] >= cutoff
    ]
    result.sort(key=lambda x: x["created_at"], reverse=True)

    # Backfill clientName/branchCode for SNs missing it, in the background —
    # skip if a previous backfill is still running (e.g. hourly auto-refresh).
    need_info = [sn for sn, v in cache_snap.items()
                 if not v.get("clientName") and (v.get("created_at") or "") >= cutoff]

    if need_info and _dash_backfill_lock.acquire(blocking=False):
        def _backfill_client_info(sns):
            try:
                def _fetch_sn(sn):
                    info = _get_basic_info(token, sn)
                    if info.get("clientName"):
                        with _sn_cache_lock:
                            if sn in _sn_cache:
                                _sn_cache[sn]["clientName"] = info["clientName"]
                                _sn_cache[sn]["branchCode"] = info["branchCode"]
                        return True
                    return False

                # Save every 50 completions (not just at the end) so a pod
                # restart mid-backfill doesn't discard everything found so far.
                done = 0
                with ThreadPoolExecutor(max_workers=15) as pool:
                    futures = [pool.submit(_fetch_sn, sn) for sn in sns]
                    for fut in as_completed(futures):
                        try:
                            fut.result()
                        except Exception:
                            pass
                        done += 1
                        if done % 50 == 0:
                            _save_sn_cache()
                _save_sn_cache()
            finally:
                _dash_backfill_lock.release()

        threading.Thread(target=_backfill_client_info, args=(need_info,), daemon=True).start()

    return jsonify({"ok": True, "data": result})


@app.route("/api/logs")
def api_logs():
    s = _sess()
    if not s.get("token"):
        return jsonify({"ok": False, "error": "未登录"}), 401
    with _op_log_lock:
        entries = list(reversed(_op_log))   # most recent first
    return jsonify({"ok": True, "logs": entries[:500]})


def _scan_worker(s: dict):
    s["stop"].clear()
    token = s["token"]
    # Start with any manually-added ships already in the list
    found = list(s["ships"])
    existing_sns = {sh["sn"] for sh in found}
    all_ca: list = []   # all shipments scanned (for dashboard stats)

    try:
        page = 1
        total_pages = 9999

        # Step 1: globalSearch to discover shipments quickly
        while page <= total_pages and not s["stop"].is_set():
            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/shipment-event/globalSearch",
                           {"current": page, "pageSize": 50})

            if not resp.get("success"):
                _log(s, f"API 错误: {resp.get('msg')}", "error")
                break

            total_pages = resp.get("totalPages") or 1
            if page == 1:
                _log(s, f"共 {resp.get('total')} 个 Shipment，{total_pages} 页，正在扫描…")

            # All shipments on this page — verify in parallel (ignore stale failedCnt cache)
            candidates = []
            for item in (resp.get("data") or []):
                sid = str(item.get("shipmentId") or "").strip()
                sn  = (item.get("shipmentNumber") or "").strip()
                if sid and sn and sn not in existing_sns:
                    et = item.get("eventTime") or ""
                    ca = et.replace("T", " ")[:19] if "T" in et else et
                    candidates.append({"sn": sn, "sid": sid, "created_at": ca})
                    all_ca.append({"sn": sn, "created_at": ca})

            if candidates and not s["stop"].is_set():
                with ThreadPoolExecutor(max_workers=15) as pool:
                    fut_map = {pool.submit(_get_error_count, token, c["sid"]): c
                               for c in candidates}
                    for fut in as_completed(fut_map):
                        if s["stop"].is_set():
                            break
                        c = fut_map[fut]
                        try:
                            real_fc = fut.result()
                        except Exception:
                            real_fc = 0
                        if real_fc > 0:
                            ship = {"sn": c["sn"], "sid": c["sid"], "failed": real_fc,
                                    "state": "found", "done": 0, "total": 0,
                                    "created_at": c.get("created_at", "")}
                            found.append(ship)
                            existing_sns.add(c["sn"])
                            _bc(s, {"type": "ship", "sn": c["sn"], "failed": real_fc,
                                     "state": "found", "done": 0, "total": 0,
                                     "created_at": c.get("created_at", "")})

            _bc(s, {"type": "scan_progress", "page": page, "total_pages": total_pages,
                    "found": len(found)})
            page += 1

        s["ships"] = found
        s["stats"]["found"] = len(found)
        _push_stat(s)

        if found:
            _log(s, f"扫描完成：发现 {len(found)} 个 Shipment 存在 HAWB 失败", "warning")
        else:
            _log(s, "扫描完成：未发现 HAWB 失败的 Shipment ✓", "success")

        _push_status(s, "scanned")
        _bc(s, {"type": "all_ships_ca", "data": all_ca})
        _bc(s, {"type": "scan_done", "count": len(found)})

    except PermissionError as exc:
        s["token"] = None
        _bc(s, {"type": "need_login"})
        _push_status(s, "idle")
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
    _push_ship(s, sn, ship["failed"], "running", 0, ship["failed"])
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("logs") / f"results_{sn}_{ts}.csv"
    try:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "shipment_number", "event_id",
                        "operate_action", "result", "message"])
            ok, fail, errs = _resync_one_ship(s, token, sn, sid, ship["failed"], w, stop_ev)
    except PermissionError as exc:
        s["ship_stops"].pop(sn, None)
        s["token"] = None
        _bc(s, {"type": "need_login"})
        _push_ship(s, sn, ship["failed"], "error", 0, 0)
        _push_status(s, "idle")
        return
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
    s["stats"]["ok"]   += ok
    s["stats"]["fail"] += fail
    _push_ship(s, sn, ship["failed"], state, ok, ok + fail, errs)
    _push_stat(s, force=True)
    _append_op_log({
        "ts":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user":  s.get("username", ""),
        "sn":    sn,
        "ok":    ok,
        "fail":  fail,
        "state": state,
    })


def _resync_worker(s: dict):
    s["stop"].clear()
    token = s["token"]

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("logs") / f"results_{ts}.csv"

    try:
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
            _push_ship(s, sn, ship["failed"], "running", 0, ship["failed"])

            ok, fail, errs = _resync_one_ship(s, token, sn, sid, ship["failed"], w, stop_ev)
            cancelled = stop_ev.is_set()
            s["ship_stops"].pop(sn, None)

            state = ("cancelled" if cancelled
                     else "done" if fail == 0
                     else "partial" if ok > 0 else "error")
            s["stats"]["ok"]   += ok
            s["stats"]["fail"] += fail
            _push_ship(s, sn, ship["failed"], state, ok, ok + fail, errs)
            _push_stat(s, force=True)
            _append_op_log({
                "ts":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user":  s.get("username", ""),
                "sn":    sn,
                "ok":    ok,
                "fail":  fail,
                "state": state,
            })
    except PermissionError as exc:
        s["token"] = None
        _bc(s, {"type": "need_login"})
        _push_status(s, "idle")
        _bc(s, {"type": "done"})
        return

    if not s["stop"].is_set():
        _log(s,
             f"全部完成 ✓  ReSync 成功: {s['stats']['ok']}  失败: {s['stats']['fail']}  "
             f"结果已保存: {csv_path}",
             "success")
    _push_status(s, "done")
    _bc(s, {"type": "done"})


def _resync_one_ship(s: dict, token: str, sn: str, sid: str, failed: int, w,
                     ship_stop: "threading.Event | None" = None) -> tuple:
    ok = fail = 0
    err_msgs: list = []
    total_logged = False
    attempted_ids: set = set()   # guard against infinite loop on permanently-failing events

    def _stopped():
        return s["stop"].is_set() or (ship_stop is not None and ship_stop.is_set())

    PAGE_SIZE = 100

    while not _stopped():
        try:
            # Probe total to find the last page, then fetch from the last page.
            # This replays events in reverse order (last item first), matching
            # the manual click behaviour the user requested.
            probe = _nimbus(token,
                            "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                            {"shipmentId": sid, "operationResult": "ERROR",
                             "current": 1, "pageSize": 1})
            if not probe.get("success"):
                _log(s, f"{sn} 错误: {probe.get('msg')}", "error")
                break
            total = probe.get("total") or 0
            if total == 0:
                break
            last_page = (total + PAGE_SIZE - 1) // PAGE_SIZE  # ceiling division

            resp = _nimbus(token,
                           "/api/admin/operate/tenant-sync/parcelTrack-event/pageSearch",
                           {"shipmentId": sid, "operationResult": "ERROR",
                            "current": last_page, "pageSize": PAGE_SIZE})
        except PermissionError:
            raise
        except Exception as exc:
            _log(s, f"{sn} 获取事件失败: {exc}", "error")
            break

        if not resp.get("success"):
            _log(s, f"{sn} 错误: {resp.get('msg')}", "error")
            break

        if not total_logged:
            _log(s, f"  {sn}: {total} 条失败事件")
            s["stats"]["total_ev"] += total
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
        # broadcast per-ship progress after each batch
        _bc(s, {"type": "ship", "sn": sn, "failed": failed,
                "state": "running", "done": ok + fail,
                "total": max(failed, ok + fail), "errors": []})

    return ok, fail, err_msgs


def _replay(token: str, ev_num: int) -> tuple:
    try:
        resp = _nimbus(token,
                       "/api/admin/operate/tenant-sync/parcelTrack-event/replay",
                       {"id": ev_num}, timeout=15)
        msg = resp.get("msg") or ""
        # "event success can't replay" means the event is already SUCCESS — treat as ok
        if msg and "success" in msg.lower() and "replay" in msg.lower():
            return True, msg
        return (resp.get("success") is True or resp.get("code") == 0), msg
    except PermissionError:
        raise
    except Exception as exc:
        return False, str(exc)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if os.environ.get("LOCAL_DEV") == "1":
        import webbrowser, threading as _t
        _t.Timer(1.2, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
