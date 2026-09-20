"""
每小时自动扫描 + 失败告警邮件。

整点执行一次（8:00、9:00、10:00…），扫描范围和判定逻辑与页面上的「开始扫描」
完全一致——直接复用 app.py 的 _scan_worker，只是喂给它一个独立的假 session，
所以跟任何登录用户的手动扫描互不影响。

扫出失败记录就立刻发邮件给「通知设置」里配置的收件人。
"""
import json
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

import mailer

LOG_DIR     = Path("logs")
CONFIG_FILE = LOG_DIR / "auto_scan_config.json"
STATE_FILE  = LOG_DIR / "auto_scan_state.json"
TICK_SEC    = int(os.environ.get("AUTOSCAN_TICK_SEC", "30"))

ADMINS = {e.strip().lower()
          for e in os.environ.get("REPORT_ADMINS", "").split(",") if e.strip()}
TRIGGER_TOKEN = os.environ.get("REPORT_TRIGGER_TOKEN", "")

_DEFAULTS = {
    "enabled":    os.environ.get("AUTOSCAN_ENABLED", "1") == "1",
    "recipients": [e.strip() for e in os.environ.get("AUTOSCAN_RECIPIENT", "").split(",")
                   if e.strip()],
}

# 三类错误，与页面上的三个子 Tab 对应
CATEGORIES = [
    ("ships",      "HAWB Milestone"),
    ("ships_info", "Shipment Info"),
    ("ships_mawb", "MAWB Milestone"),
]

_ctx: dict = {}            # init() 注入的 app 内部 helper
_cfg_lock   = threading.Lock()
_state_lock = threading.Lock()
_run_lock   = threading.Lock()   # 防止上一轮没跑完又起一轮


def _log(msg: str):
    print(f"[autoscan] {datetime.now(_tz()):%Y-%m-%d %H:%M:%S %Z} {msg}", flush=True)


def _tz():
    """跟周报用同一个时区设置，日志和邮件里的时间才对得上。"""
    try:
        import weekly_report
        return weekly_report._tz()
    except Exception:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Chicago")


# ── 配置 ──────────────────────────────────────────────────────────────────────
def _read_json(path: Path, fallback):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, type(fallback)) else fallback
    except Exception:
        return fallback


def _write_json(path: Path, data):
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
    except Exception as exc:
        print(f"[autoscan] 写入 {path.name} 失败: {exc!r}", flush=True)


def cfg() -> dict:
    with _cfg_lock:
        merged = dict(_DEFAULTS)
        merged.update(_read_json(CONFIG_FILE, {}))
        return merged


def recipients() -> list:
    r = cfg().get("recipients") or []
    if isinstance(r, str):
        r = [x.strip() for x in r.split(",")]
    return [x for x in r if x]


def save_config(patch: dict) -> dict:
    import re
    clean = {}
    if "enabled" in patch:
        clean["enabled"] = bool(patch["enabled"])
    if "recipients" in patch:
        raw = patch["recipients"]
        if isinstance(raw, str):
            raw = re.split(r"[,;\s]+", raw)
        seen, out = set(), []
        for e in (str(x).strip() for x in raw):
            if not e:
                continue
            if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", e):
                raise ValueError(f"收件人邮箱格式不对: {e}")
            if e.lower() not in seen:
                seen.add(e.lower())
                out.append(e)
        clean["recipients"] = out          # 允许清空：不配收件人就只扫描不发信

    with _cfg_lock:
        current = _read_json(CONFIG_FILE, {})
        current.update(clean)
        _write_json(CONFIG_FILE, current)
    _log(f"通知配置已更新: {clean}")
    return cfg()


# ── 扫描 ──────────────────────────────────────────────────────────────────────
def _fake_session(token: str) -> dict:
    """_scan_worker 需要的最小 session。与真实登录 session 完全隔离。"""
    return {
        "token":      token,
        "username":   "auto-scan",
        "status":     "idle",
        "ships":      [],
        "ships_info": [],
        "ships_mawb": [],
        "stats":      {"found": 0, "ok": 0, "fail": 0, "total_ev": 0, "done_ev": 0},
        "q":          queue.Queue(maxsize=8000),   # 没人消费，满了 _bc 会丢弃
        "stop":       threading.Event(),
        "ship_stops": {},
        "ship_stops_info": {},
        "ship_stops_mawb": {},
        "seen":       datetime.now(),
        "_perm_ok":   True,
    }


def run_scan(notify: bool = True) -> dict:
    """跑一次完整扫描；发现失败则发信。返回结果摘要。"""
    if not _run_lock.acquire(blocking=False):
        _log("上一轮扫描还在进行，本轮跳过")
        return {"skipped": True, "reason": "上一轮扫描尚未结束"}
    try:
        token = _ctx["nimbus_login"]()
        if not token:
            raise RuntimeError("拿不到 Nimbus token")

        s = _fake_session(token)
        started = datetime.now(_tz())
        _log("开始扫描")
        _ctx["scan_worker"](s)

        counts = {key: len(s.get(key) or []) for key, _ in CATEGORIES}
        total  = sum(counts.values())
        took   = (datetime.now(_tz()) - started).total_seconds()
        _log(f"扫描完成，用时 {took:.0f}s —— "
             + "，".join(f"{label} {counts[key]}" for key, label in CATEGORIES))

        result = {
            "at":     started.isoformat(timespec="seconds"),
            "took_s": round(took),
            "counts": counts,
            "total":  total,
            "mailed": False,
            "recipients": [],
        }

        if total and notify:
            to = recipients()
            if not to:
                _log("发现失败记录，但未配置收件人，跳过发信")
            elif not mailer.configured():
                _log("发现失败记录，但未配置 SMTP，跳过发信")
            else:
                try:
                    sent = _send_alert(s, counts, total, started)
                    result["mailed"] = True
                    result["recipients"] = sent
                except Exception as exc:
                    _log(f"告警邮件发送失败: {exc!r}")
        elif not total:
            _log("未发现失败记录，不发信")

        with _state_lock:
            st = _read_json(STATE_FILE, {})
            st["last_run_at"]     = result["at"]
            st["last_run_hour"]   = started.strftime("%Y-%m-%dT%H")
            st["last_counts"]     = counts
            st["last_total"]      = total
            st["last_mailed"]     = result["mailed"]
            st["last_took_s"]     = result["took_s"]
            _write_json(STATE_FILE, st)

        return result
    finally:
        _run_lock.release()


# ── 告警邮件 ──────────────────────────────────────────────────────────────────
_TD = "padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px"
_TH = ("padding:8px 10px;border-bottom:2px solid #d1d5db;font-size:12px;text-align:left;"
       "color:#374151;text-transform:uppercase;letter-spacing:.04em")


def _table(label: str, ships: list) -> str:
    if not ships:
        return ""
    rows = "".join(
        f'<tr>'
        f'<td style="{_TD};font-family:ui-monospace,Menlo,Consolas,monospace;white-space:nowrap">'
        f'{escape(str(sh.get("sn", "")))}</td>'
        f'<td style="{_TD};text-align:right;font-variant-numeric:tabular-nums">'
        f'{escape(str(sh.get("failed", "")))}</td>'
        f'<td style="{_TD};white-space:nowrap;color:#6b7280">'
        f'{escape(str(sh.get("created_at", "") or "—"))}</td>'
        f'</tr>'
        for sh in ships
    )
    return (
        f'<h2 style="margin:24px 0 0;font-size:15px;font-weight:650">'
        f'{escape(label)} <span style="color:#b91c1c">({len(ships)})</span></h2>'
        f'<div style="overflow-x:auto"><table style="border-collapse:collapse;width:100%;margin-top:6px">'
        f'<thead><tr><th style="{_TH}">Shipment Number</th>'
        f'<th style="{_TH};text-align:right">Failed</th>'
        f'<th style="{_TH}">Created At</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div>'
    )


def _send_alert(s: dict, counts: dict, total: int, started: datetime) -> list:
    summary = "，".join(f"{label} {counts[key]}" for key, label in CATEGORIES if counts[key])
    subject = f"[Nimbus ReSync 告警] 发现 {total} 个失败 Shipment — {summary}"

    body = "".join(_table(label, s.get(key) or []) for key, label in CATEGORIES)
    html = f"""<!doctype html>
<html><body style="margin:0;padding:24px;background:#f9fafb;
font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827">
<div style="max-width:860px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;
border-radius:10px;padding:28px">
  <h1 style="margin:0 0 4px;font-size:20px;font-weight:650">Nimbus ReSync 自动扫描告警</h1>
  <p style="margin:0 0 20px;color:#6b7280;font-size:13px">
    扫描时间 {escape(started.strftime('%Y-%m-%d %H:%M:%S %Z'))} · 扫描范围：最近 7 天创建的 Shipment
  </p>
  <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:16px 20px">
    <div style="font-size:12px;color:#b91c1c;text-transform:uppercase;letter-spacing:.05em">
      Failed Shipments</div>
    <div style="font-size:32px;font-weight:700;color:#991b1b;line-height:1.2">{total}</div>
    <div style="font-size:13px;color:#7f1d1d;margin-top:2px">{escape(summary)}</div>
  </div>
  {body}
  <p style="margin:24px 0 0;padding-top:16px;border-top:1px solid #e5e7eb;
  color:#9ca3af;font-size:11px">
    由 resync 每小时自动扫描发出 · 登录系统可对这些 Shipment 执行 ReSync
  </p>
</div></body></html>"""

    text_lines = [f"Nimbus ReSync 自动扫描告警",
                  f"扫描时间 {started:%Y-%m-%d %H:%M:%S %Z}",
                  f"共发现 {total} 个失败 Shipment（{summary}）", ""]
    for key, label in CATEGORIES:
        ships = s.get(key) or []
        if ships:
            text_lines.append(f"[{label}] {len(ships)} 个")
            text_lines += [f"  {sh.get('sn')}  failed={sh.get('failed')}  "
                           f"{sh.get('created_at', '')}" for sh in ships]
            text_lines.append("")

    to = mailer.send(subject, recipients(), "\n".join(text_lines), html)
    _log(f"告警邮件已发送 -> {', '.join(to)}（{total} 个失败）")
    return to


# ── 调度 ──────────────────────────────────────────────────────────────────────
def _tick():
    if not cfg().get("enabled"):
        return
    now  = datetime.now(_tz())
    hour = now.strftime("%Y-%m-%dT%H")
    with _state_lock:
        st = _read_json(STATE_FILE, {})
    if st.get("last_run_hour") == hour:
        return
    try:
        run_scan()
    except Exception as exc:
        _log(f"扫描失败（下个整点重试）: {exc!r}")
        # 记下这一小时已尝试过，避免同一小时内每 30 秒重试一次
        with _state_lock:
            st = _read_json(STATE_FILE, {})
            st["last_run_hour"]  = hour
            st["last_run_error"] = f"{type(exc).__name__}: {exc}"
            _write_json(STATE_FILE, st)


def _loop():
    _log(f"自动扫描已启动：每个整点执行一次，收件人 {', '.join(recipients()) or '（未配置）'}")
    while True:
        try:
            _tick()
        except Exception as exc:
            _log(f"tick 异常: {exc!r}")
        time.sleep(TICK_SEC)


def status() -> dict:
    now = datetime.now(_tz())
    with _state_lock:
        st = _read_json(STATE_FILE, {})
    nxt = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return {
        "enabled":         bool(cfg().get("enabled")),
        "now":             now.isoformat(timespec="seconds"),
        "next_run_at":     nxt.isoformat(timespec="seconds"),
        "recipients":      recipients(),
        "smtp_configured": mailer.configured(),
        "running":         _run_lock.locked(),
        "state":           st,
    }


def init(app, *, scan_worker, nimbus_login):
    """由 app.py 调用：注入扫描实现和登录方式，注册接口，启动调度线程。"""
    _ctx.update(scan_worker=scan_worker, nimbus_login=nimbus_login)

    from flask import jsonify, request, session

    def _deny():
        if TRIGGER_TOKEN and request.headers.get("X-Report-Token") == TRIGGER_TOKEN:
            return None
        if not session.get("_tok"):
            return jsonify({"ok": False, "error": "未登录"}), 401
        if ADMINS and (session.get("_usr") or "").strip().lower() not in ADMINS:
            return jsonify({"ok": False, "error": "需要管理员权限"}), 403
        return None

    @app.route("/api/notify/config")
    def api_notify_config_get():
        denied = _deny()
        if denied:
            return denied
        c = cfg()
        return jsonify({"ok": True,
                        "config": {"enabled": bool(c.get("enabled")),
                                   "recipients": recipients()},
                        "status": status()})

    @app.route("/api/notify/config", methods=["POST"])
    def api_notify_config_set():
        denied = _deny()
        if denied:
            return denied
        try:
            save_config(request.get_json(silent=True) or {})
        except Exception as exc:
            return jsonify({"ok": False, "error": f"{exc}"}), 400
        c = cfg()
        return jsonify({"ok": True,
                        "config": {"enabled": bool(c.get("enabled")),
                                   "recipients": recipients()},
                        "status": status()})

    @app.route("/api/notify/run", methods=["POST"])
    def api_notify_run():
        denied = _deny()
        if denied:
            return denied
        body = request.get_json(silent=True) or {}
        try:
            return jsonify({"ok": True,
                            "result": run_scan(notify=bool(body.get("notify", True)))})
        except Exception as exc:
            _log(f"手动触发失败: {exc!r}")
            return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500

    threading.Thread(target=_loop, daemon=True, name="auto-scan").start()
