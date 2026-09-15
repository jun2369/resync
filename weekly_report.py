"""
每周周报。

调度（美国中部时间，自动处理夏令时）：
  - 周一：抓取上一周（周一~周日）的数据，落成快照文件
  - 周二 16:00：读快照 -> 渲染 -> 发信；成功后删除快照

上一周已经结束、数据不再变化，所以每周只抓一次，发送时不再调 Nimbus。
服务重启会自动补跑错过的窗口（状态存在挂载卷上）。
"""

import csv
import io
import json
import os
import re
import smtplib
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

import requests as req

# ── 凭证与固定项：只走环境变量，不进配置文件，也不在管理页上显示 ──────────────
SMTP_HOST     = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SENDER        = os.environ.get("REPORT_SENDER", "jma2369@gmail.com")
SMTP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

NIMBUS_USERNAME = os.environ.get("NIMBUS_USERNAME", "")
NIMBUS_PASSWORD = os.environ.get("NIMBUS_PASSWORD", "")

TRIGGER_TOKEN = os.environ.get("REPORT_TRIGGER_TOKEN", "")

# 能进管理页的邮箱，逗号分隔。留空 = 任何登录用户都能改。
ADMINS = {e.strip().lower()
          for e in os.environ.get("REPORT_ADMINS", "").split(",") if e.strip()}

LOG_DIR     = Path("logs")
STATE_FILE  = LOG_DIR / "weekly_report_state.json"
CONFIG_FILE = LOG_DIR / "weekly_report_config.json"
TICK_SEC    = int(os.environ.get("REPORT_TICK_SEC", "60"))

# ── 可在管理页改的配置 ────────────────────────────────────────────────────────
# 优先级：配置文件 > 环境变量 > 这里的默认值。
# 每次用到都重新读，所以页面上保存后最迟一个 tick（60 秒）生效，不用重启。
_DEFAULTS = {
    "enabled":      os.environ.get("REPORT_ENABLED", "1") == "1",
    "timezone":     os.environ.get("REPORT_TZ", "America/Chicago"),
    "send_weekday": int(os.environ.get("REPORT_SEND_WEEKDAY", "1")),   # 0=周一
    "send_hour":    int(os.environ.get("REPORT_SEND_HOUR", "16")),
    "send_minute":  int(os.environ.get("REPORT_SEND_MINUTE", "0")),
    "recipients":   [e.strip() for e in os.environ.get(
        "REPORT_RECIPIENT", "junjie.ma@agslogistics.com").split(",") if e.strip()],
}

WEEKDAY_NAMES = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

_cfg_lock  = threading.Lock()
_cfg_cache: dict = {}      # 文件内容的内存副本
_cfg_mtime = None          # 用来判断文件有没有被外部改动

# init() 注入的 app 内部 helper
_ctx: dict = {}


def cfg() -> dict:
    """当前生效配置。文件变了会自动重新读，所以多进程/手工改文件也能跟上。"""
    global _cfg_cache, _cfg_mtime
    with _cfg_lock:
        try:
            m = CONFIG_FILE.stat().st_mtime
        except OSError:
            m = None
        if m != _cfg_mtime:
            _cfg_mtime = m
            _cfg_cache = {}
            if m is not None:
                try:
                    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                        loaded = json.load(f)
                    if isinstance(loaded, dict):
                        _cfg_cache = loaded
                except Exception as exc:
                    print(f"[weekly] 配置文件读取失败，回落到环境变量: {exc!r}",
                          flush=True)
        merged = dict(_DEFAULTS)
        merged.update(_cfg_cache)
        return merged


def _tz() -> ZoneInfo:
    name = cfg().get("timezone") or "America/Chicago"
    try:
        return ZoneInfo(name)
    except Exception:
        print(f"[weekly] 时区 {name!r} 无效，回落到 America/Chicago", flush=True)
        return ZoneInfo("America/Chicago")


def recipients() -> list:
    r = cfg().get("recipients") or []
    if isinstance(r, str):
        r = [x.strip() for x in r.split(",")]
    return [x for x in r if x]


_FIELD_LABELS = {
    "enabled":      "自动发送",
    "timezone":     "时区",
    "send_weekday": "星期几",
    "send_hour":    "小时",
    "send_minute":  "分钟",
    "recipients":   "收件人",
}


def _pretty(field: str, value):
    if field == "send_weekday":
        try:
            return WEEKDAY_NAMES[int(value)]
        except Exception:
            return str(value)
    if field == "enabled":
        return "启用" if value else "停用"
    if field == "recipients":
        v = value if isinstance(value, list) else [value]
        return ", ".join(str(x) for x in v) or "（空）"
    if field in ("send_hour", "send_minute"):
        return f"{int(value):02d}"
    return str(value)


def overrides() -> list:
    """页面配置里与环境变量/代码默认值不同的项。用来在管理页标出差异。"""
    current = cfg()
    out = []
    for field, default in _DEFAULTS.items():
        now = current.get(field)
        if now != default:
            out.append({
                "field":   field,
                "label":   _FIELD_LABELS.get(field, field),
                "default": _pretty(field, default),
                "current": _pretty(field, now),
            })
    return out


def save_config(patch: dict) -> dict:
    """校验并写入配置。只接受已知字段，返回写入后的完整生效配置。"""
    global _cfg_cache, _cfg_mtime
    clean = {}

    if "enabled" in patch:
        clean["enabled"] = bool(patch["enabled"])

    if "timezone" in patch:
        name = str(patch["timezone"]).strip()
        ZoneInfo(name)                      # 无效时区在这里就抛出，不会写进文件
        clean["timezone"] = name

    if "send_weekday" in patch:
        v = int(patch["send_weekday"])
        if not 0 <= v <= 6:
            raise ValueError("send_weekday 必须是 0~6（0=周一）")
        clean["send_weekday"] = v

    if "send_hour" in patch:
        v = int(patch["send_hour"])
        if not 0 <= v <= 23:
            raise ValueError("send_hour 必须是 0~23")
        clean["send_hour"] = v

    if "send_minute" in patch:
        v = int(patch["send_minute"])
        if not 0 <= v <= 59:
            raise ValueError("send_minute 必须是 0~59")
        clean["send_minute"] = v

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
        if not out:
            raise ValueError("收件人不能为空")
        clean["recipients"] = out

    with _cfg_lock:
        current = dict(_cfg_cache)
        current.update(clean)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        tmp = CONFIG_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(current, f, ensure_ascii=False, indent=2)
        tmp.replace(CONFIG_FILE)
        _cfg_cache = current
        try:
            _cfg_mtime = CONFIG_FILE.stat().st_mtime
        except OSError:
            _cfg_mtime = None

    _log(f"配置已更新: {clean}")
    return cfg()


def _log(msg: str):
    print(f"[weekly] {datetime.now(_tz()):%Y-%m-%d %H:%M:%S %Z} {msg}", flush=True)


# ── 周区间 ────────────────────────────────────────────────────────────────────
def last_week_range(today: date = None):
    """返回上一周的 (周一, 周日)。9/15 -> (9/07, 9/13)；9/22 -> (9/14, 9/20)。"""
    today = today or datetime.now(_tz()).date()
    this_monday = today - timedelta(days=today.weekday())
    start = this_monday - timedelta(days=7)
    return start, start + timedelta(days=6)


def send_moment(week_start: date) -> datetime:
    """该期周报应当发出的时刻 = 下一周的配置星期几 + 配置时分。"""
    c = cfg()
    this_monday = week_start + timedelta(days=7)
    d = this_monday + timedelta(days=int(c["send_weekday"]))
    return datetime(d.year, d.month, d.day,
                    int(c["send_hour"]), int(c["send_minute"]), tzinfo=_tz())


def snapshot_path(week_start: date) -> Path:
    return LOG_DIR / f"weekly_snapshot_{week_start.isoformat()}.json"


# ── 状态（存挂载卷，重启不丢） ────────────────────────────────────────────────
_state_lock = threading.Lock()


def _read_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_state(st: dict):
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
        tmp.replace(STATE_FILE)
    except Exception as exc:
        _log(f"状态写入失败: {exc!r}")


# ── Nimbus 登录（无人值守） ───────────────────────────────────────────────────
def _nimbus_login() -> str:
    """用环境变量里的账号密码登录，返回 token；失败返回空串。"""
    if not NIMBUS_USERNAME or not NIMBUS_PASSWORD:
        _log("未配置 NIMBUS_USERNAME / NIMBUS_PASSWORD，无法自动抓取")
        return ""
    attempts = [
        ("https://nimbusgroup.us",       "/api/v2/userauth/auth/sign-in",
         {"email": NIMBUS_USERNAME, "password": NIMBUS_PASSWORD}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/sign-in",
         {"email": NIMBUS_USERNAME, "password": NIMBUS_PASSWORD}),
        ("https://admin.nimbusgroup.us", "/api/v2/userauth/auth/login",
         {"email": NIMBUS_USERNAME, "password": NIMBUS_PASSWORD}),
        ("https://admin.nimbusgroup.us", "/api/admin/auth/login",
         {"username": NIMBUS_USERNAME, "password": NIMBUS_PASSWORD}),
    ]
    for base, path, body in attempts:
        try:
            r = req.post(f"{base}{path}", json=body, timeout=10,
                         headers={"Content-Type": "application/json"})
            if r.ok:
                d = r.json()
                tok = ((d.get("data") or {}).get("token")
                       or d.get("token") or d.get("access_token"))
                if tok:
                    _log(f"Nimbus 登录成功（{base}{path}）")
                    return tok
        except Exception:
            continue
    _log("Nimbus 登录失败：四个端点都没拿到 token")
    return ""


# ── 抓取 ──────────────────────────────────────────────────────────────────────
def fetch_week(week_start: date, week_end: date, token: str = "") -> list:
    """从 Nimbus 拉取指定周的 shipment，补齐 client/branch，返回行列表。"""
    token = token or _nimbus_login()
    if not token:
        raise RuntimeError("拿不到 Nimbus token")

    nimbus         = _ctx["nimbus"]
    get_basic_info = _ctx["get_basic_info"]
    sn_cache       = _ctx["sn_cache"]
    sn_cache_lock  = _ctx["sn_cache_lock"]
    save_sn_cache  = _ctx["save_sn_cache"]

    lo, hi = week_start.isoformat(), week_end.isoformat()
    found: dict = {}          # sn -> created_at
    page, total_pages = 1, 9999

    # globalSearch 按时间倒序返回：先翻过比目标周更新的，收集区间内的，
    # 一旦出现早于 week_start 的就可以停了。
    while page <= total_pages and page <= 400:
        resp = nimbus(token,
                      "/api/admin/operate/tenant-sync/shipment-event/globalSearch",
                      {"current": page, "pageSize": 50})
        if not resp.get("success"):
            break
        total_pages = resp.get("totalPages") or 1
        items = resp.get("data") or []
        if not items:
            break

        passed_window = False
        for item in items:
            sn = (item.get("shipmentNumber") or "").strip()
            et = item.get("eventTime") or ""
            ca = et.replace("T", " ")[:19] if "T" in et else et
            if not sn or not ca:
                continue
            day = ca[:10]
            if day < lo:
                passed_window = True
                break
            if day <= hi and sn not in found:
                found[sn] = ca

        if passed_window:
            break
        page += 1

    _log(f"{lo}~{hi}: globalSearch 翻了 {page} 页，命中 {len(found)} 个 SN")

    # 补 client / branch / 重量等 —— 缓存里齐了就不重复调接口。
    # 用 "entryType" 在不在做判据：clientName 早就有的老条目也缺这几个新字段，
    # 只看 clientName 的话它们永远补不上。
    need = []
    for sn in found:
        with sn_cache_lock:
            ent = sn_cache.get(sn) or {}
        if ent.get("not_found"):
            continue
        if not ent.get("clientName") or "entryType" not in ent:
            need.append(sn)

    if need:
        _log(f"需要补字段的有 {len(need)} 个，开始并发拉取")

        def _one(sn):
            info = get_basic_info(token, sn)
            with sn_cache_lock:
                ent = sn_cache.setdefault(sn, {})
                ent["created_at"] = found[sn]
                if info.get("not_found"):
                    ent["not_found"] = True
                else:
                    for k in ("clientName", "branchCode", "hawbCount",
                              "entryType", "chargeableWeight", "grossWeight"):
                        ent[k] = info.get(k)

        with ThreadPoolExecutor(max_workers=15) as pool:
            for fut in as_completed([pool.submit(_one, sn) for sn in need]):
                try:
                    fut.result()
                except Exception:
                    pass
        save_sn_cache()

    rows = []
    for sn, ca in found.items():
        with sn_cache_lock:
            ent = sn_cache.get(sn) or {}
        if ent.get("not_found"):
            continue
        rows.append({
            "date":             ca[:10],
            "sn":               sn,
            "clientName":       ent.get("clientName") or "",
            "branchCode":       ent.get("branchCode") or "",
            "hawbCount":        ent.get("hawbCount"),
            "entryType":        ent.get("entryType") or "",
            "chargeableWeight": ent.get("chargeableWeight"),
            "grossWeight":      ent.get("grossWeight"),
            "created_at":       ca,
        })
    rows.sort(key=lambda r: (r["created_at"], r["sn"]))
    return rows


def build_snapshot(week_start: date, week_end: date, force: bool = False) -> dict:
    """抓取并写快照。已存在且未 force 则直接复用。"""
    path = snapshot_path(week_start)
    if path.exists() and not force:
        try:
            with open(path, "r", encoding="utf-8") as f:
                snap = json.load(f)
            _log(f"快照已存在，复用 {path.name}（{len(snap.get('rows', []))} 条）")
            return snap
        except Exception as exc:
            _log(f"快照损坏，重新抓取: {exc!r}")

    rows = fetch_week(week_start, week_end)
    snap = {
        "week_start": week_start.isoformat(),
        "week_end":   week_end.isoformat(),
        "fetched_at": datetime.now(_tz()).isoformat(timespec="seconds"),
        "rows":       rows,
    }
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False)
    tmp.replace(path)
    _log(f"快照写入 {path.name}（{len(rows)} 条）")
    return snap


# ── 渲染 ──────────────────────────────────────────────────────────────────────
_CSS_TD = "padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px"
_CSS_TH = ("padding:8px 10px;border-bottom:2px solid #d1d5db;font-size:12px;"
           "text-align:left;color:#374151;text-transform:uppercase;letter-spacing:.04em")


def _num(v) -> str:
    """数值格式化：整数去掉小数点，小数保留两位，空值显示 —。"""
    if v is None or v == "":
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    return str(int(f)) if f.is_integer() else f"{f:,.2f}"


def _mini_table(title: str, counter: Counter) -> str:
    if not counter:
        return ""
    rows = "".join(
        f'<tr><td style="{_CSS_TD}">{escape(str(k) or "—")}</td>'
        f'<td style="{_CSS_TD};text-align:right;font-variant-numeric:tabular-nums">{v}</td></tr>'
        for k, v in counter.most_common()
    )
    return (
        f'<div style="display:inline-block;vertical-align:top;margin:0 28px 20px 0;min-width:220px">'
        f'<div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:6px;'
        f'text-transform:uppercase;letter-spacing:.04em">{escape(title)}</div>'
        f'<table style="border-collapse:collapse;width:100%">{rows}</table></div>'
    )


def render_html(snap: dict) -> str:
    rows  = snap.get("rows", [])
    start = snap.get("week_start", "")
    end   = snap.get("week_end", "")

    by_client = Counter(r.get("clientName") or "—" for r in rows)
    by_branch = Counter(r.get("branchCode") or "—" for r in rows)
    by_day    = Counter(r.get("date") or "—" for r in rows)

    if rows:
        num_td = _CSS_TD + (";text-align:right;white-space:nowrap;"
                            "font-variant-numeric:tabular-nums")
        num_th = _CSS_TH + ";text-align:right"
        body = "".join(
            f'<tr>'
            f'<td style="{_CSS_TD};white-space:nowrap">{escape(r.get("date", ""))}</td>'
            f'<td style="{_CSS_TD};white-space:nowrap;font-family:ui-monospace,Menlo,Consolas,monospace">'
            f'{escape(r.get("sn", ""))}</td>'
            f'<td style="{_CSS_TD}">{escape(r.get("clientName") or "—")}</td>'
            f'<td style="{_CSS_TD};white-space:nowrap">{escape(r.get("branchCode") or "—")}</td>'
            f'<td style="{num_td}">{_num(r.get("hawbCount"))}</td>'
            f'<td style="{_CSS_TD};white-space:nowrap">{escape(r.get("entryType") or "—")}</td>'
            f'<td style="{num_td}">{_num(r.get("chargeableWeight"))}</td>'
            f'<td style="{num_td}">{_num(r.get("grossWeight"))}</td>'
            f'</tr>'
            for r in rows
        )
        detail = (
            f'<div style="overflow-x:auto">'
            f'<table style="border-collapse:collapse;width:100%;margin-top:8px">'
            f'<thead><tr>'
            f'<th style="{_CSS_TH}">Date</th>'
            f'<th style="{_CSS_TH}">Shipment Number</th>'
            f'<th style="{_CSS_TH}">Client</th>'
            f'<th style="{_CSS_TH}">Branch</th>'
            f'<th style="{num_th}">HAWB Count</th>'
            f'<th style="{_CSS_TH}">Entry Type</th>'
            f'<th style="{num_th}">Chargeable Wt</th>'
            f'<th style="{num_th}">Gross Wt</th>'
            f'</tr></thead><tbody>{body}</tbody></table></div>'
        )
    else:
        detail = ('<p style="color:#b45309;background:#fffbeb;border:1px solid #fcd34d;'
                  'border-radius:6px;padding:12px;font-size:13px">'
                  'No shipments found for this period. If that is unexpected, '
                  'check the fetch job logs.</p>')

    daily = "".join(
        f'<tr><td style="{_CSS_TD}">{escape(d)}</td>'
        f'<td style="{_CSS_TD};text-align:right;font-variant-numeric:tabular-nums">{by_day[d]}</td></tr>'
        for d in sorted(by_day)
    )

    return f"""<!doctype html>
<html><body style="margin:0;padding:24px;background:#f9fafb;
font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827">
<div style="max-width:900px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;
border-radius:10px;padding:28px">

  <h1 style="margin:0 0 4px;font-size:20px;font-weight:650">Nimbus Micra Weekly Report</h1>
  <p style="margin:0 0 20px;color:#6b7280;font-size:13px">
    Period <strong style="color:#111827">{escape(start)}</strong> &ndash;
    <strong style="color:#111827">{escape(end)}</strong> (Mon&ndash;Sun)
  </p>

  <div style="background:#eef2ff;border:1px solid #c7d2fe;border-radius:8px;
  padding:16px 20px;margin-bottom:24px">
    <div style="font-size:12px;color:#4338ca;text-transform:uppercase;letter-spacing:.05em">
      Created Shipments</div>
    <div style="font-size:32px;font-weight:700;color:#3730a3;line-height:1.2">{len(rows)}</div>
  </div>

  {_mini_table("By Client", by_client)}
  {_mini_table("By Branch", by_branch)}
  <div style="display:inline-block;vertical-align:top;margin:0 0 20px 0;min-width:180px">
    <div style="font-size:12px;font-weight:600;color:#6b7280;margin-bottom:6px;
    text-transform:uppercase;letter-spacing:.04em">By Day</div>
    <table style="border-collapse:collapse;width:100%">{daily}</table>
  </div>

  <h2 style="margin:20px 0 0;font-size:15px;font-weight:650;padding-top:20px;
  border-top:1px solid #e5e7eb">Details</h2>
  {detail}

  <p style="margin:24px 0 0;padding-top:16px;border-top:1px solid #e5e7eb;
  color:#9ca3af;font-size:11px">
    Data fetched {escape(str(snap.get("fetched_at", "")))} &middot; Full detail in the attached CSV &middot;
    Sent automatically by resync
  </p>
</div></body></html>"""


def render_csv(snap: dict) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Date", "Shipment Number", "Client", "Branch",
                "HAWB Count", "Entry Type", "Chargeable Weight", "Gross Weight"])
    for r in snap.get("rows", []):
        w.writerow([r.get("date", ""), r.get("sn", ""),
                    r.get("clientName") or "", r.get("branchCode") or "",
                    r.get("hawbCount"), r.get("entryType") or "",
                    r.get("chargeableWeight"), r.get("grossWeight")])
    return ("﻿" + buf.getvalue()).encode("utf-8")


# ── 发信 ──────────────────────────────────────────────────────────────────────
def send_email(snap: dict):
    if not SMTP_PASSWORD:
        raise RuntimeError("未配置 GMAIL_APP_PASSWORD")

    start, end = snap.get("week_start", ""), snap.get("week_end", "")
    n = len(snap.get("rows", []))

    msg = EmailMessage()
    to = recipients()
    if not to:
        raise RuntimeError("没有配置收件人")
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(to)
    msg["Subject"] = f"[Nimbus Micra Weekly Report] {start} ~ {end} · {n} shipments"
    msg.set_content(
        f"Nimbus Micra Weekly Report — {start} to {end} (Mon-Sun)\n\n"
        f"Created Shipments: {n}\n\n"
        f"Full detail is in the attached CSV; the HTML version of this "
        f"message carries the same table inline."
    )
    msg.add_alternative(render_html(snap), subtype="html")
    msg.add_attachment(render_csv(snap), maintype="text", subtype="csv",
                       filename=f"nimbus_weekly_{start}_{end}.csv")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SENDER, SMTP_PASSWORD)
        s.send_message(msg, from_addr=SENDER, to_addrs=to)
    _log(f"邮件已发送 -> {', '.join(to)}（{start}~{end}, {n} 条）")


# ── 一期周报的完整流程 ────────────────────────────────────────────────────────
def run_once(week_start: date = None, week_end: date = None,
             force_fetch: bool = False, cleanup: bool = True) -> dict:
    """抓取(或复用快照) -> 发送 -> 清理。返回结果摘要。"""
    if week_start is None:
        week_start, week_end = last_week_range()
    snap = build_snapshot(week_start, week_end, force=force_fetch)
    send_email(snap)
    if cleanup:
        try:
            snapshot_path(week_start).unlink()
            _log(f"已清理快照 {snapshot_path(week_start).name}")
        except FileNotFoundError:
            pass
        except Exception as exc:
            _log(f"快照清理失败: {exc!r}")
    return {"week_start": week_start.isoformat(),
            "week_end":   week_end.isoformat(),
            "rows":       len(snap.get("rows", []))}


# ── 调度线程 ──────────────────────────────────────────────────────────────────
def _tick():
    """每次唤醒检查一遍：该抓的抓，该发的发。重启后会自动补跑错过的窗口。"""
    if not cfg().get("enabled"):
        return
    now = datetime.now(_tz())
    week_start, week_end = last_week_range(now.date())
    key = week_start.isoformat()

    with _state_lock:
        st = _read_state()

        # 1) 抓取：新的一周一开始，上一周的数据就固定了，随时可抓
        if st.get("last_fetch_week") != key:
            try:
                build_snapshot(week_start, week_end)
                st["last_fetch_week"] = key
                st["last_fetch_at"]   = now.isoformat(timespec="seconds")
                _write_state(st)
            except Exception as exc:
                _log(f"抓取失败（下次唤醒重试）: {exc!r}")

        # 2) 发送：到点且本期还没发过
        due = send_moment(week_start)
        if st.get("last_sent_week") != key and now >= due:
            try:
                res = run_once(week_start, week_end)
                st["last_sent_week"] = key
                st["last_sent_at"]   = now.isoformat(timespec="seconds")
                st["last_sent_rows"] = res["rows"]
                _write_state(st)
            except Exception as exc:
                _log(f"发送失败（下次唤醒重试）: {exc!r}")


def _loop():
    c = cfg()
    _log(f"调度启动：每{WEEKDAY_NAMES[int(c['send_weekday'])]} "
         f"{int(c['send_hour']):02d}:{int(c['send_minute']):02d} "
         f"{c['timezone']} 发送上一周数据 -> {', '.join(recipients())}")
    while True:
        try:
            _tick()
        except Exception as exc:
            _log(f"tick 异常: {exc!r}")
        time.sleep(TICK_SEC)


def status() -> dict:
    c = cfg()
    now = datetime.now(_tz())
    week_start, week_end = last_week_range(now.date())
    snap = snapshot_path(week_start)
    return {
        "enabled":        bool(c.get("enabled")),
        "now":            now.isoformat(timespec="seconds"),
        "timezone":       c.get("timezone"),
        "send_weekday":   int(c["send_weekday"]),
        "send_weekday_name": WEEKDAY_NAMES[int(c["send_weekday"])],
        "send_hour":      int(c["send_hour"]),
        "send_minute":    int(c["send_minute"]),
        "week_start":     week_start.isoformat(),
        "week_end":       week_end.isoformat(),
        "send_due_at":    send_moment(week_start).isoformat(timespec="seconds"),
        "snapshot_exists": snap.exists(),
        "snapshot_file":  snap.name,
        "sender":         SENDER,
        "recipients":     recipients(),
        "smtp_configured":   bool(SMTP_PASSWORD),
        "nimbus_configured": bool(NIMBUS_USERNAME and NIMBUS_PASSWORD),
        "state":          _read_state(),
    }


def init(app, *, nimbus, get_basic_info, sn_cache, sn_cache_lock, save_sn_cache):
    """由 app.py 调用：注入依赖、注册手动触发端点、启动调度线程。"""
    _ctx.update(nimbus=nimbus, get_basic_info=get_basic_info,
                sn_cache=sn_cache, sn_cache_lock=sn_cache_lock,
                save_sn_cache=save_sn_cache)

    from flask import jsonify, request, session

    def _is_admin() -> bool:
        """ADMINS 为空 = 不限制；否则只认名单里的登录邮箱。"""
        if TRIGGER_TOKEN and request.headers.get("X-Report-Token") == TRIGGER_TOKEN:
            return True
        if not session.get("_tok"):
            return False
        if not ADMINS:
            return True
        return (session.get("_usr") or "").strip().lower() in ADMINS

    def _deny():
        """整个周报模块只对管理员开放。未登录给 401，登录了但不在名单给 403。"""
        if _is_admin():
            return None
        if not session.get("_tok"):
            return jsonify({"ok": False, "error": "未登录"}), 401
        return jsonify({"ok": False, "error": "需要管理员权限"}), 403

    @app.route("/api/weekly/status")
    def api_weekly_status():
        denied = _deny()
        if denied:
            return denied
        return jsonify({"ok": True, "status": status(), "is_admin": _is_admin()})

    @app.route("/api/weekly/config")
    def api_weekly_config_get():
        denied = _deny()
        if denied:
            return denied
        c = cfg()
        return jsonify({
            "ok":       True,
            "is_admin": _is_admin(),
            "config": {
                "enabled":      bool(c.get("enabled")),
                "timezone":     c.get("timezone"),
                "send_weekday": int(c["send_weekday"]),
                "send_hour":    int(c["send_hour"]),
                "send_minute":  int(c["send_minute"]),
                "recipients":   recipients(),
            },
            # 页面配置覆盖掉环境变量/代码默认值的项，供页面标出差异
            "overrides": overrides(),
            "defaults":  _DEFAULTS,
            # 只读，供页面展示——凭证本身不返回
            "sender":            SENDER,
            "smtp_configured":   bool(SMTP_PASSWORD),
            "nimbus_configured": bool(NIMBUS_USERNAME and NIMBUS_PASSWORD),
            "status":            status(),
        })

    @app.route("/api/weekly/config", methods=["POST"])
    def api_weekly_config_set():
        denied = _deny()
        if denied:
            return denied
        patch = request.get_json(silent=True) or {}
        try:
            new_cfg = save_config(patch)
        except Exception as exc:
            return jsonify({"ok": False, "error": f"{exc}"}), 400
        return jsonify({
            "ok": True,
            "config": {
                "enabled":      bool(new_cfg.get("enabled")),
                "timezone":     new_cfg.get("timezone"),
                "send_weekday": int(new_cfg["send_weekday"]),
                "send_hour":    int(new_cfg["send_hour"]),
                "send_minute":  int(new_cfg["send_minute"]),
                "recipients":   recipients(),
            },
            "status": status(),
        })

    @app.route("/api/weekly/timezones")
    def api_weekly_timezones():
        denied = _deny()
        if denied:
            return denied
        import zoneinfo
        return jsonify({"ok": True, "timezones": sorted(zoneinfo.available_timezones())})

    @app.route("/api/weekly/run", methods=["POST"])
    def api_weekly_run():
        denied = _deny()
        if denied:
            return denied
        # silent=True: a bare POST with no Content-Type would otherwise 415
        body    = request.get_json(silent=True) or {}
        force   = bool(body.get("force_fetch"))
        cleanup = bool(body.get("cleanup", False))   # 手动触发默认保留快照
        try:
            res = run_once(force_fetch=force, cleanup=cleanup)
            return jsonify({"ok": True, "result": res})
        except Exception as exc:
            _log(f"手动触发失败: {exc!r}")
            return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500

    threading.Thread(target=_loop, daemon=True, name="weekly-report").start()
