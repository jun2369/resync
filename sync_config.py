"""
把线上正在生效的周报配置拉到本地。

  python sync_config.py          拉取、打印、写入本地
  python sync_config.py --dry    只看不写

写两个文件：
  logs/weekly_report_config.json      本地跑 app.py 时会读它，行为与线上一致（不进 git）
  weekly_report.config.json           带时间戳的快照，**进 git**

第二个文件是关键：同步完跑 git diff，就能看出线上配置自上次同步以来改了什么。
没有变化 = 工作区干净 = 本地认知与线上一致。

需要两个环境变量（放在 .env 里即可，.env 已被 gitignore）：
  RESYNC_URL             默认 https://www.nimbusgroup-resync.com
  REPORT_TRIGGER_TOKEN   与 Northflank 上配的同名 Secret 一致
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests as req

ROOT       = Path(__file__).parent
LIVE_FILE  = ROOT / "logs" / "weekly_report_config.json"
SNAP_FILE  = ROOT / "weekly_report.config.json"
WEEKDAYS   = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def load_dotenv():
    """极简 .env 读取，避免为这一个脚本引入 python-dotenv。"""
    f = ROOT / ".env"
    if not f.exists():
        return
    for line in f.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def restore(base: str, token: str):
    """把仓库里的快照推回线上。用于卷丢失后恢复，或撤销页面上的误改。"""
    if not SNAP_FILE.exists():
        print(f"找不到 {SNAP_FILE.name}，先跑一次 python sync_config.py")
        sys.exit(1)

    snap = json.loads(SNAP_FILE.read_text(encoding="utf-8"))
    cfg  = snap.get("config") or {}
    print(f"快照同步于 {snap.get('synced_at')}，内容：")
    print(f"  每{WEEKDAYS[int(cfg.get('send_weekday', 1))]} "
          f"{int(cfg.get('send_hour', 0)):02d}:{int(cfg.get('send_minute', 0)):02d} "
          f"{cfg.get('timezone')}")
    print(f"  收件人 {', '.join(cfg.get('recipients') or [])}")
    print(f"  自动发送 {'启用' if cfg.get('enabled') else '停用'}")

    if input(f"\n用它覆盖 {base} 上的当前配置？(yes/N) ").strip().lower() != "yes":
        print("已取消")
        return

    r = req.post(f"{base}/api/weekly/config", json=cfg,
                 headers={"X-Report-Token": token}, timeout=20)
    if r.ok:
        print("已恢复。再跑一次 python sync_config.py 确认。")
    else:
        print(f"失败 HTTP {r.status_code}: {r.text[:300]}")
        sys.exit(1)


def main():
    load_dotenv()
    dry = "--dry" in sys.argv

    base  = os.environ.get("RESYNC_URL", "https://www.nimbusgroup-resync.com").rstrip("/")
    token = os.environ.get("REPORT_TRIGGER_TOKEN", "")
    if not token:
        print("缺少 REPORT_TRIGGER_TOKEN。")
        print("在 Northflank 的 weekly-report secret group 里加一个随便什么随机串，")
        print("然后在项目根目录建 .env 写上同样的值：")
        print("  REPORT_TRIGGER_TOKEN=<那个随机串>")
        sys.exit(1)

    if "--restore" in sys.argv:
        restore(base, token)
        return

    try:
        r = req.get(f"{base}/api/weekly/config",
                    headers={"X-Report-Token": token}, timeout=20)
    except Exception as exc:
        print(f"请求失败：{type(exc).__name__}: {exc}")
        sys.exit(1)

    if r.status_code in (401, 403):
        print(f"鉴权失败 (HTTP {r.status_code})：本地 token 与线上 Secret 不一致")
        sys.exit(1)
    if not r.ok:
        print(f"HTTP {r.status_code}: {r.text[:300]}")
        sys.exit(1)

    body = r.json()
    cfg, st = body.get("config") or {}, body.get("status") or {}
    state   = st.get("state") or {}

    print(f"来源 {base}\n")
    print("── 当前生效配置 ─────────────────────────────")
    print(f"  自动发送   {'启用' if cfg.get('enabled') else '停用'}")
    print(f"  发送时刻   每{WEEKDAYS[int(cfg.get('send_weekday', 1))]} "
          f"{int(cfg.get('send_hour', 0)):02d}:{int(cfg.get('send_minute', 0)):02d}")
    print(f"  时区       {cfg.get('timezone')}")
    print(f"  收件人     {', '.join(cfg.get('recipients') or []) or '（空）'}")
    print(f"  发件人     {body.get('sender')}")

    print("\n── 当前状态 ─────────────────────────────────")
    print(f"  服务器时间 {st.get('now')}")
    print(f"  本期区间   {st.get('week_start')} ~ {st.get('week_end')}")
    print(f"  本期应发于 {st.get('send_due_at')}")
    print(f"  上次发送   {state.get('last_sent_at') or '—'}"
          + (f"（{state.get('last_sent_rows')} 条）" if state.get('last_sent_rows') else ""))
    print(f"  上次抓取   {state.get('last_fetch_at') or '—'}")
    print(f"  快照       {st.get('snapshot_file') if st.get('snapshot_exists') else '尚未生成'}")
    print(f"  SMTP       {'已配置' if body.get('smtp_configured') else '未配置'}")
    print(f"  Nimbus     {'已配置' if body.get('nimbus_configured') else '未配置'}")

    if dry:
        print("\n--dry：未写入任何文件")
        return

    LIVE_FILE.parent.mkdir(parents=True, exist_ok=True)
    LIVE_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2),
                         encoding="utf-8")

    snapshot = {
        "_comment":   "线上生效配置的快照，由 sync_config.py 写入。仅作记录，不是生效值。",
        "synced_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source":     base,
        "config":     cfg,
        "status": {
            "week_start": st.get("week_start"),
            "week_end":   st.get("week_end"),
            "send_due_at": st.get("send_due_at"),
            "last_sent_week": state.get("last_sent_week"),
            "last_sent_at":   state.get("last_sent_at"),
            "last_sent_rows": state.get("last_sent_rows"),
        },
    }
    SNAP_FILE.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")

    print(f"\n已写入 {LIVE_FILE.relative_to(ROOT)}  （本地运行时生效，不进 git）")
    print(f"已写入 {SNAP_FILE.relative_to(ROOT)}  （快照，进 git）")
    print("\n跑一下 git diff 看线上配置自上次同步以来改了什么；没有输出就是没变过。")


if __name__ == "__main__":
    main()
