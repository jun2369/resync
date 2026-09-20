"""
统一发信出口。周报和自动扫描告警都走这里，凭证只在这一个地方读。

配置全部来自环境变量，不落盘、不经过任何管理页面：
  SMTP_HOST / SMTP_PORT   默认 smtp.gmail.com:587
  REPORT_SENDER           发件人
  GMAIL_APP_PASSWORD      应用专用密码
"""
import os
import smtplib
from email.message import EmailMessage

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SENDER    = os.environ.get("REPORT_SENDER", "jma2369@gmail.com")
PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "")


def configured() -> bool:
    return bool(PASSWORD)


def send(subject: str, recipients: list, text: str, html: str = "",
         attachments: list = None) -> list:
    """发一封信，返回实际投递的收件人列表。

    attachments: [(filename, bytes, maintype, subtype), ...]
    """
    if not PASSWORD:
        raise RuntimeError("未配置 GMAIL_APP_PASSWORD")
    to = [r for r in (recipients or []) if r]
    if not to:
        raise RuntimeError("没有配置收件人")

    msg = EmailMessage()
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(to)
    msg["Subject"] = subject
    msg.set_content(text)
    if html:
        msg.add_alternative(html, subtype="html")
    for fn, data, maintype, subtype in (attachments or []):
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fn)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SENDER, PASSWORD)
        s.send_message(msg, from_addr=SENDER, to_addrs=to)
    return to
