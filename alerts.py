"""
Optional alerts for Matchbook Trading Bot.
Sends notifications to Telegram, Discord, and/or email on stop-loss, errors, etc.
Configure via ALERT_* env vars. Unset = channel skipped.
"""

import json
import logging
import os
import smtplib
import ssl
import urllib.error
import urllib.parse
import urllib.request
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _format_message(message: str, event_type: str) -> str:
    """Format alert message with header."""
    return f"[Matchbook Bot] {event_type}\n{message}"


def _send_telegram(text: str) -> None:
    """Send message to Telegram. Requires ALERT_TELEGRAM_BOT_TOKEN and ALERT_TELEGRAM_CHAT_ID."""
    token = os.getenv("ALERT_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("ALERT_TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    try:
        url = (
            "https://api.telegram.org/bot"
            + urllib.parse.quote(token)
            + "/sendMessage?chat_id="
            + urllib.parse.quote(chat_id)
            + "&text="
            + urllib.parse.quote(text)
        )
        with urllib.request.urlopen(url, timeout=10) as resp:
            if resp.status != 200:
                logger.warning("Telegram alert failed: status %s", resp.status)
    except urllib.error.URLError as e:
        logger.warning("Telegram alert failed: %s", e)
    except Exception as e:
        logger.warning("Telegram alert failed: %s", e)


def _send_discord(text: str) -> None:
    """Send message to Discord webhook. Requires ALERT_DISCORD_WEBHOOK_URL."""
    url = os.getenv("ALERT_DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        return
    try:
        data = json.dumps({"content": text}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 204):
                logger.warning("Discord alert failed: status %s", resp.status)
    except urllib.error.URLError as e:
        logger.warning("Discord alert failed: %s", e)
    except Exception as e:
        logger.warning("Discord alert failed: %s", e)


def _send_email(text: str) -> None:
    """Send email via SMTP. Requires ALERT_EMAIL_* vars."""
    host = os.getenv("ALERT_EMAIL_SMTP_HOST", "").strip()
    port = os.getenv("ALERT_EMAIL_SMTP_PORT", "587").strip()
    user = os.getenv("ALERT_EMAIL_USER", "").strip()
    password = os.getenv("ALERT_EMAIL_PASSWORD", "").strip()
    from_addr = os.getenv("ALERT_EMAIL_FROM", "").strip()
    to_addrs = os.getenv("ALERT_EMAIL_TO", "").strip()
    if not all([host, user, password, from_addr, to_addrs]):
        return
    try:
        port_int = int(port) if port else 587
        context = ssl.create_default_context()
        to_list = [a.strip() for a in to_addrs.split(",") if a.strip()]
        msg = MIMEText(text, "plain", "utf-8")
        msg["Subject"] = "[Matchbook Bot] Alert"
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_list)
        with smtplib.SMTP(host, port_int) as smtp:
            smtp.starttls(context=context)
            smtp.login(user, password)
            smtp.sendmail(from_addr, to_list, msg.as_string())
    except smtplib.SMTPException as e:
        logger.warning("Email alert failed: %s", e)
    except Exception as e:
        logger.warning("Email alert failed: %s", e)


def send_alert(message: str, event_type: str) -> None:
    """
    Send alert to all configured channels (Telegram, Discord, email).
    Sync, non-blocking. Swallows per-channel errors (log only).
    """
    if not message:
        return
    formatted = _format_message(message, event_type)
    _send_telegram(formatted)
    _send_discord(formatted)
    _send_email(formatted)


def get_configured_channels() -> dict[str, bool]:
    """Return dict of channel -> configured (True/False). For dashboard UI."""
    return {
        "telegram": bool(
            os.getenv("ALERT_TELEGRAM_BOT_TOKEN", "").strip()
            and os.getenv("ALERT_TELEGRAM_CHAT_ID", "").strip()
        ),
        "discord": bool(os.getenv("ALERT_DISCORD_WEBHOOK_URL", "").strip()),
        "email": bool(
            os.getenv("ALERT_EMAIL_SMTP_HOST", "").strip()
            and os.getenv("ALERT_EMAIL_USER", "").strip()
            and os.getenv("ALERT_EMAIL_PASSWORD", "").strip()
            and os.getenv("ALERT_EMAIL_FROM", "").strip()
            and os.getenv("ALERT_EMAIL_TO", "").strip()
        ),
    }
