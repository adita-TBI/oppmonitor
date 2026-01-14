import os
import re
import sqlite3
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from hashlib import sha256
from typing import List, Dict, Any, Tuple

import feedparser
import yaml
from dateutil import parser as dateparser


DB_PATH = os.environ.get("DB_PATH", "data/seen.sqlite3")


@dataclass
class Item:
    source: str
    title: str
    link: str
    published: str
    summary: str
    tags: List[str]
    score: int


def load_config(path: str = "sources.yml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen (
                id TEXT PRIMARY KEY,
                first_seen_utc TEXT NOT NULL
            )
        """)
        conn.commit()


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def item_id(title: str, link: str) -> str:
    base = normalize_text(title) + "|" + normalize_text(link)
    return sha256(base.encode("utf-8")).hexdigest()


def already_seen(iid: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT 1 FROM seen WHERE id = ?", (iid,))
        return cur.fetchone() is not None


def mark_seen(iid: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen (id, first_seen_utc) VALUES (?, ?)",
            (iid, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()


def score_item(text: str, cfg: Dict[str, Any]) -> Tuple[bool, int]:
    kw = cfg["keywords"]
    t = normalize_text(text)

    if any(normalize_text(x) in t for x in kw.get("exclude_any", [])):
        return (False, 0)

    must = kw.get("must_have_any", [])
    if must and not any(normalize_text(x) in t for x in must):
        return (False, 0)

    score = 60  # baseline if must-have matched
    nice = kw.get("nice_to_have_any", [])
    score += 5 * sum(1 for x in nice if normalize_text(x) in t)

    # Bonus for typical procurement words
    bonus_terms = ["rfp", "tender", "eoi", "expression of interest", "procurement", "bid", "request for proposal"]
    score += 5 * sum(1 for x in bonus_terms if x in t)

    return (True, min(score, 100))


def parse_date(entry: Dict[str, Any]) -> str:
    # feedparser entries may have different fields
    for key in ("published", "updated", "created"):
        if key in entry and entry[key]:
            try:
                dt = dateparser.parse(entry[key])
                return dt.date().isoformat()
            except Exception:
                pass
    return ""


def fetch_rss_source(src: Dict[str, Any], cfg: Dict[str, Any]) -> List[Item]:
    d = feedparser.parse(src["url"])
    items: List[Item] = []
    for e in d.entries[:200]:
        title = e.get("title", "").strip()
        link = e.get("link", "").strip()
        summary = (e.get("summary", "") or e.get("description", "") or "").strip()
        published = parse_date(e)
        blob = f"{title}\n{summary}"

        keep, score = score_item(blob, cfg)
        if not keep:
            continue

        iid = item_id(title, link)
        if already_seen(iid):
            continue

        items.append(Item(
            source=src["name"],
            title=title or "(no title)",
            link=link,
            published=published,
            summary=re.sub("<[^<]+?>", " ", summary)[:500],
            tags=src.get("tags", []),
            score=score
        ))
        mark_seen(iid)

    return items


def build_email_html(items: List[Item], cfg: Dict[str, Any]) -> str:
    today = datetime.now().date().isoformat()
    items_sorted = sorted(items, key=lambda x: (x.score, x.published), reverse=True)[: cfg["email"]["max_items"]]

    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for it in items_sorted:
        tags = ", ".join(it.tags) if it.tags else ""
        rows.append(f"""
          <tr>
            <td style="padding:8px;border-bottom:1px solid #eee;">
              <div style="font-size:14px;"><b>{esc(it.title)}</b></div>
              <div style="font-size:12px;color:#555;">{esc(it.source)} {('— ' + esc(tags)) if tags else ''}</div>
              <div style="font-size:12px;color:#555;">Published: {esc(it.published)}</div>
              <div style="margin:6px 0;font-size:12px;color:#333;">{esc(it.summary)}</div>
              <div><a href="{esc(it.link)}">Open</a> &nbsp; <span style="color:#999;">Score: {it.score}</span></div>
            </td>
          </tr>
        """)

    if not rows:
        body = f"<p>No new matches found for {today}.</p>"
    else:
        body = f"""
        <p>New opportunities found: <b>{len(items_sorted)}</b> (showing up to {cfg["email"]["max_items"]}).</p>
        <table style="border-collapse:collapse;width:100%;">{''.join(rows)}</table>
        """

    return f"""
    <html>
      <body style="font-family:Arial, sans-serif;">
        <h2 style="margin:0 0 8px 0;">{cfg["email"]["subject_prefix"]} — {today}</h2>
        {body}
        <p style="font-size:11px;color:#999;margin-top:16px;">
          Generated by apac-opportunity-monitor (open-source).
        </p>
      </body>
    </html>
    """


def send_email(html: str, subject: str):
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    mail_from = os.environ["MAIL_FROM"]
    mail_to = os.environ["MAIL_TO"]  # comma-separated

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to

    msg.attach(MIMEText(html, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_pass)
        server.sendmail(mail_from, mail_to.split(","), msg.as_string())


def main():
    cfg = load_config()
    ensure_db()

    all_items: List[Item] = []
    for src in cfg.get("sources", []):
        if src.get("type") == "rss":
            all_items.extend(fetch_rss_source(src, cfg))
        else:
            # Later: implement simple HTML list scraping here
            continue

    html = build_email_html(all_items, cfg)
    today = datetime.now().date().isoformat()
    subject = f'{cfg["email"]["subject_prefix"]} — {today}'
    send_email(html, subject)


if __name__ == "__main__":
    main()
