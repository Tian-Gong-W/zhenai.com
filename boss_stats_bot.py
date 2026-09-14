#!/usr/bin/env python3
import json, os, re, sqlite3, sys, tempfile, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

sys.path.insert(0, "/root/zhenai")
import app as zhenai_app

TOKEN = "8749333815:AAHENLL2YmxHtnMuim5m6U5GJ4NJs1P7an4"
DB = "/root/zhenai/zhenai.db"
TZ = ZoneInfo("Asia/Shanghai")
API = f"https://api.telegram.org/bot{TOKEN}"
BASE_TOTAL = 200
COUNT_FROM_DATE = "2026-09-10"
AUTHORIZED_USER_ID = "8400495055"

def counts():
    """按北京时间统计今日、昨日及从设定基数开始的成功推送累计数。"""
    now = datetime.now(TZ)
    today = now.date().isoformat()
    yesterday = (now.date() - timedelta(days=1)).isoformat()
    with sqlite3.connect(DB) as db:
        today_count = db.execute(
            "SELECT COUNT(DISTINCT member_id) FROM pushed_ids "
            "WHERE date(datetime(pushed_at, '+8 hours')) = ?",
            (today,),
        ).fetchone()[0]
        yesterday_count = db.execute(
            "SELECT COUNT(DISTINCT member_id) FROM pushed_ids "
            "WHERE date(datetime(pushed_at, '+8 hours')) = ?",
            (yesterday,),
        ).fetchone()[0]
        cumulative_added = db.execute(
            "SELECT COUNT(DISTINCT member_id) FROM pushed_ids "
            "WHERE date(datetime(pushed_at, '+8 hours')) >= ?",
            (COUNT_FROM_DATE,),
        ).fetchone()[0]
    return now, int(today_count), int(yesterday_count), BASE_TOTAL + int(cumulative_added)

def menu():
    now, today_count, yesterday_count, total = counts()
    text = "\n".join(["━━━━━━━━━━", "『 八 方 来 财 』", "━━━━━━━━━━", f"💭 当日实时：{today_count}条", f"📅 昨日推送：{yesterday_count}条", "━━━━━━━━━━", f"总合：{total}条", f"北京时间：{now:%m月%d日 %H:%M}"])
    return text, None

def send(chat_id, text, markup=None):
    body={"chat_id": chat_id, "text": text}
    if markup: body["reply_markup"] = markup
    requests.post(f"{API}/sendMessage", json=body, timeout=20)

def send_lookup_card(chat_id, member_id, crawler):
    profile = crawler.get_member_profile(member_id)
    if not profile:
        send(chat_id, f"⚠️ ID {member_id} 未获取到用户资料。")
        return
    parsed = crawler.parse_profile(profile) or {}
    card_fd, card_path = tempfile.mkstemp(prefix="boss_lookup_card_", suffix=".png")
    os.close(card_fd)
    avatar_path = None
    try:
        zhenai_app.render_profile_card(profile, card_path)
        avatar_url = zhenai_app.get_avatar_url(profile, {"avatar_url": parsed.get("头像链接", "")})
        avatar_path = zhenai_app.download_avatar(avatar_url)
        caption = " | ".join((
            f"ID:{member_id}", str(parsed.get("昵称", "") or ""),
            f"{parsed.get('年龄', '')}岁", str(parsed.get("居住地", "") or ""),
        ))
        if avatar_path:
            media = [
                {"type": "photo", "media": "attach://avatar", "caption": caption},
                {"type": "photo", "media": "attach://profile_card"},
            ]
            with open(avatar_path, "rb") as avatar_file, open(card_path, "rb") as card_file:
                response = requests.post(
                    f"{API}/sendMediaGroup",
                    data={"chat_id": chat_id, "media": json.dumps(media, ensure_ascii=False)},
                    files={"avatar": ("avatar.jpg", avatar_file, "image/jpeg"), "profile_card": ("profile_card.png", card_file, "image/png")},
                    timeout=60,
                )
        else:
            with open(card_path, "rb") as card_file:
                response = requests.post(
                    f"{API}/sendPhoto", data={"chat_id": chat_id, "caption": caption},
                    files={"photo": ("profile_card.png", card_file, "image/png")}, timeout=45,
                )
        if not response.ok:
            send(chat_id, f"⚠️ ID {member_id} 资料卡发送失败。")
    except Exception:
        send(chat_id, f"⚠️ ID {member_id} 查询失败。")
    finally:
        for path in (card_path, avatar_path):
            if path:
                try:
                    os.unlink(path)
                except OSError:
                    pass

def lookup_ids(chat_id, ids):
    if not zhenai_app.SCREENSHOT_TOKEN:
        send(chat_id, "⚠️ 未配置资料卡专用凭据。")
        return
    send(chat_id, f"🔎 已收到 {len(ids)} 个ID，正在查询资料卡和照片…")
    crawler = zhenai_app.ZhenaiCrawler(token=zhenai_app.SCREENSHOT_TOKEN, seedtoken=zhenai_app.SCREENSHOT_SEED)
    for member_id in ids:
        send_lookup_card(chat_id, member_id, crawler)

def handle(update):
    if "callback_query" in update:
        q=update["callback_query"]
        requests.post(f"{API}/answerCallbackQuery", json={"callback_query_id":q["id"]}, timeout=20)
        return
    msg=update.get("message", {})
    text=msg.get("text", "")
    chat=msg.get("chat", {})
    sender=msg.get("from", {})
    if chat.get("type") == "private" and str(sender.get("id", "")) == AUTHORIZED_USER_ID:
        ids=list(dict.fromkeys(re.findall(r"(?<!\d)\d{5,12}(?!\d)", text)))[:20]
        if ids:
            lookup_ids(str(chat.get("id")), ids)
            return
    if "@TopMan_Boss_Bot" in text or text.strip() in {"/stats", "/统计"}:
        text, markup=menu()
        send(msg["chat"]["id"], text, markup)

def main():
    offset=None
    while True:
        try:
            params={"timeout": 30, "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset is not None: params["offset"]=offset
            response=requests.get(f"{API}/getUpdates", params=params, timeout=40).json()
            for update in response.get("result", []):
                offset=update["update_id"]+1
                handle(update)
        except Exception:
            time.sleep(3)
if __name__ == "__main__": main()
