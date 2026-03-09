import os
import re
import uuid
import base64
import sqlite3
import datetime
import threading
import yt_dlp
import requests
import time
import math
import logging
import telebot
from telebot import types
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    InlineQueryResultVideo, InputTextMessageContent,
    LabeledPrice,
)

# ---------------- LOGGING ---------------- #
import sys
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.stream.reconfigure(encoding="utf-8", errors="replace")
_stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        _stream_handler,
    ],
)
logger = logging.getLogger(__name__)

# ---------------- CONFIG ---------------- #
TOKEN            = "8654482258:AAGrPZHxjfBrKed5KI6i4bAneMg8B7Jy2KQ"   # Вставь токен
ADMIN_ID         = 6708567261          # Твой Telegram ID
FREE_LIMIT       = 10
SUB_LIMIT        = 30
STAR_PRICE       = 100                # Telegram Stars
MAX_FILE_SIZE_MB = 50
REFERRAL_BONUS   = 3
SUB_DAYS         = 30
SUPPORTED_DOMAINS = ["tiktok.com", "vm.tiktok.com", "vt.tiktok.com"]

bot = telebot.TeleBot(TOKEN, parse_mode="Markdown")

# ---------------- TRANSLATIONS ---------------- #
T = {
    "ru": {
        "welcome": "👋 Привет, *{name}*!\n\n🎵 *TikTok Downloader*\n\nСкачиваю TikTok видео *без водяных знаков*\n\n🎁 Бесплатно: *10 видео/день*\n⭐ Premium: *50 видео/день*",
        "welcome_ref": "🎁 Ты перешёл по реферальной ссылке!\nДобро пожаловать в *TikTok Downloader* 🎵",
        "ref_bonus": "🎉 По твоей ссылке зарегистрировался новый пользователь!\n✅ +{bonus} бонусных видео зачислено.",
        "banned": "🚫 Вы заблокированы.",
        "wait": "⏳ Подождите пару секунд",
        "tiktok_only": "❌ Принимаются только ссылки TikTok.\n\nПример: `https://vm.tiktok.com/abc123`",
        "limit_reached": "❌ Лимит на {period} исчерпан!\n\nКупи Premium для большего количества загрузок.",
        "period_day": "день", "period_month": "месяц",
        "searching": "🔍 Ищу видео без водяного знака...",
        "downloading": "⬇️ Загружаю видео...",
        "progress": "⬇️ Загрузка: {pct}%\n{bar}",
        "fallback": "⏳ Подготовка через резервный метод...",
        "too_big": "⚠️ Файл слишком большой ({mb:.1f} MB). Лимит: {max} MB.",
        "done": "✅ *Готово!*\nОсталось сегодня: *{left}*",
        "error_dl": "❌ Не удалось скачать видео.\n\n• Приватный аккаунт\n• Ссылка устарела\n• Временные проблемы сервиса",
        "btn_download": "📥 Скачать видео", "btn_profile": "👤 Профиль",
        "btn_sub": "⭐ Подписка", "btn_referral": "🔗 Пригласить друга (+3 видео)",
        "btn_help": "ℹ️ Помощь", "btn_support": "💬 Поддержка",
        "btn_admin": "⚙️ Админ-панель", "btn_back": "◀️ Назад",
        "btn_menu": "◀️ В меню", "btn_more": "📥 Скачать ещё",
        "btn_buy": "⭐ Купить за 100 Stars", "btn_share": "📤 Поделиться",
        "btn_cancel": "◀️ Отмена", "btn_premium": "⭐ Купить Premium",
        "btn_lang": "🌍 English",
        "profile_title": "👤 *Профиль*", "profile_id": "🆔 ID",
        "profile_used": "📥 Использовано", "profile_left": "✅ Осталось",
        "profile_bonus": "🎁 Бонус рефералов", "profile_refs": "👥 Приглашено",
        "profile_total": "📊 Всего скачано", "profile_sub": "⭐ Подписка",
        "profile_sub_yes": "✅ Premium до {date}", "profile_sub_no": "❌ Нет",
        "profile_joined": "📅 В боте с",
        "sub_title": "⭐ *Premium подписка*",
        "sub_body": "✅ 50 видео в день\n✅ Приоритетная обработка\n✅ Поддержка\n\n💰 Цена: 100 Telegram Stars",
        "payment_ok": "🎉 *Оплата прошла!*\n\nPremium активирован на 30 дней. Наслаждайся! ⭐",
        "sub_expired": "⏰ Срок Premium истёк.\nТы вернулся на бесплатный план (10 видео/день).\n\n⭐ Продлить подписку:",
        "limit_warning": "⚠️ Осталось *{left}* загрузки на сегодня!\n\nКупи Premium чтобы не останавливаться.",
        "ref_title": "🔗 *Реферальная программа*",
        "ref_body": "Приглашай друзей и получай *+{bonus} видео* за каждого!\n\nТвоя ссылка:\n`{link}`\n\n👥 Приглашено: *{count}* чел.\n🎁 Получено: *+{total}* видео\n\n_Бонус приходит автоматически_",
        "help_text": "📖 *Как пользоваться:*\n\n1️⃣ Открой TikTok и скопируй ссылку\n2️⃣ Отправь её сюда\n3️⃣ Получи видео *без водяного знака* 🎬\n\n🔗 *Форматы ссылок:*\n• `https://www.tiktok.com/@user/video/...`\n• `https://vm.tiktok.com/abc123`\n\n🌐 *Inline режим:*\nНапиши `@бот ссылка` в любом чате!\n\n❓ Поддержка: @ceosocialnetwork",
        "download_hint": "📎 *Отправь ссылку на TikTok видео*\n\nПример:\n• `https://vm.tiktok.com/abc123`",
        "top_title": "🏆 *Топ скачиваний*\n\n",
        "top_row": "{medal} {name} — *{count}* видео\n",
        "top_empty": "Пока нет данных",
        "admin_title": "⚙️ *Админ-панель*",
        "admin_stats": "📊 *Статистика*\n\n👥 Пользователей: {users}\n⭐ Premium: {subs}\n📥 Скачиваний: {dl}\n🔇 Забанено: {banned}\n📅 За сегодня: {today_dl}",
        "admin_ban_ask": "🔇 Введи ID для бана:",
        "admin_unban_ask": "🔊 Введи ID для разбана:",
        "admin_sub_ask": "⭐ Введи ID для выдачи подписки:",
        "admin_broadcast_ask": "📢 Введи текст рассылки:",
        "admin_ban_ok": "🔇 Пользователь {id} заблокирован",
        "admin_unban_ok": "🔊 Пользователь {id} разблокирован",
        "admin_sub_ok": "⭐ Подписка выдана пользователю {id}",
        "broadcast_done": "📢 Рассылка: ✅ {ok} | ❌ {fail}",
        "invalid_id": "❌ Неверный ID",
        "btn_stats": "📊 Статистика", "btn_ban": "🔇 Бан", "btn_unban": "🔊 Разбан",
        "btn_givesub": "⭐ Дать подписку", "btn_broadcast": "📢 Рассылка", "btn_top": "🏆 Топ",
    },
    "en": {
        "welcome": "👋 Hey, *{name}*!\n\n🎵 *TikTok Downloader*\n\nDownload TikTok videos *without watermarks*\n\n🎁 Free: *10 videos/day*\n⭐ Premium: *50 videos/day*",
        "welcome_ref": "🎁 You joined via referral!\nWelcome to *TikTok Downloader* 🎵",
        "ref_bonus": "🎉 Someone joined via your link!\n✅ +{bonus} bonus videos added.",
        "banned": "🚫 You are banned.",
        "wait": "⏳ Please wait a moment",
        "tiktok_only": "❌ Only TikTok links accepted.\n\nExample: `https://vm.tiktok.com/abc123`",
        "limit_reached": "❌ {period} limit reached!\n\nBuy Premium for more downloads.",
        "period_day": "daily", "period_month": "monthly",
        "searching": "🔍 Looking for watermark-free video...",
        "downloading": "⬇️ Downloading video...",
        "progress": "⬇️ Downloading: {pct}%\n{bar}",
        "fallback": "⏳ Trying fallback method...",
        "too_big": "⚠️ File too large ({mb:.1f} MB). Limit: {max} MB.",
        "done": "✅ *Done!*\nRemaining today: *{left}*",
        "error_dl": "❌ Failed to download video.\n\n• Private account\n• Expired link\n• Service issues",
        "btn_download": "📥 Download video", "btn_profile": "👤 Profile",
        "btn_sub": "⭐ Subscription", "btn_referral": "🔗 Invite friend (+3 videos)",
        "btn_help": "ℹ️ Help", "btn_support": "💬 Support",
        "btn_admin": "⚙️ Admin panel", "btn_back": "◀️ Back",
        "btn_menu": "◀️ Menu", "btn_more": "📥 Download more",
        "btn_buy": "⭐ Buy for 100 Stars", "btn_share": "📤 Share",
        "btn_cancel": "◀️ Cancel", "btn_premium": "⭐ Buy Premium",
        "btn_lang": "🌍 Русский",
        "profile_title": "👤 *Profile*", "profile_id": "🆔 ID",
        "profile_used": "📥 Used", "profile_left": "✅ Remaining",
        "profile_bonus": "🎁 Referral bonus", "profile_refs": "👥 Friends invited",
        "profile_total": "📊 Total downloads", "profile_sub": "⭐ Subscription",
        "profile_sub_yes": "✅ Premium until {date}", "profile_sub_no": "❌ None",
        "profile_joined": "📅 Member since",
        "sub_title": "⭐ *Premium subscription*",
        "sub_body": "✅ 50 videos/day\n✅ Priority processing\n✅ Support\n\n💰 Price: 100 Telegram Stars",
        "payment_ok": "🎉 *Payment successful!*\n\nPremium activated for 30 days. Enjoy! ⭐",
        "sub_expired": "⏰ Your Premium has expired.\nBack to free plan (10 videos/day).\n\n⭐ Renew:",
        "limit_warning": "⚠️ Only *{left}* downloads left today!\n\nBuy Premium to keep going.",
        "ref_title": "🔗 *Referral program*",
        "ref_body": "Invite friends and get *+{bonus} videos* each!\n\nYour link:\n`{link}`\n\n👥 Invited: *{count}*\n🎁 Earned: *+{total}* videos\n\n_Bonus credited automatically_",
        "help_text": "📖 *How to use:*\n\n1️⃣ Copy TikTok video link\n2️⃣ Send it here\n3️⃣ Get video *without watermark* 🎬\n\n🔗 *Link formats:*\n• `https://www.tiktok.com/@user/video/...`\n• `https://vm.tiktok.com/abc123`\n\n🌐 *Inline mode:*\nType `@bot link` in any chat!\n\n❓ Support: @ceosocialnetwork",
        "download_hint": "📎 *Send a TikTok video link*\n\nExample:\n• `https://vm.tiktok.com/abc123`",
        "top_title": "🏆 *Top downloaders*\n\n",
        "top_row": "{medal} {name} — *{count}* videos\n",
        "top_empty": "No data yet",
        "admin_title": "⚙️ *Admin panel*",
        "admin_stats": "📊 *Stats*\n\n👥 Users: {users}\n⭐ Premium: {subs}\n📥 Downloads: {dl}\n🔇 Banned: {banned}\n📅 Today: {today_dl}",
        "admin_ban_ask": "🔇 Enter user ID to ban:",
        "admin_unban_ask": "🔊 Enter user ID to unban:",
        "admin_sub_ask": "⭐ Enter user ID to give subscription:",
        "admin_broadcast_ask": "📢 Enter broadcast message:",
        "admin_ban_ok": "🔇 User {id} banned",
        "admin_unban_ok": "🔊 User {id} unbanned",
        "admin_sub_ok": "⭐ Subscription given to {id}",
        "broadcast_done": "📢 Broadcast: ✅ {ok} | ❌ {fail}",
        "invalid_id": "❌ Invalid ID",
        "btn_stats": "📊 Stats", "btn_ban": "🔇 Ban", "btn_unban": "🔊 Unban",
        "btn_givesub": "⭐ Give sub", "btn_broadcast": "📢 Broadcast", "btn_top": "🏆 Top",
    },
}

def t(uid: int, key: str, **kwargs) -> str:
    lang = get_lang(uid)
    text = T.get(lang, T["ru"]).get(key, T["ru"].get(key, key))
    return text.format(**kwargs) if kwargs else text

# ---------------- DATABASE ---------------- #
conn = sqlite3.connect("users.db", check_same_thread=False)
db_lock = threading.Lock()
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id      INTEGER PRIMARY KEY,
    downloads_today  INTEGER DEFAULT 0,
    downloads_month  INTEGER DEFAULT 0,
    last_reset       TEXT,
    subscription     INTEGER DEFAULT 0,
    banned           INTEGER DEFAULT 0,
    total_downloads  INTEGER DEFAULT 0,
    joined_at        TEXT,
    bonus_downloads  INTEGER DEFAULT 0,
    referred_by      INTEGER DEFAULT NULL,
    lang             TEXT DEFAULT 'ru',
    sub_expires      TEXT DEFAULT NULL,
    username         TEXT DEFAULT NULL,
    first_name       TEXT DEFAULT NULL
);
CREATE TABLE IF NOT EXISTS download_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER,
    url         TEXT,
    status      TEXT,
    ts          TEXT
);
CREATE TABLE IF NOT EXISTS referrals (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    inviter_id INTEGER,
    invited_id INTEGER UNIQUE,
    ts         TEXT
);
""")
conn.commit()

# ---------------- DB MIGRATION (для старых users.db) ---------------- #
def _migrate_db():
    migrations = [
        ("lang",        "ALTER TABLE users ADD COLUMN lang TEXT DEFAULT 'ru'"),
        ("sub_expires", "ALTER TABLE users ADD COLUMN sub_expires TEXT DEFAULT NULL"),
        ("username",    "ALTER TABLE users ADD COLUMN username TEXT DEFAULT NULL"),
        ("first_name",  "ALTER TABLE users ADD COLUMN first_name TEXT DEFAULT NULL"),
        ("bonus_downloads", "ALTER TABLE users ADD COLUMN bonus_downloads INTEGER DEFAULT 0"),
        ("referred_by", "ALTER TABLE users ADD COLUMN referred_by INTEGER DEFAULT NULL"),
    ]
    cursor.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in cursor.fetchall()}
    for col, sql in migrations:
        if col not in existing:
            try:
                cursor.execute(sql)
                logger.info(f"Migration: added column '{col}'")
            except Exception as e:
                logger.warning(f"Migration warning for '{col}': {e}")
    conn.commit()

_migrate_db()

# ---------------- USER FUNCTIONS ---------------- #
def get_user(uid: int) -> tuple:
    with db_lock:
        cursor.execute("SELECT * FROM users WHERE telegram_id=?", (uid,))
        user = cursor.fetchone()
        if not user:
            today = datetime.date.today().isoformat()
            cursor.execute("INSERT INTO users (telegram_id, last_reset, joined_at) VALUES (?,?,?)", (uid, today, today))
            conn.commit()
            cursor.execute("SELECT * FROM users WHERE telegram_id=?", (uid,))
            user = cursor.fetchone()
        return user

def get_lang(uid: int) -> str:
    with db_lock:
        cursor.execute("SELECT lang FROM users WHERE telegram_id=?", (uid,))
        row = cursor.fetchone()
        return row[0] if row and row[0] else "ru"

def set_lang(uid: int, lang: str):
    with db_lock:
        cursor.execute("UPDATE users SET lang=? WHERE telegram_id=?", (lang, uid))
        conn.commit()

def update_user_info(uid: int, username, first_name):
    with db_lock:
        cursor.execute("UPDATE users SET username=?, first_name=? WHERE telegram_id=?", (username, first_name, uid))
        conn.commit()

def reset_daily(user: tuple):
    today = datetime.date.today().isoformat()
    if user[3] != today:
        with db_lock:
            cursor.execute("UPDATE users SET downloads_today=0, last_reset=? WHERE telegram_id=?", (today, user[0]))
            conn.commit()

def check_sub_expired(user: tuple) -> bool:
    if not user[4]: return False
    sub_expires = user[11] if len(user) > 11 else None
    if sub_expires and datetime.date.today().isoformat() > sub_expires:
        with db_lock:
            cursor.execute("UPDATE users SET subscription=0, sub_expires=NULL WHERE telegram_id=?", (user[0],))
            conn.commit()
        return True
    return False

def check_limits(user: tuple) -> bool:
    bonus = user[8] if len(user) > 8 else 0
    if user[4]: return user[1] < (SUB_LIMIT + bonus)
    return user[1] < (FREE_LIMIT + bonus)

def get_remaining(user: tuple) -> int:
    bonus = user[8] if len(user) > 8 else 0
    if user[4]: return max(0, SUB_LIMIT + bonus - user[1])
    return max(0, FREE_LIMIT + bonus - user[1])

def update_usage(user: tuple):
    with db_lock:
        cursor.execute("UPDATE users SET downloads_today=downloads_today+1, downloads_month=downloads_month+1, total_downloads=total_downloads+1 WHERE telegram_id=?", (user[0],))
        conn.commit()

def log_download(uid: int, url: str, status: str):
    with db_lock:
        cursor.execute("INSERT INTO download_log (telegram_id, url, status, ts) VALUES (?,?,?,?)", (uid, url, status, datetime.datetime.now().isoformat()))
        conn.commit()

def get_stats() -> dict:
    with db_lock:
        cursor.execute("SELECT COUNT(*) FROM users"); total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE subscription=1"); sub_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM download_log WHERE status IN ('ok','ok_inline')"); total_dl = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE banned=1"); banned = cursor.fetchone()[0]
        today = datetime.date.today().isoformat()
        cursor.execute("SELECT COUNT(*) FROM download_log WHERE ts LIKE ?", (f"{today}%",)); today_dl = cursor.fetchone()[0]
    return {"total_users": total_users, "sub_users": sub_users, "total_downloads": total_dl, "banned": banned, "today_dl": today_dl}

def get_top_users(limit: int = 10) -> list:
    with db_lock:
        cursor.execute("SELECT telegram_id, first_name, username, total_downloads FROM users WHERE banned=0 ORDER BY total_downloads DESC LIMIT ?", (limit,))
        return cursor.fetchall()

# ---------------- REFERRAL ---------------- #
def get_referral_stats(uid: int) -> dict:
    with db_lock:
        cursor.execute("SELECT COUNT(*) FROM referrals WHERE inviter_id=?", (uid,))
        count = cursor.fetchone()[0]
    return {"count": count, "bonus": count * REFERRAL_BONUS}

def apply_referral(inviter_id: int, invited_id: int) -> bool:
    with db_lock:
        cursor.execute("SELECT id FROM referrals WHERE invited_id=?", (invited_id,))
        if cursor.fetchone() or inviter_id == invited_id: return False
        ts = datetime.datetime.now().isoformat()
        cursor.execute("INSERT INTO referrals (inviter_id, invited_id, ts) VALUES (?,?,?)", (inviter_id, invited_id, ts))
        cursor.execute("UPDATE users SET bonus_downloads=bonus_downloads+? WHERE telegram_id=?", (REFERRAL_BONUS, inviter_id))
        cursor.execute("UPDATE users SET referred_by=? WHERE telegram_id=?", (inviter_id, invited_id))
        conn.commit()
    return True

# ---------------- ANTISPAM ---------------- #
last_req: dict = {}
spam_lock = threading.Lock()

def anti_spam(uid: int, cooldown: float = 3.0) -> bool:
    now = time.time()
    with spam_lock:
        if uid in last_req and now - last_req[uid] < cooldown: return False
        last_req[uid] = now
    return True

# ---------------- URL VALIDATION ---------------- #
def is_supported_url(url: str) -> bool:
    return any(d in url for d in SUPPORTED_DOMAINS)

# ---------------- WATERMARK REMOVAL — 5 API CHAIN ---------------- #
def _api_tikwm(url):
    r = requests.get(f"https://tikwm.com/api/?url={url}", timeout=10).json()
    v = r.get("data", {}).get("play")
    return v if v and v.startswith("http") else None

def _api_musicaldown(url):
    s = requests.Session(); s.headers.update({"User-Agent": "Mozilla/5.0"})
    page = s.get("https://musicaldown.com/", timeout=10)
    m = re.search(r'name="([^"]+)" value="([^"]+)"', page.text)
    if not m: return None
    r = s.post("https://musicaldown.com/download", data={"id": url, m.group(1): m.group(2)}, timeout=10)
    links = re.findall(r'href="(https://[^"]+\.mp4[^"]*)"', r.text)
    clean = [l for l in links if "wm" not in l.lower()]
    return clean[0] if clean else (links[0] if links else None)

def _api_snaptik(url):
    s = requests.Session(); s.headers.update({"User-Agent": "Mozilla/5.0"})
    page = s.get("https://snaptik.app/", timeout=10)
    m = re.search(r'name="token" value="([^"]+)"', page.text)
    if not m: return None
    r = s.post("https://snaptik.app/abc2.php", data={"url": url, "token": m.group(1)}, timeout=10)
    sm = re.search(r'eval\(atob\("([^"]+)"\)', r.text)
    if not sm: return None
    decoded = base64.b64decode(sm.group(1)).decode("utf-8", errors="ignore")
    links = re.findall(r'https://[^\s"\'<>]+\.mp4[^\s"\'<>]*', decoded)
    clean = [l for l in links if "watermark" not in l.lower() and "wm" not in l.lower()]
    return clean[0] if clean else (links[0] if links else None)

def _api_tikmate(url):
    r = requests.post("https://tikmate.online/api/lookup", data={"url": url}, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
    token, vid_id = r.get("token"), r.get("id")
    return f"https://tikmate.online/download/{token}/{vid_id}.mp4?hd=1" if token and vid_id else None

def _api_ttsave(url):
    s = requests.Session(); s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://ttsave.app/"})
    r = s.post("https://ttsave.app/download", json={"query": url, "language_id": "1"}, timeout=10)
    links = re.findall(r'https://[^\s"\'<>]+\.mp4[^\s"\'<>]*', r.text)
    return links[0] if links else None

WATERMARK_APIS = [("tikwm", _api_tikwm), ("musicaldown", _api_musicaldown), ("snaptik", _api_snaptik), ("tikmate", _api_tikmate), ("ttsave", _api_ttsave)]

def remove_watermark_api(url: str):
    for name, func in WATERMARK_APIS:
        try:
            result = func(url)
            if result: logger.info(f"✅ via {name}"); return result
            logger.warning(f"⚠️ {name}: no result")
        except Exception as e: logger.warning(f"❌ {name}: {e}")
    return None

def get_tikwm_meta(url: str) -> dict:
    try:
        r = requests.get(f"https://tikwm.com/api/?url={url}", timeout=10).json()
        d = r.get("data", {})
        return {"thumb": d.get("cover") or "https://www.tiktok.com/favicon.ico", "title": (d.get("title") or "TikTok video")[:64]}
    except Exception:
        return {"thumb": "https://www.tiktok.com/favicon.ico", "title": "TikTok video"}

# ---------------- MENUS ---------------- #
def main_menu(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton(t(uid, "btn_download"), callback_data="download"))
    kb.add(
        InlineKeyboardButton(t(uid, "btn_profile"), callback_data="profile"),
        InlineKeyboardButton(t(uid, "btn_sub"), callback_data="sub"),
    )
    kb.add(InlineKeyboardButton(t(uid, "btn_referral"), callback_data="referral"))
    kb.add(
        InlineKeyboardButton(t(uid, "btn_help"), callback_data="help"),
        InlineKeyboardButton(t(uid, "btn_support"), url="https://t.me/ceosocialnetwork"),
    )
    kb.add(InlineKeyboardButton(t(uid, "btn_lang"), callback_data="toggle_lang"))
    if uid == ADMIN_ID:
        kb.add(InlineKeyboardButton(t(uid, "btn_admin"), callback_data="admin_menu"))
    return kb

def admin_menu_kb(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(t(uid, "btn_stats"), callback_data="admin_stats"),
        InlineKeyboardButton(t(uid, "btn_top"), callback_data="admin_top"),
    )
    kb.add(
        InlineKeyboardButton(t(uid, "btn_ban"), callback_data="admin_ban"),
        InlineKeyboardButton(t(uid, "btn_unban"), callback_data="admin_unban"),
    )
    kb.add(
        InlineKeyboardButton(t(uid, "btn_givesub"), callback_data="admin_givesub"),
        InlineKeyboardButton(t(uid, "btn_broadcast"), callback_data="admin_broadcast"),
    )
    kb.add(InlineKeyboardButton(t(uid, "btn_back"), callback_data="back_main"))
    return kb

def back_kb(uid: int, cb: str = "back_main") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(t(uid, "btn_back"), callback_data=cb))
    return kb

def menu_kb(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(t(uid, "btn_menu"), callback_data="back_main"))
    return kb

# ---------------- PROFILE TEXT ---------------- #
def build_profile_text(uid: int, user: tuple) -> str:
    used = user[1]
    bonus = user[8] if len(user) > 8 else 0
    limit = SUB_LIMIT + bonus if user[4] else FREE_LIMIT + bonus
    period = t(uid, "period_day")
    remaining = get_remaining(user)
    ref_stats = get_referral_stats(uid)
    sub_str = t(uid, "profile_sub_yes", date=user[11]) if user[4] else t(uid, "profile_sub_no")
    lines = [
        f"{t(uid, 'profile_title')}\n",
        f"{t(uid, 'profile_id')}: `{uid}`",
        f"{t(uid, 'profile_used')}: {used}/{limit} ({period})",
        f"{t(uid, 'profile_left')}: {remaining}",
    ]
    if bonus > 0: lines.append(f"{t(uid, 'profile_bonus')}: +{bonus}")
    lines += [
        f"{t(uid, 'profile_refs')}: {ref_stats['count']}",
        f"{t(uid, 'profile_total')}: {user[6]}",
        f"{t(uid, 'profile_sub')}: {sub_str}",
        f"{t(uid, 'profile_joined')}: {user[7] or '—'}",
    ]
    return "\n".join(lines)

# ---------------- AUTO SUB EXPIRY (background thread) ---------------- #
def sub_expiry_checker():
    while True:
        try:
            today = datetime.date.today().isoformat()
            with db_lock:
                cursor.execute("SELECT telegram_id FROM users WHERE subscription=1 AND sub_expires IS NOT NULL AND sub_expires < ?", (today,))
                expired = [r[0] for r in cursor.fetchall()]
                for uid in expired:
                    cursor.execute("UPDATE users SET subscription=0, sub_expires=NULL WHERE telegram_id=?", (uid,))
                if expired: conn.commit()
            for uid in expired:
                try:
                    kb = InlineKeyboardMarkup()
                    kb.add(InlineKeyboardButton(t(uid, "btn_buy"), callback_data="buy"))
                    bot.send_message(uid, t(uid, "sub_expired"), reply_markup=kb)
                except Exception: pass
            if expired: logger.info(f"Expired subs reset: {len(expired)}")
        except Exception as e:
            logger.error(f"sub_expiry_checker error: {e}")
        # Проверяем раз в час
        time.sleep(3600)

# ================================================================
#                        HANDLERS
# ================================================================

# ---------------- /start ---------------- #
@bot.message_handler(commands=["start"])
def cmd_start(msg: types.Message):
    uid = msg.from_user.id
    update_user_info(uid, msg.from_user.username, msg.from_user.first_name)
    user = get_user(uid)
    if user[5]:
        bot.send_message(uid, t(uid, "banned")); return

    # Referral check
    args = msg.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            inviter_id = int(args[1][4:])
            inviter = get_user(inviter_id)
            if not inviter[5]:
                if apply_referral(inviter_id, uid):
                    try: bot.send_message(inviter_id, t(inviter_id, "ref_bonus", bonus=REFERRAL_BONUS))
                    except Exception: pass
                    bot.send_message(uid, t(uid, "welcome_ref"))
        except (ValueError, IndexError): pass

    bot.send_message(uid, t(uid, "welcome", name=msg.from_user.first_name), reply_markup=main_menu(uid))

# ---------------- CALLBACK QUERY HANDLER ---------------- #
@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call: types.CallbackQuery):
    uid = call.from_user.id
    user = get_user(uid)
    data = call.data
    bot.answer_callback_query(call.id)

    # Check sub expiry silently
    if check_sub_expired(user):
        user = get_user(uid)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(t(uid, "btn_buy"), callback_data="buy"))
        try: bot.send_message(uid, t(uid, "sub_expired"), reply_markup=kb)
        except Exception: pass

    def edit(text, markup=None):
        try:
            bot.edit_message_text(text, uid, call.message.message_id, parse_mode="Markdown", reply_markup=markup)
        except Exception:
            bot.send_message(uid, text, parse_mode="Markdown", reply_markup=markup)

    if data == "download":
        edit(t(uid, "download_hint"), back_kb(uid))

    elif data == "profile":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton(t(uid, "btn_referral"), callback_data="referral"))
        kb.add(InlineKeyboardButton(t(uid, "btn_back"), callback_data="back_main"))
        edit(build_profile_text(uid, user), kb)

    elif data == "sub":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton(t(uid, "btn_buy"), callback_data="buy"))
        kb.add(InlineKeyboardButton(t(uid, "btn_back"), callback_data="back_main"))
        edit(f"{t(uid, 'sub_title')}\n\n{t(uid, 'sub_body')}", kb)

    elif data == "buy":
        bot.send_invoice(
            uid,
            title="Premium",
            description="50 videos/day",
            invoice_payload="sub",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Premium", amount=STAR_PRICE)],
        )

    elif data == "referral":
        me = bot.get_me()
        ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
        ref_stats = get_referral_stats(uid)
        text = f"{t(uid, 'ref_title')}\n\n{t(uid, 'ref_body', bonus=REFERRAL_BONUS, link=ref_link, count=ref_stats['count'], total=ref_stats['bonus'])}"
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton(t(uid, "btn_share"), url=f"https://t.me/share/url?url={ref_link}&text=TikTok+без+водяных+знаков!"))
        kb.add(InlineKeyboardButton(t(uid, "btn_back"), callback_data="back_main"))
        edit(text, kb)

    elif data == "help":
        edit(t(uid, "help_text"), back_kb(uid))

    elif data == "toggle_lang":
        new_lang = "en" if get_lang(uid) == "ru" else "ru"
        set_lang(uid, new_lang)
        edit(t(uid, "welcome", name=call.from_user.first_name), main_menu(uid))

    elif data == "back_main":
        edit(t(uid, "welcome", name=call.from_user.first_name), main_menu(uid))

    # -------- ADMIN --------
    elif data == "admin_menu":
        if uid != ADMIN_ID: return
        edit(t(uid, "admin_title"), admin_menu_kb(uid))

    elif data == "admin_stats":
        if uid != ADMIN_ID: return
        s = get_stats()
        text = t(uid, "admin_stats", users=s["total_users"], subs=s["sub_users"], dl=s["total_downloads"], banned=s["banned"], today_dl=s["today_dl"])
        edit(text, back_kb(uid, "admin_menu"))

    elif data == "admin_top":
        if uid != ADMIN_ID: return
        medals = ["🥇","🥈","🥉"] + ["🏅"]*7
        rows = get_top_users(10)
        text = t(uid, "top_title")
        if rows:
            for i, (tid, fname, uname, cnt) in enumerate(rows):
                name = fname or uname or str(tid)
                text += t(uid, "top_row", medal=medals[i], name=name, count=cnt)
        else:
            text += t(uid, "top_empty")
        edit(text, back_kb(uid, "admin_menu"))

    elif data == "admin_ban":
        if uid != ADMIN_ID: return
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_cancel"), callback_data="admin_menu"))
        edit(t(uid, "admin_ban_ask"), kb)
        bot.register_next_step_handler(call.message, admin_action_handler, action="ban")

    elif data == "admin_unban":
        if uid != ADMIN_ID: return
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_cancel"), callback_data="admin_menu"))
        edit(t(uid, "admin_unban_ask"), kb)
        bot.register_next_step_handler(call.message, admin_action_handler, action="unban")

    elif data == "admin_givesub":
        if uid != ADMIN_ID: return
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_cancel"), callback_data="admin_menu"))
        edit(t(uid, "admin_sub_ask"), kb)
        bot.register_next_step_handler(call.message, admin_action_handler, action="givesub")

    elif data == "admin_broadcast":
        if uid != ADMIN_ID: return
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_cancel"), callback_data="admin_menu"))
        edit(t(uid, "admin_broadcast_ask"), kb)
        bot.register_next_step_handler(call.message, admin_action_handler, action="broadcast")

# ---------------- ADMIN ACTION HANDLER ---------------- #
def admin_action_handler(msg: types.Message, action: str):
    uid = ADMIN_ID
    text = msg.text.strip() if msg.text else ""
    try: bot.delete_message(uid, msg.message_id)
    except Exception: pass

    if action == "broadcast":
        with db_lock:
            cursor.execute("SELECT telegram_id FROM users WHERE banned=0")
            ids = [r[0] for r in cursor.fetchall()]
        ok = fail = 0
        for target in ids:
            try: bot.send_message(target, text, parse_mode="Markdown"); ok += 1; time.sleep(0.05)
            except Exception: fail += 1
        bot.send_message(uid, t(uid, "broadcast_done", ok=ok, fail=fail))
        return

    try: target_id = int(text)
    except ValueError:
        bot.send_message(uid, t(uid, "invalid_id")); return

    if action == "ban":
        with db_lock:
            cursor.execute("UPDATE users SET banned=1 WHERE telegram_id=?", (target_id,)); conn.commit()
        bot.send_message(uid, t(uid, "admin_ban_ok", id=target_id))
    elif action == "unban":
        with db_lock:
            cursor.execute("UPDATE users SET banned=0 WHERE telegram_id=?", (target_id,)); conn.commit()
        bot.send_message(uid, t(uid, "admin_unban_ok", id=target_id))
    elif action == "givesub":
        expires = (datetime.date.today() + datetime.timedelta(days=SUB_DAYS)).isoformat()
        with db_lock:
            cursor.execute("UPDATE users SET subscription=1, downloads_month=0, sub_expires=? WHERE telegram_id=?", (expires, target_id)); conn.commit()
        bot.send_message(uid, t(uid, "admin_sub_ok", id=target_id))

# ---------------- PAYMENT HANDLERS ---------------- #
@bot.pre_checkout_query_handler(func=lambda q: True)
def precheckout(query: types.PreCheckoutQuery):
    bot.answer_pre_checkout_query(query.id, ok=True)

@bot.message_handler(content_types=["successful_payment"])
def successful_payment(msg: types.Message):
    uid = msg.from_user.id
    expires = (datetime.date.today() + datetime.timedelta(days=SUB_DAYS)).isoformat()
    with db_lock:
        cursor.execute("UPDATE users SET subscription=1, downloads_month=0, sub_expires=? WHERE telegram_id=?", (expires, uid))
        conn.commit()
    bot.send_message(uid, t(uid, "payment_ok"))

# ---------------- INLINE MODE ---------------- #
@bot.inline_handler(func=lambda q: True)
def inline_handler(query: types.InlineQuery):
    uid = query.from_user.id
    url = query.query.strip()

    if not url:
        bot.answer_inline_query(query.id, [], switch_pm_text="📎 Вставь ссылку TikTok", switch_pm_parameter="inline_help", cache_time=0); return
    if not is_supported_url(url):
        bot.answer_inline_query(query.id, [], switch_pm_text="❌ Только TikTok ссылки", switch_pm_parameter="inline_help", cache_time=0); return

    user = get_user(uid)
    if user[5]:
        bot.answer_inline_query(query.id, [], cache_time=0); return

    reset_daily(user); user = get_user(uid)
    if not check_limits(user):
        bot.answer_inline_query(query.id, [], switch_pm_text="❌ Лимит исчерпан — купи Premium", switch_pm_parameter="sub", cache_time=0); return

    try:
        video_url = remove_watermark_api(url)
        if not video_url:
            bot.answer_inline_query(query.id, [], switch_pm_text="❌ Не удалось получить видео", switch_pm_parameter="inline_help", cache_time=0); return
        meta = get_tikwm_meta(url)
        result = InlineQueryResultVideo(
            id=str(uuid.uuid4()),
            video_url=video_url,
            mime_type="video/mp4",
            thumb_url=meta["thumb"],
            title=f"🎵 {meta['title']}",
            description="Без водяного знака ✅",
            caption="📥 @YourBotUsername",
            input_message_content=None,
        )
        bot.answer_inline_query(query.id, [result], cache_time=30)
        update_usage(user); log_download(uid, url, "ok_inline")
    except Exception as e:
        logger.error(f"Inline error {uid}: {e}")
        bot.answer_inline_query(query.id, [], switch_pm_text="❌ Ошибка", switch_pm_parameter="inline_help", cache_time=0)

# ---------------- VIDEO DOWNLOAD HANDLER ---------------- #
@bot.message_handler(func=lambda m: m.content_type == "text" and not m.text.startswith("/"))
def handle_link(msg: types.Message):
    uid = msg.from_user.id

    if not anti_spam(uid):
        err = bot.send_message(uid, t(uid, "wait"))
        time.sleep(3)
        try: bot.delete_message(uid, err.message_id)
        except Exception: pass
        try: bot.delete_message(uid, msg.message_id)
        except Exception: pass
        return

    update_user_info(uid, msg.from_user.username, msg.from_user.first_name)
    user = get_user(uid)

    if user[5]:
        bot.send_message(uid, t(uid, "banned")); return

    if check_sub_expired(user):
        user = get_user(uid)
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_buy"), callback_data="buy"))
        bot.send_message(uid, t(uid, "sub_expired"), reply_markup=kb)

    url = msg.text.strip()
    try: bot.delete_message(uid, msg.message_id)
    except Exception: pass

    if not is_supported_url(url):
        bot.send_message(uid, t(uid, "tiktok_only"), reply_markup=menu_kb(uid)); return

    reset_daily(user); user = get_user(uid)

    if not check_limits(user):
        period = t(uid, "period_month") if user[4] else t(uid, "period_day")
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton(t(uid, "btn_premium"), callback_data="sub"))
        kb.add(InlineKeyboardButton(t(uid, "btn_menu"), callback_data="back_main"))
        bot.send_message(uid, t(uid, "limit_reached", period=period), reply_markup=kb); return

    # Warn at 2 remaining
    remaining = get_remaining(user)
    if remaining == 2 and not user[4]:
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_premium"), callback_data="sub"))
        bot.send_message(uid, t(uid, "limit_warning", left=remaining), reply_markup=kb)

    # Download in a thread so bot doesn't freeze
    threading.Thread(target=download_worker, args=(uid, url, user), daemon=True).start()

def download_worker(uid: int, url: str, user: tuple):
    filename = None
    status_msg = bot.send_message(uid, t(uid, "searching"))

    try:
        video_url = remove_watermark_api(url)
        if video_url:
            os.makedirs("downloads", exist_ok=True)
            bot.edit_message_text(t(uid, "downloading"), uid, status_msg.message_id)
            r = requests.get(video_url, timeout=60, stream=True)
            filename = f"downloads/{uid}_{int(time.time())}.mp4"
            total = int(r.headers.get("content-length", 0))
            downloaded = 0; last_pct = -1
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk); downloaded += len(chunk)
                    if total:
                        pct = int(downloaded / total * 100)
                        if pct != last_pct and pct % 10 == 0:
                            last_pct = pct
                            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                            try: bot.edit_message_text(t(uid, "progress", pct=pct, bar=bar), uid, status_msg.message_id)
                            except Exception: pass

        # yt-dlp fallback
        if not filename or not os.path.exists(filename):
            bot.edit_message_text(t(uid, "fallback"), uid, status_msg.message_id)
            os.makedirs("downloads", exist_ok=True)
            ydl_opts = {
                "format": "best[ext=mp4]/best",
                "outtmpl": f"downloads/{uid}_%(id)s.%(ext)s",
                "noplaylist": True, "quiet": True, "no_warnings": True,
                "concurrent_fragment_downloads": 4, "merge_output_format": None,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info)

        if not filename or not os.path.exists(filename):
            raise FileNotFoundError("File not downloaded")

        size_mb = os.path.getsize(filename) / 1024 / 1024
        if size_mb > MAX_FILE_SIZE_MB:
            bot.edit_message_text(t(uid, "too_big", mb=size_mb, max=MAX_FILE_SIZE_MB), uid, status_msg.message_id)
            os.remove(filename); return

        try: bot.delete_message(uid, status_msg.message_id)
        except Exception: pass

        remaining_after = max(0, get_remaining(user) - 1)
        kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_more"), callback_data="download"))
        with open(filename, "rb") as f:
            bot.send_video(uid, f, caption=t(uid, "done", left=remaining_after), reply_markup=kb)

        update_usage(user); log_download(uid, url, "ok")

    except Exception as e:
        logger.error(f"Download error {uid}: {e}")
        try:
            kb = InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(t(uid, "btn_menu"), callback_data="back_main"))
            bot.edit_message_text(t(uid, "error_dl"), uid, status_msg.message_id, reply_markup=kb)
        except Exception: pass
        log_download(uid, url, f"error: {e}")
    finally:
        if filename and os.path.exists(filename):
            try: os.remove(filename)
            except Exception: pass

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    # Фоновый поток проверки истёкших подписок
    threading.Thread(target=sub_expiry_checker, daemon=True).start()
    logger.info("🤖 Bot started (pyTelegramBotAPI)")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)