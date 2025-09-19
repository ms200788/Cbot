#!/usr/bin/env python3
# Cbot.py
# Single-file CBot + MBot manager (Render-ready) using environment variables.
# - Tokens and OWNER_ID are read from environment variables (no hardcoding).
# - Limits MBots to 20 (configurable).
# - Flask for webhooks and /ping endpoint for UptimeRobot.
# - python-telegram-bot v13 style Dispatcher usage via webhooks.
# - SQLite persistence: one cbot.db and per-mbot mbot_<alias>.db
# - Upload "bag" sessions (persistent), deep-links work after restarts.
# - Auto-delete affects delivered messages only; DB keeps files forever.
# - Broadcast supports copy|forward (forward preserves channel identity only).
# - /removebot deletes MBot DB and files directory permanently; /stopbot freezes; /runbot resumes.
#
# Deployment:
# - Set environment variables in Render:
#     CBOT_TOKEN (required)
#     OWNER_ID  (required)  -- numeric
#     WEBHOOK_BASE (optional) e.g. https://myservice.onrender.com
# - Deploy as a Web Service. Start command: gunicorn owner_mbots_render_env:app
#
# NOTE: Do not put tokens in repo. Use Render environment variables.
# -----------------------------------------------------------------------------

import os
import sys
import time
import uuid
import sqlite3
import threading
import logging
import traceback
import shutil
import base64
from functools import wraps
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, request, jsonify, abort

# -------------------------
# Read configuration from environment variables (secure)
# -------------------------
CBOT_TOKEN = os.getenv("CBOT_TOKEN")  # must be set in Render env
OWNER_ID_RAW = os.getenv("OWNER_ID")  # must be set to numeric string
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE")  # optional, used to build deep-links for webhooks

if OWNER_ID_RAW:
    try:
        OWNER_ID = int(OWNER_ID_RAW)
    except Exception:
        OWNER_ID = None
else:
    OWNER_ID = None

# Storage folder (Render gives ephemeral /tmp, but repository root may persist between deploys)
BASE_DIR = os.getenv("BASE_DIR", os.path.join(os.path.expanduser("~"), "owner_mbots_data"))
if not os.path.exists(BASE_DIR):
    try:
        os.makedirs(BASE_DIR, exist_ok=True)
    except Exception:
        BASE_DIR = os.path.abspath(".")
CBOT_DB = os.path.join(BASE_DIR, "cbot.db")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("owner_mbots_env")

# -------------------------
# imghdr shim (Pillow fallback) - helpful for some environments
# -------------------------
try:
    import imghdr  # type: ignore
except Exception:
    try:
        import types as _types
        from PIL import Image as _PILImage  # type: ignore

        fake_imghdr = _types.ModuleType("imghdr")

        def _what(file, h=None):
            try:
                if hasattr(file, "read"):
                    pos = None
                    try:
                        pos = file.tell()
                    except Exception:
                        pos = None
                    try:
                        if pos is not None:
                            file.seek(0)
                        img = _PILImage.open(file)
                        fmt = img.format
                        try:
                            if pos is not None:
                                file.seek(pos)
                        except Exception:
                            pass
                        return fmt.lower() if fmt else None
                    except Exception:
                        try:
                            if pos is not None:
                                file.seek(pos)
                        except Exception:
                            pass
                        return None
                else:
                    img = _PILImage.open(file)
                    fmt = img.format
                    return fmt.lower() if fmt else None
            except Exception:
                return None

        fake_imghdr.what = _what  # type: ignore
        sys.modules["imghdr"] = fake_imghdr
        logger.info("imghdr shim installed using Pillow")
    except Exception:
        logger.debug("Pillow not available; imghdr shim skipped")

# -------------------------
# telegram imports (v13)
# -------------------------
try:
    from telegram import (
        Bot,
        Update,
        InlineKeyboardMarkup,
        InlineKeyboardButton,
        ParseMode,
        InputMediaPhoto,
        InputMediaVideo,
        InputMediaDocument,
    )
    from telegram.ext import (
        Dispatcher,
        CommandHandler,
        MessageHandler,
        Filters,
        CallbackContext,
        CallbackQueryHandler,
    )
    import telegram.error as tg_errors
except Exception as e:
    logger.exception("python-telegram-bot v13.x is required. Install python-telegram-bot==13.15")
    raise

# -------------------------
# Tunables and constants
# -------------------------
MAX_MBOTS = 20  # requested limit
BROADCAST_WORKERS = 100
BROADCAST_BATCH_SIZE = 60
BROADCAST_BATCH_PAUSE = 0.05
SQLITE_RETRY_WAIT = 0.25
SQLITE_MAX_RETRIES = 12
UPLOAD_SESSION_TIMEOUT = 30 * 60  # 30 minutes idle timeout
AUTO_CLEAN_INTERVAL = 24 * 3600   # 24 hours
WEBHOOK_WORKERS = 6

# -------------------------
# SQLite helpers (WAL + retry/backoff)
# -------------------------
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        cur.close()
    except Exception:
        pass
    return conn

def sqlite_exec_retry(db_path: str, fn, *args, **kwargs):
    attempt = 0
    while True:
        try:
            conn = get_conn(db_path)
            try:
                res = fn(conn, *args, **kwargs)
                conn.commit()
                conn.close()
                return res
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn.close()
                raise
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg:
                attempt += 1
                if attempt > SQLITE_MAX_RETRIES:
                    logger.exception("SQLite max retries exceeded for %s", db_path)
                    raise
                wait_t = SQLITE_RETRY_WAIT * attempt
                logger.warning("SQLite locked — retrying in %.2fs (attempt %d) for %s", wait_t, attempt, db_path)
                time.sleep(wait_t)
                continue
            else:
                raise

# -------------------------
# Initialize cbot DB
# -------------------------
def ensure_cbot_db():
    def _init(conn):
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                alias TEXT PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                username TEXT,
                status TEXT DEFAULT 'running'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                alias TEXT NOT NULL,
                username TEXT NOT NULL,
                PRIMARY KEY(alias, username)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cur.close()
    sqlite_exec_retry(CBOT_DB, _init)

ensure_cbot_db()

# -------------------------
# Utility functions
# -------------------------
def sanitize_alias(alias: str) -> str:
    if not alias:
        return ""
    return "".join(ch for ch in alias if ch.isalnum() or ch in ("_", "-")).strip()

def pretty_exception(e: Exception) -> str:
    return "".join(traceback.format_exception_only(e.__class__, e)).strip()

def mask_token(token: str) -> str:
    if not token:
        return ""
    if len(token) <= 10:
        return "*" * len(token)
    return "*" * (len(token) - 8) + token[-8:]

def send_with_retry(fn, *args, **kwargs) -> bool:
    max_attempts = 6
    attempt = 0
    while attempt < max_attempts:
        try:
            fn(*args, **kwargs)
            return True
        except tg_errors.RetryAfter as e:
            wait = getattr(e, "retry_after", 1.0)
            logger.warning("RetryAfter: sleeping %s", wait)
            time.sleep(wait + 0.5)
            attempt += 1
        except tg_errors.TimedOut:
            logger.warning("TimedOut; retrying")
            time.sleep(0.5)
            attempt += 1
        except tg_errors.NetworkError:
            logger.warning("NetworkError; retrying")
            time.sleep(0.5)
            attempt += 1
        except Exception as e:
            logger.debug("send_with_retry unexpected exception: %s", e)
            return False
    return False

# -------------------------
# Owner-only decorator for CBot handlers
# -------------------------
def owner_only(func):
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        if OWNER_ID is None:
            try:
                update.message.reply_text("OWNER_ID is not configured in the environment.")
            except Exception:
                pass
            return
        if user.id != OWNER_ID:
            try:
                # friendly message
                update.message.reply_text("Unauthorized. Owner only.")
            except Exception:
                pass
            return
        return func(update, context, *args, **kwargs)
    return wrapper

# -------------------------
# Check if username is admin for alias (CBot stores admins)
# -------------------------
def is_admin_for(alias: str, username_or_id: Optional[str]) -> bool:
    if not username_or_id:
        return False
    uname = str(username_or_id).lstrip("@")
    def _fn(conn):
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM admins WHERE alias = ? AND username = ?", (alias, uname))
        return cur.fetchone()
    try:
        return sqlite_exec_retry(CBOT_DB, lambda conn: _fn(conn)) is not None
    except Exception:
        return False

# -------------------------
# MBot class
# -------------------------
class MBot:
    def __init__(self, alias: str, token: str, username: Optional[str] = None, shared_executor: Optional[ThreadPoolExecutor] = None):
        self.alias = sanitize_alias(alias)
        self.token = token
        self.username = username
        self.db_file = os.path.join(BASE_DIR, f"mbot_{self.alias}.db")
        self.files_dir = os.path.join(BASE_DIR, f"files_{self.alias}")
        if not os.path.exists(self.files_dir):
            try:
                os.makedirs(self.files_dir, exist_ok=True)
            except Exception:
                pass
        try:
            self.bot = Bot(token=token)
        except Exception as e:
            logger.exception("Failed to create Bot for %s: %s", alias, e)
            self.bot = None
        if self.bot:
            self.dp = Dispatcher(self.bot, None, use_context=True, workers=4)
        else:
            self.dp = None
        self.sessions: Dict[int, Dict[str, Any]] = {}
        self.sessions_lock = threading.Lock()
        self.executor = shared_executor or ThreadPoolExecutor(max_workers=16)
        self._threads: List[threading.Thread] = []
        self.running = True
        self._ensure_db()
        self._register_handlers()
        self._start_auto_clean_scheduler()

    def _ensure_db(self):
        def _init(conn):
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS uploads (
                    session_id TEXT PRIMARY KEY,
                    owner_id INTEGER,
                    protect INTEGER,
                    autodelete_minutes INTEGER,
                    created_ts INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS upload_files (
                    session_id TEXT,
                    seq INTEGER,
                    file_id TEXT,
                    file_type TEXT,
                    file_name TEXT,
                    caption TEXT
                )
            """)
            cur.close()
        sqlite_exec_retry(self.db_file, _init)

    def _register_handlers(self):
        if not self.dp:
            return
        self.dp.add_handler(CommandHandler("start", self.cmd_start, pass_args=True))
        self.dp.add_handler(CommandHandler("help", self.cmd_help))
        self.dp.add_handler(CommandHandler("adminp", self.cmd_adminp))
        self.dp.add_handler(CommandHandler("setchannel", self.cmd_setchannel, pass_args=True))
        self.dp.add_handler(CommandHandler("setmessage", self.cmd_setmessage))
        self.dp.add_handler(CommandHandler("setimage", self.cmd_setimage))
        self.dp.add_handler(CommandHandler("upload", self.cmd_upload))
        self.dp.add_handler(CommandHandler("d", self.cmd_finish_upload))
        self.dp.add_handler(CommandHandler("e", self.cmd_terminate_upload))
        self.dp.add_handler(CommandHandler("broadcast", self.cmd_broadcast, pass_args=True))
        self.dp.add_handler(CommandHandler("announcement", self.cmd_announcement, pass_args=True))
        self.dp.add_handler(CommandHandler("status", self.cmd_status))
        self.dp.add_handler(CallbackQueryHandler(self._callback_handler))
        self.dp.add_handler(MessageHandler(Filters.photo | Filters.document | Filters.video | (Filters.text & ~Filters.command), self.msg_handler))

    # -------------------------
    # User tracking
    # -------------------------
    def _ensure_user(self, user):
        try:
            def _ins(conn):
                cur = conn.cursor()
                cur.execute("INSERT OR REPLACE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                            (user.id, getattr(user, "username", None), getattr(user, "first_name", None)))
            sqlite_exec_retry(self.db_file, lambda conn: _ins(conn))
        except Exception:
            logger.exception("Failed to ensure user in mbot db for %s", self.alias)

    def _fetch_user_ids(self) -> List[int]:
        try:
            def _fn(conn):
                cur = conn.cursor()
                cur.execute("SELECT user_id FROM users")
                return [r[0] for r in cur.fetchall()]
            return sqlite_exec_retry(self.db_file, lambda conn: _fn(conn))
        except Exception:
            logger.exception("Failed to fetch users for %s", self.alias)
            return []

    def count_users(self) -> int:
        try:
            return len(self._fetch_user_ids())
        except Exception:
            return 0

    # -------------------------
    # Settings helpers
    # -------------------------
    def _get_setting(self, key: str) -> Optional[str]:
        try:
            def _fn(conn):
                cur = conn.cursor()
                cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
                r = cur.fetchone()
                return r[0] if r else None
            return sqlite_exec_retry(self.db_file, lambda conn: _fn(conn))
        except Exception:
            return None

    def _set_setting(self, key: str, value: str):
        try:
            def _fn(conn):
                cur = conn.cursor()
                cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
            sqlite_exec_retry(self.db_file, lambda conn: _fn(conn))
        except Exception:
            logger.exception("Failed to set setting %s for %s", key, self.alias)

    def _format_username(self, user) -> str:
        if getattr(user, "username", None):
            return user.username.lstrip("@")
        return getattr(user, "first_name", "") or str(user.id)

    # -------------------------
    # /start and deep-link handling
    # -------------------------
    def cmd_start(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        self._ensure_user(user)
        payload = None
        if context.args:
            payload = context.args[0]
        if payload and str(payload).startswith("dl_"):
            session_id = payload[3:]
            self._serve_upload_session(session_id, update, context)
            return
        start_text = self._get_setting("start_message") or "Hello {username}! Use /help."
        start_image = self._get_setting("start_image")
        join_channel = self._get_setting("join_channel")
        uname = self._format_username(user)
        text = start_text.replace("{username}", uname)
        buttons = [[InlineKeyboardButton("Help", callback_data="help")]]
        if join_channel and join_channel.lower() != "none":
            buttons[0].append(InlineKeyboardButton("Join Channel", url=join_channel))
        reply_markup = InlineKeyboardMarkup(buttons)
        try:
            if start_image:
                update.message.reply_photo(photo=start_image, caption=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            else:
                update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except Exception:
            try:
                update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except Exception:
                pass

    def cmd_help(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        self._ensure_user(user)
        help_text = self._get_setting("help_message") or "User commands:\n/start\n/help"
        help_image = self._get_setting("help_image")
        try:
            if help_image:
                update.message.reply_photo(photo=help_image, caption=help_text, parse_mode=ParseMode.HTML)
            else:
                update.message.reply_text(help_text, parse_mode=ParseMode.HTML)
        except Exception:
            try:
                update.message.reply_text(help_text, parse_mode=ParseMode.HTML)
            except Exception:
                pass

    def cmd_adminp(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("You are not admin for this bot.")
            return
        admin_text = (
            "Admin Panel:\n"
            "/setimage - Reply to image then /setimage\n"
            "/setmessage - Reply to message then /setmessage\n"
            "/setchannel <link|none> - Set join channel\n"
            "/upload - Start upload session\n"
            "/d - Finish upload and produce link\n"
            "/e - Cancel upload\n"
            "/broadcast copy|forward - Reply to message then /broadcast copy or /broadcast forward\n"
            "/announcement <text> - Announcement\n"
            "/status - Show stats\n"
        )
        update.message.reply_text(admin_text)

    # -------------------------
    # Settings: channel/message/image
    # -------------------------
    def cmd_setchannel(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        args = context.args
        if not args:
            update.message.reply_text("Usage: /setchannel <link|none>")
            return
        channel = args[0].strip()
        self._set_setting("join_channel", channel)
        update.message.reply_text(f"Join channel set to {channel}")

    def cmd_setmessage(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        if not update.message.reply_to_message:
            update.message.reply_text("Please reply to the message you want to set, then send /setmessage")
            return
        replied = update.message.reply_to_message
        content = replied.text or replied.caption or ""
        update.message.reply_text('Reply with "start" or "help" to choose where to save this message.')
        with self.sessions_lock:
            self.sessions[user.id] = {"pending_setmessage": content}

    def cmd_setimage(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        if not update.message.reply_to_message:
            update.message.reply_text("Please reply to a photo and send /setimage")
            return
        replied = update.message.reply_to_message
        photo_fid = None
        caption = None
        if replied.photo:
            photo_fid = replied.photo[-1].file_id
            caption = replied.caption or ""
        elif replied.document and getattr(replied.document, "mime_type", "").startswith("image"):
            photo_fid = replied.document.file_id
            caption = replied.caption or ""
        else:
            update.message.reply_text("Replied message does not contain a photo.")
            return
        update.message.reply_text('Reply with "start" or "help" to set this image for that screen.')
        with self.sessions_lock:
            self.sessions[user.id] = {"pending_setimage": photo_fid, "pending_caption": caption}

    # -------------------------
    # Broadcast & announcement (admin only)
    # -------------------------
    def cmd_broadcast(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        args = context.args or []
        mode = "forward"
        if args and args[0].lower() in ("copy", "forward"):
            mode = args[0].lower()
        if not update.message.reply_to_message:
            update.message.reply_text("Reply to a message to broadcast (or use /broadcast copy|forward and reply).")
            return
        reply = update.message.reply_to_message
        user_ids = self._fetch_user_ids()
        if not user_ids:
            update.message.reply_text("No users to broadcast.")
            return
        sent, failed = self._parallel_copy_forward_broadcast(user_ids, reply, mode)
        update.message.reply_text(f"Broadcast complete. Sent: {sent}, Failed: {failed}")

    def cmd_announcement(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        text = " ".join(context.args) if context.args else None
        if update.message.reply_to_message:
            src = update.message.reply_to_message
            user_ids = self._fetch_user_ids()
            if not user_ids:
                update.message.reply_text("No users to announce.")
                return
            sent, failed = self._parallel_copy_forward_broadcast(user_ids, src, "copy")
            update.message.reply_text(f"Announcement sent. Sent: {sent}, Failed: {failed}")
            return
        if not text:
            update.message.reply_text("Usage: /announcement <text> or reply to message and /announcement")
            return
        user_ids = self._fetch_user_ids()
        if not user_ids:
            update.message.reply_text("No users to announce.")
            return
        sent, failed = self._parallel_text_broadcast(user_ids, text)
        update.message.reply_text(f"Announcement sent. Sent: {sent}, Failed: {failed}")

    def _parallel_text_broadcast(self, user_ids: List[int], text: str) -> Tuple[int, int]:
        sent = 0
        failed = 0
        batches = [user_ids[i:i+BROADCAST_BATCH_SIZE] for i in range(0, len(user_ids), BROADCAST_BATCH_SIZE)]
        for batch in batches:
            futures = [self.executor.submit(send_with_retry, self.bot.send_message, chat_id=uid, text=text) for uid in batch]
            for fut in as_completed(futures):
                try:
                    ok = fut.result()
                    if ok:
                        sent += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
            time.sleep(BROADCAST_BATCH_PAUSE)
        return sent, failed

    def _parallel_copy_forward_broadcast(self, user_ids: List[int], reply_msg, mode: str) -> Tuple[int, int]:
        sent = 0
        failed = 0
        batches = [user_ids[i:i+BROADCAST_BATCH_SIZE] for i in range(0, len(user_ids), BROADCAST_BATCH_SIZE)]
        for batch in batches:
            futures = []
            for uid in batch:
                futures.append(self.executor.submit(self._send_copy_forward_one, uid, reply_msg, mode))
            for fut in as_completed(futures):
                try:
                    ok = fut.result()
                    if ok:
                        sent += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
            time.sleep(BROADCAST_BATCH_PAUSE)
        return sent, failed

    def _send_copy_forward_one(self, uid: int, reply_msg, mode: str) -> bool:
        try:
            chat = getattr(reply_msg, "chat", None)
            # forward only preserves channel identity; otherwise copy to avoid showing uploader
            if mode == "forward" and chat and getattr(chat, "type", "") == "channel":
                try:
                    return send_with_retry(self.bot.forward_message, chat_id=uid, from_chat_id=chat.id, message_id=reply_msg.message_id)
                except Exception:
                    return self._fallback_resend(uid, reply_msg)
            else:
                return self._fallback_resend(uid, reply_msg)
        except Exception:
            return False

    def _fallback_resend(self, uid: int, msg) -> bool:
        try:
            if getattr(msg, "photo", None):
                fid = msg.photo[-1].file_id
                cap = getattr(msg, "caption", None)
                return send_with_retry(self.bot.send_photo, chat_id=uid, photo=fid, caption=cap) if fid else False
            if getattr(msg, "document", None):
                fid = msg.document.file_id
                cap = getattr(msg, "caption", None)
                return send_with_retry(self.bot.send_document, chat_id=uid, document=fid, caption=cap) if fid else False
            if getattr(msg, "video", None):
                fid = msg.video.file_id
                cap = getattr(msg, "caption", None)
                return send_with_retry(self.bot.send_video, chat_id=uid, video=fid, caption=cap) if fid else False
            if getattr(msg, "text", None):
                return send_with_retry(self.bot.send_message, chat_id=uid, text=msg.text)
            # fallback to forward if possible
            from_chat = getattr(msg.chat, "id", None) if getattr(msg, "chat", None) else getattr(msg, "chat_id", None)
            if from_chat and getattr(msg, "message_id", None):
                return send_with_retry(self.bot.forward_message, chat_id=uid, from_chat_id=from_chat, message_id=msg.message_id)
            return False
        except Exception:
            return False

    # -------------------------
    # Upload (bag-style) flow:
    # /upload -> send files (one or many) -> /d -> protect on/off -> autodelete minutes -> link
    # /e cancels
    # -------------------------
    def cmd_upload(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if not is_admin_for(self.alias, user.username) and user.id != OWNER_ID:
            update.message.reply_text("Unauthorized. Admins only.")
            return
        with self.sessions_lock:
            self.sessions[user.id] = {"upload": {"files": [], "protect": None, "autodelete": None, "owner": user.id, "last_ts": time.time(), "stage": "collect"}}
        update.message.reply_text("Upload session started. Send files now (single or multiple). Use /d when done, /e to cancel. Session expires in 30 minutes.")

    def cmd_terminate_upload(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        with self.sessions_lock:
            s = self.sessions.pop(user.id, None)
        if s and "upload" in s:
            update.message.reply_text("Upload session terminated. Files discarded.")
        else:
            update.message.reply_text("No active upload session.")

    def cmd_finish_upload(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        with self.sessions_lock:
            s = self.sessions.get(user.id)
        if not s or "upload" not in s:
            update.message.reply_text("No active upload session. Start with /upload")
            return
        upload = s["upload"]
        stage = upload.get("stage", "collect")
        if stage == "collect":
            upload["stage"] = "ask_protect"
            update.message.reply_text('Reply with "on" or "off" to set Protect content for this session.')
            return
        if stage == "ask_protect":
            update.message.reply_text('Reply with "on" or "off" to set Protect content for this session.')
            return
        if stage == "ask_autodelete":
            update.message.reply_text("Send autodelete minutes (0 = never, 1 - 10080).")
            return

    def msg_handler(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        uid = user.id
        text = update.message.text or update.message.caption or ""
        # handle pending setmessage/setimage
        with self.sessions_lock:
            s = self.sessions.get(uid)
        if s and "pending_setmessage" in s:
            choice = (text or "").strip().lower()
            original = s.get("pending_setmessage")
            if choice in ("start", "help"):
                key = f"{choice}_message"
                self._set_setting(key, original)
                update.message.reply_text(f"Set {choice} message.")
                with self.sessions_lock:
                    try:
                        del self.sessions[uid]
                    except Exception:
                        pass
                return
            else:
                update.message.reply_text('Please reply with "start" or "help"')
                return
        if s and "pending_setimage" in s:
            choice = (text or "").strip().lower()
            fid = s.get("pending_setimage")
            caption = s.get("pending_caption")
            if choice in ("start", "help"):
                key = f"{choice}_image"
                self._set_setting(key, fid)
                if caption:
                    self._set_setting(f"{choice}_message", caption)
                update.message.reply_text(f"Set {choice} image.")
                with self.sessions_lock:
                    try:
                        del self.sessions[uid]
                    except Exception:
                        pass
                return
            else:
                update.message.reply_text('Please reply with "start" or "help"')
                return
        # upload flow
        with self.sessions_lock:
            s = self.sessions.get(uid)
        if s and "upload" in s:
            upload = s["upload"]
            upload["last_ts"] = time.time()
            stage = upload.get("stage", "collect")
            # protect step
            if stage == "ask_protect":
                if text and text.strip().lower() in ("on", "off"):
                    upload["protect"] = text.strip().lower()
                    upload["stage"] = "ask_autodelete"
                    update.message.reply_text(f"Protect set to {upload['protect']}. Now send autodelete minutes (0-10080).")
                    return
                else:
                    update.message.reply_text('Please send "on" or "off" for protect setting.')
                    return
            # autodelete step
            if stage == "ask_autodelete":
                try:
                    minutes = int(text.strip())
                    if minutes < 0 or minutes > 10080:
                        raise ValueError("out of range")
                    upload["autodelete"] = minutes
                    # finalize: persist session
                    files = upload.get("files", [])
                    protect_flag = 1 if upload.get("protect") == "on" else 0
                    autodelete = int(upload.get("autodelete") or 0)
                    session_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("utf-8").rstrip("=")
                    def _persist(conn):
                        cur = conn.cursor()
                        cur.execute("INSERT INTO uploads (session_id, owner_id, protect, autodelete_minutes, created_ts) VALUES (?, ?, ?, ?, ?)",
                                    (session_id, user.id, protect_flag, autodelete, int(time.time())))
                        seq = 0
                        for f in files:
                            seq += 1
                            cur.execute("INSERT INTO upload_files (session_id, seq, file_id, file_type, file_name, caption) VALUES (?, ?, ?, ?, ?, ?)",
                                        (session_id, seq, f.get("file_id"), f.get("type"), f.get("file_name"), f.get("caption")))
                        conn.commit()
                    try:
                        sqlite_exec_retry(self.db_file, _persist)
                    except Exception:
                        update.message.reply_text("Failed to persist upload session.")
                        return
                    with self.sessions_lock:
                        try:
                            del self.sessions[uid]
                        except Exception:
                            pass
                    link = f"https://t.me/{self.username}?start=dl_{session_id}" if self.username else f"dl_{session_id}"
                    if WEBHOOK_BASE and self.token:
                        link = f"{WEBHOOK_BASE}/{self.token}?start=dl_{session_id}"
                    # final message (exact format requested)
                    if autodelete and autodelete > 0:
                        update.message.reply_text(f"Upload saved. Link: {link}\nAll files will be deleted in {autodelete} minutes")
                    else:
                        update.message.reply_text(f"Upload saved. Link: {link}\nFiles will not be deleted")
                    return
                except Exception:
                    update.message.reply_text("Please send integer minutes between 0 and 10080.")
                    return
            # collect files
            if stage == "collect":
                if update.message.photo:
                    photos = update.message.photo
                    for p in photos:
                        fid = p.file_id
                        cap = update.message.caption or None
                        upload["files"].append({"file_id": fid, "type": "photo", "file_name": None, "caption": cap})
                    update.message.reply_text(f"Saved {len(photos)} photo(s) to session.")
                    return
                if update.message.document:
                    doc = update.message.document
                    cap = update.message.caption or None
                    upload["files"].append({"file_id": doc.file_id, "type": "document", "file_name": getattr(doc, "file_name", None), "caption": cap})
                    update.message.reply_text("Document saved in session.")
                    return
                if update.message.video:
                    vid = update.message.video
                    cap = update.message.caption or None
                    upload["files"].append({"file_id": vid.file_id, "type": "video", "file_name": None, "caption": cap})
                    update.message.reply_text("Video saved in session.")
                    return
                if update.message.text and not update.message.text.startswith("/"):
                    txt = update.message.text
                    fname = f"text_{uuid.uuid4().hex[:8]}.txt"
                    fpath = os.path.join(self.files_dir, fname)
                    try:
                        with open(fpath, "w", encoding="utf-8") as f:
                            f.write(txt)
                        # try to upload to owner to get a file_id for persistence if OWNER_ID set
                        if OWNER_ID:
                            try:
                                sent = self.bot.send_document(chat_id=OWNER_ID, document=open(fpath, "rb"))
                                file_id = sent.document.file_id
                                upload["files"].append({"file_id": file_id, "type": "document", "file_name": fname, "caption": None})
                                update.message.reply_text("Text saved and uploaded to owner.")
                            except Exception:
                                upload["files"].append({"file_id": None, "type": "textfile", "file_name": fname, "caption": txt})
                                update.message.reply_text("Text saved locally; failed upload to owner.")
                        else:
                            upload["files"].append({"file_id": None, "type": "textfile", "file_name": fname, "caption": txt})
                            update.message.reply_text("Text saved locally.")
                    except Exception:
                        update.message.reply_text("Failed to save text.")
                    return
                update.message.reply_text("Send photos, videos, or documents to add, or /d when done.")
                return
        # no session: ensure user stored
        self._ensure_user(user)

    # -------------------------
    # Serve upload session (deep-link)
    # -------------------------
    def _serve_upload_session(self, session_id: str, update: Update, context: CallbackContext):
        def _fetch(conn):
            cur = conn.cursor()
            cur.execute("SELECT session_id, owner_id, protect, autodelete_minutes, created_ts FROM uploads WHERE session_id = ?", (session_id,))
            base = cur.fetchone()
            if not base:
                return None
            cur.execute("SELECT seq, file_id, file_type, file_name, caption FROM upload_files WHERE session_id = ? ORDER BY seq ASC", (session_id,))
            files = cur.fetchall()
            return (base, files)
        try:
            data = sqlite_exec_retry(self.db_file, _fetch)
        except Exception:
            data = None
        if not data:
            update.message.reply_text("Link not found or expired.")
            return
        (session_id, owner_id, protect, autodelete_minutes, created_ts), files = data
        chat_id = update.effective_chat.id
        sent_message_ids: List[int] = []
        photos = [f for f in files if f[2] == "photo"]
        docs = [f for f in files if f[2] != "photo"]
        try:
            if photos:
                media = []
                for i, p in enumerate(photos):
                    seq, file_id, ftype, fname, caption = p
                    if not file_id:
                        continue
                    if i == 0 and caption:
                        media.append(InputMediaPhoto(media=file_id, caption=caption))
                    else:
                        media.append(InputMediaPhoto(media=file_id))
                if media:
                    sent_group = self.bot.send_media_group(chat_id=chat_id, media=media)
                    for m in sent_group:
                        if getattr(m, "message_id", None):
                            sent_message_ids.append(m.message_id)
        except Exception:
            for i, p in enumerate(photos):
                seq, file_id, ftype, fname, caption = p
                try:
                    if i == 0 and caption:
                        s = self.bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption)
                    else:
                        s = self.bot.send_photo(chat_id=chat_id, photo=file_id)
                    sent_message_ids.append(s.message_id)
                except Exception:
                    pass
        for d in docs:
            seq, file_id, file_type, file_name, caption = d
            try:
                if file_type == "video":
                    if file_id:
                        if caption:
                            s = self.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
                        else:
                            s = self.bot.send_video(chat_id=chat_id, video=file_id)
                        sent_message_ids.append(s.message_id)
                    else:
                        fpath = os.path.join(self.files_dir, file_name) if file_name else None
                        if fpath and os.path.exists(fpath):
                            if caption:
                                s = self.bot.send_video(chat_id=chat_id, video=open(fpath, "rb"), caption=caption)
                            else:
                                s = self.bot.send_video(chat_id=chat_id, video=open(fpath, "rb"))
                            sent_message_ids.append(s.message_id)
                else:
                    if file_id:
                        if caption:
                            s = self.bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
                        else:
                            s = self.bot.send_document(chat_id=chat_id, document=file_id)
                        sent_message_ids.append(s.message_id)
                    else:
                        fpath = os.path.join(self.files_dir, file_name) if file_name else None
                        if fpath and os.path.exists(fpath):
                            if caption:
                                s = self.bot.send_document(chat_id=chat_id, document=open(fpath, "rb"), caption=caption)
                            else:
                                s = self.bot.send_document(chat_id=chat_id, document=open(fpath, "rb"))
                            sent_message_ids.append(s.message_id)
                        else:
                            if caption:
                                s = self.bot.send_message(chat_id=chat_id, text=caption)
                                sent_message_ids.append(s.message_id)
            except Exception:
                pass
        # schedule autodelete on messages (only deletes messages from chat; DB unchanged)
        if autodelete_minutes and autodelete_minutes > 0 and sent_message_ids:
            delay = autodelete_minutes * 60
            t = threading.Thread(target=self._schedule_delete_messages, args=(chat_id, sent_message_ids, delay), daemon=True)
            t.start()
            self._threads.append(t)
        # last message: either "All files will be deleted in <minutes> minutes" or "Files will not be deleted"
        try:
            if autodelete_minutes and autodelete_minutes > 0:
                self.bot.send_message(chat_id=chat_id, text=f"All files will be deleted in {autodelete_minutes} minutes")
            else:
                self.bot.send_message(chat_id=chat_id, text="Files will not be deleted")
        except Exception:
            pass
        # notify about protect flag (informational only)
        if protect:
            try:
                self.bot.send_message(chat_id=chat_id, text="Content protected: saving/sharing may be restricted by the uploader.")
            except Exception:
                pass

    def _schedule_delete_messages(self, chat_id: int, message_ids: List[int], delay: int):
        logger.info("Scheduling deletion in %s seconds for %s messages in chat %s", delay, len(message_ids), chat_id)
        time.sleep(delay)
        for mid in message_ids:
            try:
                self.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass

    # -------------------------
    # Callback (help button)
    # -------------------------
    def _callback_handler(self, update: Update, context: CallbackContext):
        query = update.callback_query
        if not query:
            return
        if query.data == "help":
            try:
                self.cmd_help(update, context)
                query.answer()
            except Exception:
                try:
                    query.answer()
                except Exception:
                    pass
        else:
            try:
                query.answer()
            except Exception:
                pass

    # -------------------------
    # Auto-clean scheduler (sessions & old local files)
    # -------------------------
    def _start_auto_clean_scheduler(self):
        def cleaner_loop():
            while True:
                try:
                    self._auto_clean_once()
                except Exception:
                    logger.exception("Auto-clean failed for %s", self.alias)
                time.sleep(AUTO_CLEAN_INTERVAL)
        t = threading.Thread(target=cleaner_loop, daemon=True)
        t.start()
        self._threads.append(t)

    def _auto_clean_once(self):
        now = time.time()
        with self.sessions_lock:
            stale = []
            for uid, s in list(self.sessions.items()):
                if "upload" in s:
                    last = s["upload"].get("last_ts", now)
                    if now - last > UPLOAD_SESSION_TIMEOUT:
                        stale.append(uid)
            for uid in stale:
                try:
                    del self.sessions[uid]
                except Exception:
                    pass
        # prune very old local text files (optional)
        try:
            cutoff = now - (30 * 24 * 3600)
            for fn in os.listdir(self.files_dir):
                fpath = os.path.join(self.files_dir, fn)
                try:
                    if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                        os.remove(fpath)
                except Exception:
                    pass
        except Exception:
            pass

# -------------------------
# CBot class (owner-only)
# -------------------------
class CBot:
    def __init__(self, token: str):
        if not token:
            raise ValueError("CBOT_TOKEN must be provided via environment")
        self.token = token
        try:
            self.bot = Bot(token=token)
            self.dp = Dispatcher(self.bot, None, use_context=True, workers=WEBHOOK_WORKERS)
        except Exception as e:
            logger.exception("Failed to init CBot Bot/Dispatcher: %s", e)
            raise
        self.mbots: Dict[str, MBot] = {}
        self.token_map: Dict[str, str] = {}
        self.executor = ThreadPoolExecutor(max_workers=BROADCAST_WORKERS)
        self._register_handlers()
        self._load_mbots_from_db()

    def _register_handlers(self):
        self.dp.add_handler(CommandHandler("start", self.cmd_start))
        self.dp.add_handler(CommandHandler("help", self.cmd_help))
        self.dp.add_handler(CommandHandler("addbot", self.cmd_addbot, pass_args=True))
        self.dp.add_handler(CommandHandler("removebot", self.cmd_removebot, pass_args=True))
        self.dp.add_handler(CommandHandler("stopbot", self.cmd_stopbot, pass_args=True))
        self.dp.add_handler(CommandHandler("runbot", self.cmd_runbot, pass_args=True))
        self.dp.add_handler(CommandHandler("addadmin", self.cmd_addadmin, pass_args=True))
        self.dp.add_handler(CommandHandler("removeadmin", self.cmd_removeadmin, pass_args=True))
        self.dp.add_handler(CommandHandler("listbot", self.cmd_listbot))
        self.dp.add_handler(CommandHandler("listadmin", self.cmd_listadmin))
        self.dp.add_handler(CommandHandler("broadcast", self.cmd_broadcast, pass_args=True))
        self.dp.add_handler(CommandHandler("stats", self.cmd_stats))
        self.dp.add_handler(CommandHandler("setstart", self.cmd_setstart))
        self.dp.add_handler(CommandHandler("setimage", self.cmd_setimage))

    def _load_mbots_from_db(self):
        def _all(conn):
            cur = conn.cursor()
            cur.execute("SELECT alias, token, username, status FROM bots")
            return cur.fetchall()
        try:
            rows = sqlite_exec_retry(CBOT_DB, _all)
            for alias, token, username, status in rows:
                try:
                    alias_s = sanitize_alias(alias)
                    if not alias_s:
                        continue
                    if token in self.token_map:
                        logger.warning("Token already in use by %s; skipping startup for %s", self.token_map[token], alias_s)
                        continue
                    if len(self.mbots) >= MAX_MBOTS:
                        logger.warning("MAX_MBOTS reached; skipping start for %s", alias_s)
                        continue
                    mb = MBot(alias=alias_s, token=token, username=username, shared_executor=self.executor)
                    self.mbots[alias_s] = mb
                    self.token_map[token] = alias_s
                    if status != "running":
                        mb.running = False
                    logger.info("Loaded and started mbot: %s (@%s)", alias_s, username)
                except Exception:
                    logger.exception("Failed to load mbot from DB: %s", alias)
        except Exception:
            logger.exception("Failed to load MBots from DB")

    # -------------------------
    # CBot handlers
    # -------------------------
    def cmd_start(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        try:
            if user.id == OWNER_ID:
                update.message.reply_text("CBot active. Use /help to manage MBots.")
            else:
                update.message.reply_text("This is the owner control bot. Only the owner can use management commands.")
        except Exception:
            pass

    def cmd_help(self, update: Update, context: CallbackContext):
        user = update.effective_user
        if not user:
            return
        if user.id != OWNER_ID:
            update.message.reply_text("Only the owner can use CBot commands. Use /start.")
            return
        help_text = (
            "CBot Owner Commands:\n"
            "/start - show this bot\n"
            "/help - this help\n"
            "/stats - show counts\n"
            "/addbot <token> <alias> - register and start MBot\n"
            "/removebot <alias> - remove MBot and ALL its data (permanent)\n"
            "/stopbot <alias> - freeze MBot (no data deleted)\n"
            "/runbot <alias> - unfreeze MBot\n"
            "/listbot - list registered MBots and status\n"
            "/addadmin <mbot_alias> <user_or_@username> - add admin to MBot\n"
            "/removeadmin <mbot_alias> <user_or_@username> - remove admin\n"
            "/listadmin - list admins per MBot\n"
            "/setstart - reply to message or /setstart <text>\n"
            "/setimage - reply to image then /setimage\n"
            "/broadcast copy|forward <alias|all> - reply to a message then use this to broadcast\n"
        )
        update.message.reply_text(help_text)

    @owner_only
    def cmd_addbot(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 2:
            update.message.reply_text("Usage: /addbot <bot_token> <alias>")
            return
        token = args[0].strip()
        alias = sanitize_alias(args[1].strip())
        if not alias:
            update.message.reply_text("Invalid alias.")
            return
        try:
            b = Bot(token=token)
            me = b.get_me()
            username = me.username or None
        except Exception as e:
            update.message.reply_text(f"Token validation failed: {pretty_exception(e)}")
            return
        # check MAX_MBOTS
        try:
            def _count(conn):
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM bots")
                return cur.fetchone()[0]
            curcnt = sqlite_exec_retry(CBOT_DB, lambda conn: _count(conn))
            if curcnt >= MAX_MBOTS:
                update.message.reply_text(f"Cannot add more bots; MAX_MBOTS={MAX_MBOTS} reached.")
                return
        except Exception:
            pass
        def _insert(conn):
            cur = conn.cursor()
            cur.execute("INSERT INTO bots (alias, token, username, status) VALUES (?, ?, ?, ?)", (alias, token, username, "running"))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _insert(conn))
            update.message.reply_text(f"Added mbot `{alias}` with username @{username}", parse_mode=ParseMode.MARKDOWN)
        except sqlite3.IntegrityError:
            update.message.reply_text("Alias or token already exists.")
            return
        except Exception as e:
            update.message.reply_text(f"Failed to add bot to DB: {pretty_exception(e)}")
            return
        try:
            mb = MBot(alias=alias, token=token, username=username, shared_executor=self.executor)
            self.mbots[alias] = mb
            self.token_map[token] = alias
            update.message.reply_text(f"MBot {alias} started.")
        except Exception as e:
            update.message.reply_text(f"Added to DB but failed to activate MBot: {pretty_exception(e)}")
            logger.exception("Failed to start MBot: %s", e)

    @owner_only
    def cmd_removebot(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 1:
            update.message.reply_text("Usage: /removebot <alias>")
            return
        alias = sanitize_alias(args[0].strip())
        if not alias:
            update.message.reply_text("Invalid alias.")
            return
        # stop in-memory
        mb = self.mbots.pop(alias, None)
        if mb:
            try:
                if mb.token and mb.token in self.token_map:
                    del self.token_map[mb.token]
            except Exception:
                pass
            try:
                mb.running = False
            except Exception:
                pass
        # delete DB row and admins
        dbpath = os.path.join(BASE_DIR, f"mbot_{alias}.db")
        filesdir = os.path.join(BASE_DIR, f"files_{alias}")
        def _delete(conn):
            cur = conn.cursor()
            cur.execute("DELETE FROM bots WHERE alias = ?", (alias,))
            cur.execute("DELETE FROM admins WHERE alias = ?", (alias,))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _delete(conn))
        except Exception:
            logger.exception("Failed to delete DB rows for %s", alias)
        # remove mbot db & files directory permanently
        try:
            if os.path.exists(dbpath):
                os.remove(dbpath)
        except Exception:
            pass
        try:
            if os.path.exists(filesdir):
                shutil.rmtree(filesdir, ignore_errors=True)
        except Exception:
            pass
        update.message.reply_text(f"Removed bot {alias} and its data permanently.")

    @owner_only
    def cmd_stopbot(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 1:
            update.message.reply_text("Usage: /stopbot <alias>")
            return
        alias = sanitize_alias(args[0].strip())
        mb = self.mbots.get(alias)
        if mb:
            mb.running = False
            def _set(conn):
                cur = conn.cursor()
                cur.execute("UPDATE bots SET status = 'stopped' WHERE alias = ?", (alias,))
                conn.commit()
            try:
                sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            except Exception:
                pass
            update.message.reply_text(f"Bot {alias} stopped (frozen).")
        else:
            update.message.reply_text(f"Bot {alias} not running in memory; marking as stopped in DB.")
            def _set(conn):
                cur = conn.cursor()
                cur.execute("UPDATE bots SET status = 'stopped' WHERE alias = ?", (alias,))
                conn.commit()
            try:
                sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            except Exception:
                pass

    @owner_only
    def cmd_runbot(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 1:
            update.message.reply_text("Usage: /runbot <alias>")
            return
        alias = sanitize_alias(args[0].strip())
        def _get(conn):
            cur = conn.cursor()
            cur.execute("SELECT token, username FROM bots WHERE alias = ?", (alias,))
            return cur.fetchone()
        try:
            row = sqlite_exec_retry(CBOT_DB, lambda conn: _get(conn))
        except Exception:
            row = None
        if not row:
            update.message.reply_text("Alias not registered in DB.")
            return
        token, username = row
        mb = self.mbots.get(alias)
        if mb:
            mb.running = True
            def _set(conn):
                cur = conn.cursor()
                cur.execute("UPDATE bots SET status = 'running' WHERE alias = ?", (alias,))
                conn.commit()
            try:
                sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            except Exception:
                pass
            update.message.reply_text(f"Bot {alias} is now running.")
            return
        # create MBot in memory
        try:
            newmb = MBot(alias=alias, token=token, username=username, shared_executor=self.executor)
            self.mbots[alias] = newmb
            self.token_map[token] = alias
            def _set(conn):
                cur = conn.cursor()
                cur.execute("UPDATE bots SET status = 'running' WHERE alias = ?", (alias,))
                conn.commit()
            try:
                sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            except Exception:
                pass
            update.message.reply_text(f"Bot {alias} started.")
        except Exception as e:
            update.message.reply_text(f"Failed to start MBot: {pretty_exception(e)}")
            logger.exception("Failed to run bot %s: %s", alias, e)

    @owner_only
    def cmd_addadmin(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 2:
            update.message.reply_text("Usage: /addadmin <mbot_alias> <user_or_@username>")
            return
        alias = sanitize_alias(args[0].strip())
        usertok = args[1].lstrip("@").strip()
        def _insert(conn):
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO admins (alias, username) VALUES (?, ?)", (alias, usertok))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _insert(conn))
            update.message.reply_text(f"Added admin {usertok} to {alias}")
        except Exception as e:
            update.message.reply_text(f"Failed to add admin: {pretty_exception(e)}")

    @owner_only
    def cmd_removeadmin(self, update: Update, context: CallbackContext):
        args = context.args
        if len(args) < 2:
            update.message.reply_text("Usage: /removeadmin <mbot_alias> <user_or_@username>")
            return
        alias = sanitize_alias(args[0].strip())
        usertok = args[1].lstrip("@").strip()
        def _delete(conn):
            cur = conn.cursor()
            cur.execute("DELETE FROM admins WHERE alias = ? AND username = ?", (alias, usertok))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _delete(conn))
            update.message.reply_text(f"Removed admin {usertok} from {alias}")
        except Exception:
            update.message.reply_text("Failed to remove admin.")

    @owner_only
    def cmd_listbot(self, update: Update, context: CallbackContext):
        def _get(conn):
            cur = conn.cursor()
            cur.execute("SELECT alias, username, status FROM bots")
            return cur.fetchall()
        try:
            rows = sqlite_exec_retry(CBOT_DB, lambda conn: _get(conn))
            lines = []
            for alias, username, status in rows:
                running = "running" if alias in self.mbots and self.mbots[alias].running else status
                lines.append(f"- {alias} (@{username}) status:{running}")
            update.message.reply_text("Registered MBots:\n" + "\n".join(lines))
        except Exception:
            update.message.reply_text("Failed to list MBots.")

    @owner_only
    def cmd_listadmin(self, update: Update, context: CallbackContext):
        def _get(conn):
            cur = conn.cursor()
            cur.execute("SELECT alias, username FROM admins ORDER BY alias")
            return cur.fetchall()
        try:
            rows = sqlite_exec_retry(CBOT_DB, lambda conn: _get(conn))
            if not rows:
                update.message.reply_text("No admins set.")
                return
            out = {}
            for alias, username in rows:
                out.setdefault(alias, []).append(username)
            lines = []
            for k, v in out.items():
                lines.append(f"{k}: {', '.join(v)}")
            update.message.reply_text("Admins per MBot:\n" + "\n".join(lines))
        except Exception:
            update.message.reply_text("Failed to list admins.")

    @owner_only
    def cmd_broadcast(self, update: Update, context: CallbackContext):
        args = context.args or []
        if not args:
            update.message.reply_text("Usage: /broadcast copy|forward <alias|all> (reply required)")
            return
        mode = args[0].lower()
        if mode not in ("copy", "forward"):
            update.message.reply_text("Mode should be 'copy' or 'forward'")
            return
        if len(args) < 2:
            update.message.reply_text("Specify target alias or 'all'")
            return
        target = args[1]
        if not update.message.reply_to_message:
            update.message.reply_text("Reply to a message to broadcast.")
            return
        src_msg = update.message.reply_to_message
        targets = []
        if target == "all":
            def _all(conn):
                cur = conn.cursor()
                cur.execute("SELECT alias FROM bots")
                return [r[0] for r in cur.fetchall()]
            try:
                targets = sqlite_exec_retry(CBOT_DB, lambda conn: _all(conn))
            except Exception:
                update.message.reply_text("Failed to read registered MBots.")
                return
        else:
            targets = [sanitize_alias(target)]
        totals = {"mbots": 0, "sent": 0, "failed": 0}
        for alias in targets:
            if not alias:
                continue
            def _get_token(conn):
                cur = conn.cursor()
                cur.execute("SELECT token FROM bots WHERE alias = ?", (alias,))
                r = cur.fetchone()
                return r[0] if r else None
            try:
                token = sqlite_exec_retry(CBOT_DB, lambda conn: _get_token(conn))
            except Exception:
                token = None
            if not token:
                update.message.reply_text(f"Alias {alias} not registered.")
                continue
            mb = self.mbots.get(alias)
            db_file = mb.db_file if mb else os.path.join(BASE_DIR, f"mbot_{alias}.db")
            def _get_users(conn):
                cur = conn.cursor()
                cur.execute("SELECT user_id FROM users")
                return [r[0] for r in cur.fetchall()]
            try:
                user_ids = sqlite_exec_retry(db_file, lambda conn: _get_users(conn))
            except Exception:
                update.message.reply_text(f"Failed to fetch users for {alias}")
                continue
            if not user_ids:
                update.message.reply_text(f"No users for {alias}")
                continue
            totals["mbots"] += 1
            if mb:
                sent, failed = mb._parallel_copy_forward_broadcast(user_ids, src_msg, mode)
            else:
                temp_bot = Bot(token=token)
                sent = 0
                failed = 0
                batches = [user_ids[i:i+BROADCAST_BATCH_SIZE] for i in range(0, len(user_ids), BROADCAST_BATCH_SIZE)]
                for batch in batches:
                    futures = []
                    for uid in batch:
                        futures.append(self.executor.submit(self._send_copy_forward_one_external, temp_bot, uid, src_msg, mode))
                    for fut in as_completed(futures):
                        try:
                            ok = fut.result()
                            if ok:
                                sent += 1
                            else:
                                failed += 1
                        except Exception:
                            failed += 1
                    time.sleep(BROADCAST_BATCH_PAUSE)
            totals["sent"] += sent
            totals["failed"] += failed
            update.message.reply_text(f"[{alias}] Broadcast complete. Sent: {sent}, Failed: {failed}")
        update.message.reply_text(f"Broadcast summary. MBots processed: {totals['mbots']}. Total sent: {totals['sent']}. Total failed: {totals['failed']}")

    def _send_copy_forward_one_external(self, bot_obj: Bot, uid: int, reply_msg, mode: str) -> bool:
        try:
            chat = getattr(reply_msg, "chat", None)
            if mode == "forward" and chat and getattr(chat, "type", "") == "channel":
                try:
                    bot_obj.forward_message(chat_id=uid, from_chat_id=chat.id, message_id=reply_msg.message_id)
                    return True
                except Exception:
                    return self._fallback_resend_external(bot_obj, uid, reply_msg)
            else:
                return self._fallback_resend_external(bot_obj, uid, reply_msg)
        except Exception:
            return False

    def _fallback_resend_external(self, bot_obj: Bot, uid: int, msg) -> bool:
        try:
            if getattr(msg, "photo", None):
                fid = msg.photo[-1].file_id
                cap = getattr(msg, "caption", None)
                bot_obj.send_photo(chat_id=uid, photo=fid, caption=cap)
                return True
            if getattr(msg, "document", None):
                fid = msg.document.file_id
                cap = getattr(msg, "caption", None)
                bot_obj.send_document(chat_id=uid, document=fid, caption=cap)
                return True
            if getattr(msg, "video", None):
                fid = msg.video.file_id
                cap = getattr(msg, "caption", None)
                bot_obj.send_video(chat_id=uid, video=fid, caption=cap)
                return True
            if getattr(msg, "text", None):
                bot_obj.send_message(chat_id=uid, text=msg.text)
                return True
            from_chat = getattr(msg.chat, "id", None) if getattr(msg, "chat", None) else getattr(msg, "chat_id", None)
            if from_chat and getattr(msg, "message_id", None):
                bot_obj.forward_message(chat_id=uid, from_chat_id=from_chat, message_id=msg.message_id)
                return True
            return False
        except Exception:
            return False

    @owner_only
    def cmd_stats(self, update: Update, context: CallbackContext):
        def _get_bots(conn):
            cur = conn.cursor()
            cur.execute("SELECT alias, username, status FROM bots")
            return cur.fetchall()
        try:
            rows = sqlite_exec_retry(CBOT_DB, lambda conn: _get_bots(conn))
            lines = []
            total = 0
            for alias, username, status in rows:
                mb = self.mbots.get(alias)
                count = mb.count_users() if mb else 0
                total += count
                running = "running" if alias in self.mbots and self.mbots[alias].running else status
                lines.append(f"- {alias} (@{username}) status:{running} users:{count}")
            lines.append(f"Total users across MBots: {total}")
            update.message.reply_text("\n".join(lines))
        except Exception:
            update.message.reply_text("Failed to fetch stats.")

    @owner_only
    def cmd_setstart(self, update: Update, context: CallbackContext):
        if update.message.reply_to_message:
            content = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
            def _set(conn):
                cur = conn.cursor()
                cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("start_message", content))
                conn.commit()
            try:
                sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
                update.message.reply_text("CBot start message saved (from replied message).")
            except Exception:
                update.message.reply_text("Failed to save start message.")
            return
        text = " ".join(context.args) if context.args else None
        if not text:
            update.message.reply_text("Usage: reply to a message with /setstart or /setstart <text>")
            return
        def _set(conn):
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("start_message", text))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            update.message.reply_text("CBot start message saved.")
        except Exception:
            update.message.reply_text("Failed to save start message.")

    @owner_only
    def cmd_setimage(self, update: Update, context: CallbackContext):
        if not update.message.reply_to_message:
            update.message.reply_text("Reply to a photo and send /setimage")
            return
        replied = update.message.reply_to_message
        fid = None
        if replied.photo:
            fid = replied.photo[-1].file_id
        elif replied.document and getattr(replied.document, "mime_type", "").startswith("image"):
            fid = replied.document.file_id
        else:
            update.message.reply_text("Replied message has no photo.")
            return
        def _set(conn):
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("start_image", fid))
            conn.commit()
        try:
            sqlite_exec_retry(CBOT_DB, lambda conn: _set(conn))
            update.message.reply_text("CBot start image saved.")
        except Exception:
            update.message.reply_text("Failed to save start image.")

# -------------------------
# Flask application for webhooks and health
# -------------------------
app = Flask(__name__)
cbot_instance: Optional[CBot] = None
if CBOT_TOKEN:
    try:
        cbot_instance = CBot(CBOT_TOKEN)
        logger.info("CBot created.")
    except Exception:
        logger.exception("Failed to create CBot instance on startup.")

@app.route("/", methods=["GET"])
def health():
    return "owner_mbots_render_env OK"

@app.route("/ping", methods=["GET"])
def ping():
    return "OK"

@app.route("/<token>", methods=["POST"])
def webhook_handler(token):
    # Accept incoming Telegram webhook JSON and dispatch to correct Bot/Dispatcher
    if request.headers.get("content-type", "").startswith("application/json"):
        data = request.get_json(force=True)
    else:
        try:
            data = request.get_json(force=True)
        except Exception:
            abort(400)
    if not data:
        abort(400)
    global cbot_instance
    bot_obj = None
    dp = None
    # route to CBot if token matches
    if cbot_instance and token == cbot_instance.token:
        bot_obj = cbot_instance.bot
        dp = cbot_instance.dp
    else:
        found_mbot = None
        # fast path: token_map
        if cbot_instance:
            alias = cbot_instance.token_map.get(token)
            if alias:
                found_mbot = cbot_instance.mbots.get(alias)
        if not found_mbot:
            def _get_alias(conn):
                cur = conn.cursor()
                cur.execute("SELECT alias, username FROM bots WHERE token = ?", (token,))
                return cur.fetchone()
            try:
                info = sqlite_exec_retry(CBOT_DB, lambda conn: _get_alias(conn))
                if info:
                    alias, username = info
                    if cbot_instance:
                        found_mbot = cbot_instance.mbots.get(alias)
                        if not found_mbot:
                            # create MBot on demand (if under limit)
                            try:
                                # check count
                                def _count(conn):
                                    cur = conn.cursor()
                                    cur.execute("SELECT COUNT(*) FROM bots")
                                    return cur.fetchone()[0]
                                curcnt = sqlite_exec_retry(CBOT_DB, lambda conn: _count(conn))
                                if curcnt <= MAX_MBOTS:
                                    mb = MBot(alias=alias, token=token, username=username, shared_executor=cbot_instance.executor)
                                    cbot_instance.mbots[alias] = mb
                                    cbot_instance.token_map[token] = alias
                                    found_mbot = mb
                            except Exception:
                                found_mbot = None
            except Exception:
                found_mbot = None
        if found_mbot:
            bot_obj = found_mbot.bot
            dp = found_mbot.dp
        else:
            # last resort: try create short-lived Bot/Dispatcher to process update
            try:
                bot_obj = Bot(token=token)
                dp = Dispatcher(bot_obj, None, use_context=True, workers=1)
            except Exception:
                logger.warning("Update for unknown token and cannot create Bot.")
                return jsonify({"ok": False}), 200
    try:
        update = Update.de_json(data, bot_obj)
    except Exception as e:
        logger.exception("Failed to deserialize update: %s", e)
        return jsonify({"ok": False}), 200
    try:
        dp.process_update(update)
    except Exception:
        logger.exception("Error processing update")
    return jsonify({"ok": True}), 200

# Helper to set webhooks for all registered bots to WEBHOOK_BASE/<token>
def set_all_webhooks(manual_base: Optional[str] = None):
    base = manual_base or WEBHOOK_BASE
    if not base:
        logger.warning("WEBHOOK_BASE not set")
        return []
    def _get_tokens(conn):
        cur = conn.cursor()
        cur.execute("SELECT token FROM bots")
        return [r[0] for r in cur.fetchall()]
    try:
        tokens = sqlite_exec_retry(CBOT_DB, lambda conn: _get_tokens(conn))
    except Exception:
        tokens = []
    results = []
    for t in tokens:
        try:
            url = f"{base}/{t}"
            ok = Bot(token=t).set_webhook(url=url)
            results.append((t[-8:], ok))
            logger.info("Set webhook for token ending %s -> %s", t[-8:], ok)
        except Exception as e:
            results.append((t[-8:], f"Error: {e}"))
            logger.exception("Failed to set webhook for token ending %s: %s", t[-8:], e)
    return results

# -------------------------
# CLI / local run helpers
# -------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Owner MBots webhook server (env version)")
    parser.add_argument("--run", action="store_true", help="Run Flask dev server")
    parser.add_argument("--set-webhooks", action="store_true", help="Set webhooks for all registered bots to WEBHOOK_BASE")
    parser.add_argument("--set-webhooks-base", default=None, help="Override WEBHOOK_BASE")
    args = parser.parse_args()
    if args.set_webhooks:
        res = set_all_webhooks(args.set_webhooks_base)
        for r in res:
            print(r)
    if args.run:
        logger.info("Running local Flask server on http://0.0.0.0:5000")
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
    else:
        if cbot_instance:
            logger.info("CBot ready. Flask app exposed as 'app'. Deploy with gunicorn owner_mbots_render_env:app")
        else:
            logger.warning("CBot instance not created. Set CBOT_TOKEN in environment before deploying.")
