# main.py — Quantum Analyzer VPS Server (Fully Fixed & Optimized)
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import firebase_admin
from firebase_admin import credentials, db
import requests as rq
import hmac as _hmac
import hashlib, os, time, logging, threading, datetime, json, base64, re
from collections import defaultdict

# Prediction engine (server.py) — same process, same FastAPI app
import server
from server import (
    router as prediction_router,
    start_prediction_engine,
    read_prediction, read_both, engine_status_payload,
    read_results, read_latest_result,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
# ⬇️⬇️⬇️ APNI PURANI VALUES YAHAN PASTE KARO (jaise pehle thi) ⬇️⬇️⬇️
BRIDGE_SECRET   = "PASTE_YOUR_BRIDGE_SECRET_HERE"     # bridge (Vercel) wala same secret
FIREBASE_DB_URL = "PASTE_YOUR_FIREBASE_DB_URL_HERE"   # e.g. https://xxxx.firebaseio.com
TG_CHAT_ID      = "PASTE_YOUR_ADMIN_CHAT_ID_HERE"     # admin approval chat id
# ⬆️⬆️⬆️ ye 3 values aapke purane main.py me thi, wahi yahan daal do ⬆️⬆️⬆️

# Telegram bot token (already filled):
TG_BOT_TOKEN    = "8802337078:AAG_unL2WE3JqIs2P7ixTMTrLoHGLIWjjqk"
TG_API          = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"

# ── Telegram verification (channel-join gate) — already filled ──
BOT_USERNAME    = "xx_drago_bot"                       # bot username (without @)
TG_CHANNEL_ID   = -1002782160527                       # channel users must join
CHANNEL_INVITE  = "https://t.me/+t_giD22Lulk0NmE1"     # "Join Channel" link

DAY_MS = 86400000

def parse_duration_to_days(d: str) -> int:
    """Best-effort parse of a human duration string into days.
    '1 Month'->30, '7 Days'->7, '1 Year'->365, '3 Months'->90,
    'Lifetime'->36500. Falls back to the bare number as days."""
    if not d:
        return 0
    s = str(d).strip().lower()
    if any(k in s for k in ("life", "unlimited", "forever", "permanent")):
        return 36500
    m = re.search(r"(\d+)", s)
    n = int(m.group(1)) if m else 1
    if any(k in s for k in ("year", "yr", "saal")):   return n * 365
    if any(k in s for k in ("month", "mah")):         return n * 30
    if any(k in s for k in ("week", "hafta")):        return n * 7
    if any(k in s for k in ("day", "din")):           return n
    if any(k in s for k in ("hour", "ghanta")):       return max(1, n // 24)
    return n

def tg_verification_enabled() -> bool:
    """Global switch (admin-controlled) for Telegram verification.
    Stored in Firebase at settings/telegram_verification. Default ON.
    When OFF, no user (new or old) is gated by Telegram verification."""
    try:
        v = db.reference("settings/telegram_verification").get()
        if v is None:
            return True
        return bool(v)
    except Exception as e:
        logger.error(f"tg_verification_enabled: {e}")
        return True

# ─────────────────────────────────────────────
# FIREBASE INIT
# ─────────────────────────────────────────────
try:
    cred = credentials.Certificate("firebase-adminsdk.json")
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})
    logger.info("✅ Firebase initialized.")
except Exception as e:
    logger.critical(f"Firebase init failed: {e}")
    raise SystemExit(1)

# ─────────────────────────────────────────────
# RATE LIMITER  (per-IP, in-memory)
# ─────────────────────────────────────────────
_rate: dict = defaultdict(list)

def check_rate_limit(request: Request, limit: int = 20, window: int = 60):
    ip  = request.client.host
    now = time.time()
    _rate[ip] = [t for t in _rate[ip] if now - t < window]
    if len(_rate[ip]) >= limit:
        raise HTTPException(429, "Too many requests. Try again later.")
    _rate[ip].append(now)

# ─────────────────────────────────────────────
# BRIDGE AUTH
# ─────────────────────────────────────────────
def verify_bridge(x_bridge_secret: str = Header(None)):
    if not x_bridge_secret:
        raise HTTPException(403, "Unauthorized.")
    if not _hmac.compare_digest(x_bridge_secret.encode(), BRIDGE_SECRET.encode()):
        raise HTTPException(403, "Unauthorized.")
    return True

# ─────────────────────────────────────────────
# SESSION SIGNATURE  (daily rotating, 3-day window)
# BUG FIX: was using hmac.new() which doesn't exist — correct is hmac.new()
# Actually Python's hmac module uses hmac.new() — but import alias was wrong.
# Fixed: using _hmac.new() with the correct alias.
# ─────────────────────────────────────────────
def _make_sig(uid: str, day: str) -> str:
    return _hmac.new(
        BRIDGE_SECRET.encode(),
        f"{uid}:{day}".encode(),
        hashlib.sha256
    ).hexdigest()

def generate_session_sig(uid: str) -> str:
    return _make_sig(uid, str(int(time.time()) // 86400))

def verify_session_sig(uid: str, sig: str) -> bool:
    today = int(time.time()) // 86400
    for offset in [0, -1, -2]:
        if _hmac.compare_digest(_make_sig(uid, str(today + offset)), sig):
            return True
    return False

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def get_user(uid: str) -> dict:
    user = db.reference(f"users/{uid}").get()
    if not user:
        raise HTTPException(404, "User not found.")
    return user

def check_active(user: dict):
    s = user.get("status", "active")
    if s in ("banned", "blocked", "paused"):
        raise HTTPException(403, s)

def require_paid(user: dict):
    """Prediction sirf PAID active plan ko. Free / none / expired ko block."""
    check_active(user)
    sub = user.get("subscription_status", "none")
    if sub != "active":
        # free, none, expired, rejected — sabko prediction se rok do
        raise HTTPException(403, "upgrade_required")
    exp = int(user.get("plan_expires_at") or 0)
    if exp and exp < int(time.time() * 1000):
        raise HTTPException(403, "upgrade_required")

def ist_now(ts_ms: int) -> str:
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000) + datetime.timedelta(hours=5, minutes=30)
    return dt.strftime("%d %b %Y, %I:%M %p IST")

# ─────────────────────────────────────────────
# GAME HELPERS
# ─────────────────────────────────────────────
VALID_CATEGORIES = {"30s", "1min"}   # 30 Seconds / 1 Minute

def require_admin(uid: str) -> dict:
    """Only users with role == 'admin' may manage games."""
    user = get_user(uid)
    check_active(user)
    if user.get("role") != "admin":
        raise HTTPException(403, "Admin access required.")
    return user

def slugify(text: str) -> str:
    """Turn a game name into a URL-safe slug, e.g. 'Wingo Lite' -> 'wingo-lite'."""
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower()).strip("-")
    return s or "game"

def unique_slug(base: str, exclude_id: str = None) -> str:
    """Guarantee slug uniqueness across all games (so /game/{slug} is unambiguous)."""
    games = db.reference("games").get() or {}
    existing = set()
    for gid, g in games.items():
        if gid == exclude_id or not g:
            continue
        if g.get("slug"):
            existing.add(g["slug"])
    slug, i = base, 2
    while slug in existing:
        slug = f"{base}-{i}"
        i += 1
    return slug

def serialize_game(gid: str, g: dict) -> dict:
    return {
        "id":         gid,
        "name":       g.get("name", ""),
        "image":      g.get("image", ""),
        "link":       g.get("link", ""),
        "category":   g.get("category", ""),
        "slug":       g.get("slug", ""),
        "created_at": int(g.get("created_at") or 0),
    }

# ─────────────────────────────────────────────
# TELEGRAM HELPERS
# ─────────────────────────────────────────────
def tg_send(text: str, markup: dict = None):
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if markup:
            payload["reply_markup"] = json.dumps(markup)
        r = rq.post(f"{TG_API}/sendMessage", json=payload, timeout=8)
        if not r.ok:
            logger.warning(f"tg_send failed: {r.text[:200]}")
    except Exception as e:
        logger.error(f"tg_send: {e}")

def tg_answer(cb_id: str, text: str, alert: bool = True):
    try:
        rq.post(f"{TG_API}/answerCallbackQuery",
                json={"callback_query_id": cb_id, "text": text, "show_alert": alert},
                timeout=5)
    except Exception as e:
        logger.error(f"tg_answer: {e}")

def tg_edit(chat_id, msg_id, label: str):
    try:
        rq.post(f"{TG_API}/editMessageReplyMarkup", json={
            "chat_id":      chat_id,
            "message_id":   msg_id,
            "reply_markup": json.dumps({"inline_keyboard": [[{"text": label, "callback_data": "done"}]]})
        }, timeout=5)
    except Exception as e:
        logger.error(f"tg_edit: {e}")

# ─────────────────────────────────────────────
# TELEGRAM VERIFICATION HELPERS
# ─────────────────────────────────────────────
def b64url_encode(s: str) -> str:
    """email -> URL-safe code (valid Telegram start param & Firebase key)."""
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def b64url_decode(code: str) -> str:
    pad = "=" * (-len(code) % 4)
    return base64.urlsafe_b64decode((code + pad).encode()).decode()

def tg_send_to(chat_id, text: str, markup: dict = None):
    """Send a message to a specific user/chat (not the admin chat)."""
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                   "disable_web_page_preview": True}
        if markup:
            payload["reply_markup"] = json.dumps(markup)
        r = rq.post(f"{TG_API}/sendMessage", json=payload, timeout=8)
        if not r.ok:
            logger.warning(f"tg_send_to failed: {r.text[:200]}")
    except Exception as e:
        logger.error(f"tg_send_to: {e}")

def tg_is_member(user_id) -> bool:
    """True if the Telegram user is a member of the required channel."""
    try:
        r = rq.get(f"{TG_API}/getChatMember",
                   params={"chat_id": TG_CHANNEL_ID, "user_id": user_id}, timeout=8)
        d = r.json()
        if not d.get("ok"):
            logger.warning(f"getChatMember not ok: {d.get('description')}")
            return False
        return d["result"]["status"] in ("member", "administrator", "creator")
    except Exception as e:
        logger.error(f"tg_is_member: {e}")
        return False

def _tg_full_name(frm: dict) -> str:
    return f"{frm.get('first_name','')} {frm.get('last_name','')}".strip()

def finalize_verify(code: str, uid: str, frm: dict, chat_id) -> bool:
    """Link the Telegram account to the Google user and mark verified."""
    tg_uid    = frm.get("id")
    username  = frm.get("username", "")
    tg_name   = _tg_full_name(frm)

    # Anti-abuse: a Telegram account may verify only one Google account.
    existing = db.reference(f"tg_users/{tg_uid}").get()
    if existing and existing != uid:
        tg_send_to(chat_id, "⚠️ Ye Telegram account already kisi aur account se linked hai. "
                            "Support: https://t.me/xx_drago")
        return False

    db.reference(f"users/{uid}").update({
        "telegram_verified":    True,
        "telegram_id":          tg_uid,
        "telegram_username":    username,
        "telegram_name":        tg_name,
        "telegram_verified_at": {".sv": "timestamp"},
    })
    db.reference(f"tg_users/{tg_uid}").set(uid)
    db.reference(f"tg_codes/{code}").update({"verified": True, "tg_id": tg_uid})

    uname = f"@{username}" if username else "—"
    tg_send_to(chat_id,
        "✅ <b>Verification Complete!</b>\n\n"
        f"👤 <b>Name :</b> {tg_name or 'N/A'}\n"
        f"🔗 <b>User :</b> {uname}\n"
        f"🆔 <b>ID   :</b> <code>{tg_uid}</code>\n\n"
        "Ab app me wapas jao — aapka account verify ho gaya hai. 🎉")
    logger.info(f"TG verified — uid={uid} tg_id={tg_uid}")
    return True

def prompt_join(chat_id, code: str):
    tg_send_to(chat_id,
        "🔐 <b>Verification Required</b>\n\n"
        "Verify hone ke liye pehle hamara official channel join karo, "
        "phir niche <b>✅ I've Joined</b> button dabao.",
        {"inline_keyboard": [
            [{"text": "📢 Join Channel", "url": CHANNEL_INVITE}],
            [{"text": "✅ I've Joined",  "callback_data": f"vjoin|{code}"}],
        ]})

# ─────────────────────────────────────────────
# TELEGRAM /start MESSAGE HANDLER
# ─────────────────────────────────────────────
def handle_message(msg: dict):
    try:
        chat    = msg.get("chat") or {}
        chat_id = chat.get("id")
        frm     = msg.get("from") or {}
        text    = (msg.get("text") or "").strip()

        if not text.startswith("/start"):
            return

        parts = text.split(maxsplit=1)
        code  = parts[1].strip() if len(parts) > 1 else ""

        if not code:
            tg_send_to(chat_id, "👋 Welcome to <b>DRAGO PREDICTOR</b>!\n\n"
                                "Verify karne ke liye app/website se <b>Verify on Telegram</b> "
                                "button dabao — wahi se aapko sahi link milega.")
            return

        link = db.reference(f"tg_codes/{code}").get()
        if not link or not link.get("uid"):
            tg_send_to(chat_id, "❌ Invalid ya expired verification link.\n"
                                "App me wapas jaakar dobara <b>Verify on Telegram</b> dabao.")
            return

        uid = link.get("uid")
        # Save the user's Telegram identity against this code.
        db.reference(f"tg_codes/{code}").update({
            "tg_id":       frm.get("id"),
            "tg_chat":     chat_id,
            "tg_username": frm.get("username", ""),
            "tg_name":     _tg_full_name(frm),
        })

        if tg_is_member(frm.get("id")):
            finalize_verify(code, uid, frm, chat_id)
        else:
            prompt_join(chat_id, code)
    except Exception as e:
        logger.error(f"handle_message: {e}")

# ─────────────────────────────────────────────
# TELEGRAM CALLBACK HANDLER
# ────────────────────────────���────────────────
def handle_callback(cb_id: str, data: str, chat_id, msg_id, cb_from: dict = None):
    try:
        if data == "done":
            tg_answer(cb_id, "Already processed.", alert=False)
            return

        # ── Channel-join verification ("I've Joined" button) ──
        if data.startswith("vjoin|"):
            code = data.split("|", 1)[1]
            link = db.reference(f"tg_codes/{code}").get()
            if not link or not link.get("uid"):
                tg_answer(cb_id, "❌ Link expired. App se dobara try karo.")
                return
            frm = cb_from or {}
            if tg_is_member(frm.get("id")):
                if finalize_verify(code, link.get("uid"), frm, chat_id):
                    tg_answer(cb_id, "✅ Verified! Ab app me wapas jao.", alert=True)
                    tg_edit(chat_id, msg_id, "✅ VERIFIED")
                else:
                    tg_answer(cb_id, "⚠️ Ye Telegram account already linked hai.", alert=True)
            else:
                tg_answer(cb_id, "❌ Abhi channel join nahi kiya. Pehle Join Channel dabao, phir try karo.", alert=True)
            return

        parts = data.split("|")
        if len(parts) != 3:
            logger.warning(f"Bad callback data: {data}")
            tg_answer(cb_id, "❌ Invalid callback data.")
            return

        action, pay_key, uid = parts
        if action not in ("approve", "reject", "ban"):
            tg_answer(cb_id, "❌ Unknown action.")
            return

        user_ref = db.reference(f"users/{uid}")
        pay_ref  = db.reference(f"payments/{pay_key}")
        user     = user_ref.get()
        payment  = pay_ref.get()

        if not user or not payment:
            tg_answer(cb_id, "❌ User or payment not found!")
            logger.warning(f"Not found — uid:{uid} pay_key:{pay_key}")
            return

        # Prevent double-processing
        if payment.get("status") in ("approved", "rejected", "banned"):
            tg_answer(cb_id, f"⚠️ Already {payment.get('status')}.", alert=False)
            return

        name = user.get("name", "Unknown")
        plan = f"{payment.get('plan', '')} {payment.get('duration', '')}".strip()

        if action == "approve":
            now_ms      = int(time.time() * 1000)
            new_days    = parse_duration_to_days(payment.get("duration", ""))
            prev_expiry = int(user.get("plan_expires_at") or 0)
            was_active  = user.get("subscription_status") == "active"
            # Carry over remaining time of the previous plan onto the new one.
            base_ms      = max(now_ms, prev_expiry) if was_active else now_ms
            new_expiry   = base_ms + new_days * DAY_MS
            carried_days = max(0, round((prev_expiry - now_ms) / DAY_MS)) if prev_expiry > now_ms else 0
            user_ref.update({
                "status":              "active",
                "subscription_status": "active",
                "plan":                payment.get("plan", ""),
                "plan_index":          payment.get("plan_index", 0),
                "plan_duration":       payment.get("duration", ""),
                "plan_started_at":     now_ms,
                "plan_expires_at":     new_expiry,
                "subscribed_at":       {".sv": "timestamp"},
            })
            pay_ref.update({"status": "approved", "carried_days": carried_days})
            extra = f"\n➕ {carried_days} purane din add hue (carry-over)." if carried_days else ""
            tg_answer(cb_id, f"✅ Approved!\n{name} ka plan '{plan}' activate ho gaya.{extra}")
            tg_edit(chat_id, msg_id, f"✅ APPROVED — {name}")

        elif action == "reject":
            user_ref.update({"subscription_status": "rejected"})
            pay_ref.update({"status": "rejected"})
            tg_answer(cb_id, f"❌ Rejected!\n{name} ki payment reject ho gayi.")
            tg_edit(chat_id, msg_id, f"❌ REJECTED — {name}")

        elif action == "ban":
            user_ref.update({"status": "banned", "subscription_status": "none"})
            pay_ref.update({"status": "banned"})
            tg_answer(cb_id, f"🚫 Banned!\n{name} ab access nahi kar sakta.")
            tg_edit(chat_id, msg_id, f"🚫 BANNED — {name}")

        logger.info(f"TG '{action}' done — uid={uid}")

    except Exception as e:
        logger.error(f"handle_callback error: {e}")
        try:
            tg_answer(cb_id, f"⚠️ Server error: {str(e)[:80]}")
        except:
            pass

# ─────────────────────────────────────────────
# TELEGRAM POLLING THREAD
# ─────────────────────────────────────────────
_offset = None

def _polling_loop():
    global _offset
    logger.info("🤖 Telegram polling started.")

    # Delete any existing webhook so long-polling works
    try:
        r = rq.post(f"{TG_API}/deleteWebhook", json={"drop_pending_updates": False}, timeout=5)
        logger.info(f"deleteWebhook: {r.json().get('description', 'ok')}")
    except Exception as e:
        logger.warning(f"deleteWebhook error: {e}")

    while True:
        try:
            params = {
                "timeout":          25,
                "allowed_updates":  ["callback_query", "message"],
            }
            if _offset is not None:
                params["offset"] = _offset

            resp   = rq.get(f"{TG_API}/getUpdates", params=params, timeout=35)
            result = resp.json()

            if not result.get("ok"):
                logger.warning(f"getUpdates error: {result}")
                time.sleep(5)
                continue

            for update in result.get("result", []):
                _offset = update["update_id"] + 1

                # /start verification messages
                msg_update = update.get("message")
                if msg_update:
                    handle_message(msg_update)
                    continue

                cb = update.get("callback_query")
                if not cb:
                    continue
                cb_id   = cb.get("id")
                cb_data = cb.get("data", "")
                msg     = cb.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                msg_id  = msg.get("message_id")
                logger.info(f"CB: {cb_data}")
                handle_callback(cb_id, cb_data, chat_id, msg_id, cb.get("from") or {})

        except rq.exceptions.ReadTimeout:
            pass  # normal for long polling
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)

# ─────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(_app):
    # 1) Telegram bot polling (auth/payment)
    t = threading.Thread(target=_polling_loop, daemon=True, name="tg-poll")
    t.start()
    logger.info("Telegram polling thread started.")

    # 2) Prediction engine — 3 streams (TRX-1m, Wingo-1m, Wingo-30s)
    #    background threads me chalu, server ke saath-saath.
    try:
        start_prediction_engine()
        logger.info("Prediction engine streams started.")
    except Exception as e:
        logger.error(f"Prediction engine start failed: {e}")

    yield

# ──────��──────────────────────────────────────
# APP + CORS
# ─────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["X-Bridge-Secret", "Content-Type"],
)

# Prediction endpoints (/api/prediction/...) — server.py se
app.include_router(prediction_router)

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────
class AuthReq(BaseModel):
    access_token: str

class SessBase(BaseModel):
    uid: str
    session_signature: str

class PayDetailReq(BaseModel):
    uid: str
    session_signature: str
    plan_index: int

class UtrReq(BaseModel):
    uid: str
    session_signature: str
    plan_index: int
    utr: str

class CalcReq(BaseModel):
    uid: str
    session_signature: str
    data: dict

class PredictReq(BaseModel):
    uid: str
    session_signature: str
    game: str                      # "trx" | "wingo"
    timeframe: str                 # "1m" | "30s"
    target: str | None = None      # "color" | "size" | None (dono)

# ── PREMIUM (.drago) DATA MODELS ──────────────
# Browser ka client-side ".drago" engine yahi RAW results le kar SAME algorithm
# device par chalata hai. Free + paid dono ke liye open (sirf active user).
class ResultsReq(BaseModel):
    uid: str
    session_signature: str
    game: str                      # "trx" | "wingo"
    timeframe: str                 # "1m" | "30s"
    limit: int | None = 200        # kitne recent results (max 300)

# ── GAME MODELS ───────────────────────────────
class GamesReq(BaseModel):
    uid: str
    session_signature: str
    category: str | None = None   # "30s" | "1min" | None (all)

class GameDetailReq(BaseModel):
    uid: str
    session_signature: str
    slug: str

class AdminAddGameReq(BaseModel):
    uid: str
    session_signature: str
    name: str
    image: str = ""
    link: str = ""
    category: str                 # "30s" | "1min"

class AdminUpdateGameReq(BaseModel):
    uid: str
    session_signature: str
    game_id: str
    name: str | None = None
    image: str | None = None
    link: str | None = None
    category: str | None = None

class AdminDeleteGameReq(BaseModel):
    uid: str
    session_signature: str
    game_id: str

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "active", "mode": "polling"}


# ── AUTH ──────────────────────────────────────
@app.post("/verify-token")
def verify_token(req: AuthReq, request: Request,
                 _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    try:
        resp = rq.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {req.access_token}"},
            timeout=6
        )
        if resp.status_code != 200:
            raise HTTPException(401, "Google authentication failed.")

        g       = resp.json()
        uid     = g.get("sub")
        email   = g.get("email")
        name    = g.get("name", "User")
        picture = g.get("picture", "")

        if not uid or not email:
            raise HTTPException(400, "Invalid Google token.")

        ref  = db.reference(f"users/{uid}")
        user = ref.get()

        if not user:
            user = {
                "uid": uid, "email": email, "name": name,
                "picture": picture, "role": "user",
                "status": "active", "subscription_status": "none",
                "telegram_verified": False,
                "created_at": {".sv": "timestamp"},
            }
            ref.set(user)
        else:
            ref.update({"name": name, "picture": picture})
            user["name"]    = name
            user["picture"] = picture

        check_active(user)

        return {
            "status":            "success",
            "session_signature": generate_session_sig(uid),
            "telegram_verification_enabled": tg_verification_enabled(),
            "user": {
                "uid":                 uid,
                "name":                name,
                "email":               email,
                "picture":             picture,
                "role":                user.get("role", "user"),
                "status":              user.get("status", "active"),
                "subscription_status": user.get("subscription_status", "none"),
                "telegram_verified":   bool(user.get("telegram_verified", False)),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"verify_token: {e}")
        raise HTTPException(500, "Authentication failed.")


# ── PLANS ───��─────────────────────────────────
@app.post("/plans")
def get_plans(req: SessBase, request: Request,
              _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    try:
        raw_plans = db.reference("plans").get() or []
        payments  = db.reference("payments").get() or {}

        # Find pending plan index for this user
        pending_idx = None
        if isinstance(payments, dict):
            for pval in payments.values():
                if (pval and pval.get("uid") == req.uid
                        and pval.get("status") == "pending"):
                    pending_idx = pval.get("plan_index")
                    break

        safe = []
        for i, p in enumerate(raw_plans):
            if not p:
                continue
            safe.append({
                "index":    i,
                "p":        p.get("p", ""),
                "d":        p.get("d", ""),
                "f":        p.get("f", []),
                "discount": p.get("discount", ""),
                "has_upi":  bool((p.get("upi") or "").strip()),
                "has_qr":   bool((p.get("qr") or "").strip()),
            })

        return {
            "status":              "success",
            "plans":               safe,
            "pending_plan_index":  pending_idx,
            "account_status":      user.get("status", "active"),
            "subscription_status": user.get("subscription_status", "none"),
            "active_plan":         user.get("plan", ""),
            "active_plan_index":   user.get("plan_index"),
            "active_plan_duration": user.get("plan_duration", ""),
            "plan_expires_at":     int(user.get("plan_expires_at") or 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_plans: {e}")
        raise HTTPException(500, "Failed to load plans.")


# ── PAYMENT DETAIL ────────────────────────────
@app.post("/get-payment-detail")
def payment_detail(req: PayDetailReq, request: Request,
                   _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    try:
        raw = db.reference("plans").get() or []
        if not (0 <= req.plan_index < len(raw)):
            raise HTTPException(404, "Plan not found.")
        plan = raw[req.plan_index]
        if not plan:
            raise HTTPException(404, "Plan not found.")

        return {
            "status":    "success",
            "plan_name": f"{plan.get('p','')} {plan.get('d','')}".strip(),
            "price":     plan.get("p", ""),
            "upi":       plan.get("upi", ""),
            "qr":        plan.get("qr", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"payment_detail: {e}")
        raise HTTPException(500, "Failed to load payment detail.")


# ── SUBMIT UTR ────────────────────────────────
@app.post("/submit-utr")
def submit_utr(req: UtrReq, request: Request,
               _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    utr = (req.utr or "").strip()
    # Accept numeric OR alphanumeric UTR (some banks send alphanum)
    if len(utr) < 8 or len(utr) > 30:
        raise HTTPException(400, "UTR must be 8–30 characters.")

    try:
        raw = db.reference("plans").get() or []
        if not (0 <= req.plan_index < len(raw)):
            raise HTTPException(404, "Invalid plan.")
        plan = raw[req.plan_index] or {}

        now_ms  = int(time.time() * 1000)
        pay_ref = db.reference("payments").push()
        pay_key = pay_ref.key
        pay_ref.set({
            "uid":        req.uid,
            "name":       user.get("name", ""),
            "email":      user.get("email", ""),
            "plan":       plan.get("p", ""),
            "plan_index": req.plan_index,
            "duration":   plan.get("d", ""),
            "utr":        utr,
            "status":     "pending",
            "timestamp":  now_ms,
        })

        plan_label = f"{plan.get('p','')} {plan.get('d','')}".strip()
        tg_send(
            f"💰 <b>New Payment Request</b>\n\n"
            f"👤 <b>Name  :</b> {user.get('name','')}\n"
            f"📧 <b>Email :</b> {user.get('email','')}\n"
            f"📦 <b>Plan  :</b> {plan_label}\n"
            f"🔢 <b>UTR   :</b> <code>{utr}</code>\n"
            f"🕐 <b>Time  :</b> {ist_now(now_ms)}",
            {
                "inline_keyboard": [[
                    {"text": "✅ Approve", "callback_data": f"approve|{pay_key}|{req.uid}"},
                    {"text": "❌ Reject",  "callback_data": f"reject|{pay_key}|{req.uid}"},
                    {"text": "🚫 Ban",     "callback_data": f"ban|{pay_key}|{req.uid}"},
                ]]
            }
        )
        return {"status": "success", "message": "UTR submitted. Awaiting approval."}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"submit_utr: {e}")
        raise HTTPException(500, "Failed to submit UTR.")


# ── ACCOUNT STATUS ────────────────────────────
@app.post("/account-status")
def account_status(req: SessBase, request: Request,
                   _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=30)  # higher limit — called frequently
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    try:
        user = db.reference(f"users/{req.uid}").get() or {}
        return {
            "status":              "success",
            "account_status":      user.get("status", "active"),
            "subscription_status": user.get("subscription_status", "none"),
            "plan":                user.get("plan", ""),
            "plan_duration":       user.get("plan_duration", ""),
            "plan_expires_at":     int(user.get("plan_expires_at") or 0),
            "telegram_verification_enabled": tg_verification_enabled(),
            "telegram_verified":   bool(user.get("telegram_verified", False)),
        }
    except Exception as e:
        logger.error(f"account_status: {e}")
        raise HTTPException(500, "Status check failed.")


# ── FULL PROFILE ──────────────────────────────
@app.post("/profile")
def profile(req: SessBase, request: Request,
            _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=30)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    try:
        user = db.reference(f"users/{req.uid}").get() or {}
        return {
            "status":              "success",
            "uid":                 req.uid,
            "email":               user.get("email", ""),
            "name":                user.get("name", ""),
            "picture":             user.get("picture", ""),
            "account_status":      user.get("status", "active"),
            "subscription_status": user.get("subscription_status", "none"),
            "plan":                user.get("plan", ""),
            "plan_duration":       user.get("plan_duration", ""),
            "plan_expires_at":     int(user.get("plan_expires_at") or 0),
            "telegram_verified":   bool(user.get("telegram_verified", False)),
            "telegram_id":         user.get("telegram_id", ""),
            "telegram_username":   user.get("telegram_username", ""),
            "telegram_name":       user.get("telegram_name", ""),
        }
    except Exception as e:
        logger.error(f"profile: {e}")
        raise HTTPException(500, "Profile fetch failed.")


# ── TELEGRAM: START / GENERATE DEEP-LINK CODE ─
@app.post("/tg-start")
def tg_start(req: SessBase, request: Request,
             _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    try:
        email = user.get("email", "") or req.uid
        # Unique id derived from the user's email (URL-safe encoded).
        code  = b64url_encode(email)
        db.reference(f"tg_codes/{code}").update({
            "uid":   req.uid,
            "email": email,
            "ts":    {".sv": "timestamp"},
        })
        return {
            "status":            "success",
            "bot":               BOT_USERNAME,
            "code":              code,
            "deep_link":         f"https://t.me/{BOT_USERNAME}?start={code}",
            "channel_invite":    CHANNEL_INVITE,
            "telegram_verification_enabled": tg_verification_enabled(),
            "telegram_verified": bool(user.get("telegram_verified", False)),
            "telegram_name":     user.get("telegram_name", ""),
            "telegram_username": user.get("telegram_username", ""),
            "telegram_id":       user.get("telegram_id", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"tg_start: {e}")
        raise HTTPException(500, "Failed to start verification.")


# ── TELEGRAM: POLL VERIFICATION STATUS ────────
@app.post("/tg-status")
def tg_status(req: SessBase, request: Request,
              _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)  # polled frequently
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    try:
        user = db.reference(f"users/{req.uid}").get() or {}
        return {
            "status":              "success",
            "account_status":      user.get("status", "active"),
            "subscription_status": user.get("subscription_status", "none"),
            "telegram_verification_enabled": tg_verification_enabled(),
            "telegram_verified":   bool(user.get("telegram_verified", False)),
            "telegram_name":       user.get("telegram_name", ""),
            "telegram_username":   user.get("telegram_username", ""),
            "telegram_id":         user.get("telegram_id", ""),
        }
    except Exception as e:
        logger.error(f"tg_status: {e}")
        raise HTTPException(500, "Status check failed.")


# ── SESSION REFRESH ───────────────────────────
@app.post("/refresh-session")
def refresh_session(req: SessBase, request: Request,
                    _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    try:
        user = get_user(req.uid)
        check_active(user)
        return {"status": "success", "session_signature": generate_session_sig(req.uid)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"refresh_session: {e}")
        raise HTTPException(500, "Refresh failed.")


# ── CALCULATE ─────────────────────────────────
@app.post("/calculate")
def calculate(req: CalcReq, request: Request,
              _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    try:
        user = get_user(req.uid)
        check_active(user)
        val    = sum(len(str(v)) for v in req.data.values()) % 100
        result = {
            "prediction":     "High Stability" if val > 50 else "Dynamic Flux",
            "score":          val,
            "security_stamp": hashlib.sha256(
                f"{req.uid}-{val}-{int(time.time())}".encode()
            ).hexdigest(),
        }
        db.reference(f"history/{req.uid}").push().set({
            "input": req.data, "result": result, "timestamp": {".sv": "timestamp"}
        })
        return {"status": "success", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"calculate: {e}")
        raise HTTPException(500, "Calculation failed.")


# ── PREDICTION (self-learning engine) — PAID ──
@app.post("/get-prediction")
def get_prediction(req: PredictReq, request: Request,
                   _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)   # game page polls this frequently
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    user = get_user(req.uid)
    require_paid(user)                     # sirf PAID active plan (free block)
    try:
        if req.target:
            data = read_prediction(req.game, req.timeframe, req.target)
        else:
            data = read_both(req.game, req.timeframe)
        return {"status": "success", "result": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get-prediction: {e}")
        raise HTTPException(500, "Prediction failed.")


# ─────────────────────────────────────────────
# PREMIUM (.drago) — RAW RESULTS (free + paid)
# Yahan koi prediction/calculation NAHI hoti. Browser apni .drago file ke
# calculation se khud prediction banata hai; ye sirf raw lottery results deta.
# ─────────────────────────────────────────────
@app.post("/results-latest")
def results_latest(req: ResultsReq, request: Request,
                   _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    user = get_user(req.uid)
    check_active(user)                     # active hona kaafi (free bhi allowed)
    try:
        data = read_latest_result(req.game, req.timeframe)
        return {"status": "success", "result": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"results-latest: {e}")
        raise HTTPException(500, "Results fetch failed.")


@app.post("/results-history")
def results_history(req: ResultsReq, request: Request,
                    _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    user = get_user(req.uid)
    check_active(user)                     # active hona kaafi (free bhi allowed)
    try:
        data = read_results(req.game, req.timeframe, req.limit or 200)
        return {"status": "success", "result": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"results-history: {e}")
        raise HTTPException(500, "Results fetch failed.")


# ─────────────────────────────────────────────
# GAMES — PUBLIC (logged-in users) READ
# ─────────────────────────────────────────────
# ── LIST GAMES (optionally by category) ───────
@app.post("/games")
def list_games(req: GamesReq, request: Request,
               _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)  # home page loads this often
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    cat = (req.category or "").strip()
    if cat and cat not in VALID_CATEGORIES:
        raise HTTPException(400, "Invalid category.")

    try:
        games = db.reference("games").get() or {}
        out = []
        for gid, g in games.items():
            if not g:
                continue
            if cat and g.get("category") != cat:
                continue
            out.append(serialize_game(gid, g))
        # Newest first so freshly added games surface at the front of the slider
        out.sort(key=lambda x: x["created_at"], reverse=True)
        return {"status": "success", "games": out}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"list_games: {e}")
        raise HTTPException(500, "Failed to load games.")


# ── GAME DETAIL BY SLUG (for /game/{slug} page) ──
@app.post("/game-detail")
def game_detail(req: GameDetailReq, request: Request,
                _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request, limit=40)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")

    user = get_user(req.uid)
    check_active(user)

    slug = (req.slug or "").strip()
    if not slug:
        raise HTTPException(400, "Missing slug.")

    try:
        games = db.reference("games").get() or {}
        for gid, g in games.items():
            if g and g.get("slug") == slug:
                return {"status": "success", "game": serialize_game(gid, g)}
        raise HTTPException(404, "Game not found.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"game_detail: {e}")
        raise HTTPException(500, "Failed to load game.")


# ─────────────────────────────────────────────
# GAMES — ADMIN ONLY (manage from admin panel)
# ─────────────────────────────────────────────
# ── ADD GAME ──────────────────────────────────
@app.post("/admin/add-game")
def admin_add_game(req: AdminAddGameReq, request: Request,
                   _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    require_admin(req.uid)

    name = (req.name or "").strip()
    if not name:
        raise HTTPException(400, "Game name is required.")
    if req.category not in VALID_CATEGORIES:
        raise HTTPException(400, "Invalid category.")

    try:
        slug = unique_slug(slugify(name))
        ref  = db.reference("games").push()
        ref.set({
            "name":       name,
            "image":      (req.image or "").strip(),
            "link":       (req.link or "").strip(),
            "category":   req.category,
            "slug":       slug,
            "created_at": {".sv": "timestamp"},
        })
        logger.info(f"Game added — id={ref.key} slug={slug}")
        return {"status": "success", "id": ref.key, "slug": slug}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_add_game: {e}")
        raise HTTPException(500, "Failed to add game.")


# ── UPDATE GAME (name / image / link / category) ──
@app.post("/admin/update-game")
def admin_update_game(req: AdminUpdateGameReq, request: Request,
                      _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    require_admin(req.uid)

    try:
        ref = db.reference(f"games/{req.game_id}")
        g   = ref.get()
        if not g:
            raise HTTPException(404, "Game not found.")

        updates: dict = {}
        if req.name is not None:
            nm = req.name.strip()
            if not nm:
                raise HTTPException(400, "Game name cannot be empty.")
            updates["name"] = nm
            # Keep the slug in sync with the (new) name, staying unique
            updates["slug"] = unique_slug(slugify(nm), exclude_id=req.game_id)
        if req.image is not None:
            updates["image"] = req.image.strip()
        if req.link is not None:
            updates["link"] = req.link.strip()
        if req.category is not None:
            if req.category not in VALID_CATEGORIES:
                raise HTTPException(400, "Invalid category.")
            updates["category"] = req.category

        if not updates:
            raise HTTPException(400, "Nothing to update.")

        ref.update(updates)
        logger.info(f"Game updated — id={req.game_id} fields={list(updates)}")
        return {"status": "success", "id": req.game_id, "slug": updates.get("slug", g.get("slug", ""))}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_update_game: {e}")
        raise HTTPException(500, "Failed to update game.")


# ── DELETE GAME ───────────────────────────────
@app.post("/admin/delete-game")
def admin_delete_game(req: AdminDeleteGameReq, request: Request,
                      _auth: bool = Depends(verify_bridge)):
    check_rate_limit(request)
    if not verify_session_sig(req.uid, req.session_signature):
        raise HTTPException(403, "session_expired")
    require_admin(req.uid)

    try:
        ref = db.reference(f"games/{req.game_id}")
        if not ref.get():
            raise HTTPException(404, "Game not found.")
        ref.delete()
        logger.info(f"Game deleted — id={req.game_id}")
        return {"status": "success", "id": req.game_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_delete_game: {e}")
        raise HTTPException(500, "Failed to delete game.")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8000")))
    logger.info(f"Starting on port {port}…")
    uvicorn.run(app, host="0.0.0.0", port=port)
