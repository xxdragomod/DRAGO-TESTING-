# api/index.py — Vercel Bridge (optimized & fixed)
import os, logging, time
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# ⬇️⬇️⬇️ APNI PURANI VALUES YAHAN PASTE KARO (jaise pehle thi) ⬇️⬇️⬇️
VPS_URL       = os.getenv("VPS_URL", "http://2.56.246.128:30289")          # e.g. http://1.2.3.4:8000
BRIDGE_SECRET = os.getenv("BRIDGE_SECRET", "a3f8c2e1b4d6e9f0e1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4")    # VPS wale main.py se bilkul SAME
# ⬆️⬆️⬆️ dono values aapke purane index.py me thi, wahi yahan daal do ⬆️⬆️⬆️

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["Content-Type"],
)

# ── Rate limiter ──
_rate: dict = defaultdict(list)

def rate_check(request: Request, limit: int = 25, window: int = 60):
    ip  = request.headers.get("x-forwarded-for", request.client.host or "").split(",")[0].strip()
    now = time.time()
    _rate[ip] = [t for t in _rate[ip] if now - t < window]
    if len(_rate[ip]) >= limit:
        raise HTTPException(429, "Too many requests.")
    _rate[ip].append(now)

def bh():
    return {"X-Bridge-Secret": BRIDGE_SECRET, "Content-Type": "application/json"}

async def fwd(path: str, body: dict, request: Request, rl_limit: int = 25):
    rate_check(request, limit=rl_limit)
    async with httpx.AsyncClient(timeout=14.0) as client:
        try:
            r = await client.post(f"{VPS_URL}{path}", json=body, headers=bh())
            # Pass through 400/403/404/429 with original detail
            if r.status_code in (400, 403, 404, 429):
                try:
                    detail = r.json().get("detail", r.text[:200])
                except Exception:
                    detail = r.text[:200]
                raise HTTPException(r.status_code, detail)
            if r.status_code != 200:
                logger.warning(f"VPS {path} → {r.status_code}: {r.text[:200]}")
                raise HTTPException(502, "Upstream error.")
            return r.json()
        except httpx.TimeoutException:
            logger.error(f"VPS timeout: {path}")
            raise HTTPException(504, "Server timeout. Try again.")
        except httpx.RequestError as e:
            logger.error(f"VPS connection error {path}: {e}")
            raise HTTPException(503, "Server unavailable. Try again later.")

# ── Routes ──
@app.get("/")
def root():
    return {"status": "active", "bridge": "online"}

@app.post("/api/auth")
async def api_auth(payload: dict, request: Request):
    if not payload.get("access_token"):
        raise HTTPException(400, "Missing access_token.")
    ip = request.headers.get("x-forwarded-for", request.client.host or "").split(",")[0].strip()
    return await fwd("/verify-token", {
        "access_token": payload["access_token"],
        "device_id":    payload.get("device_id", "unknown"),
        "client_ip":    ip,
    }, request)

@app.post("/api/plans")
async def api_plans(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/plans", {"uid": uid, "session_signature": sig}, request)

@app.post("/api/payment-detail")
async def api_pay_detail(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    idx = payload.get("plan_index")
    if not uid or not sig or idx is None:
        raise HTTPException(400, "Missing uid, session_signature or plan_index.")
    return await fwd("/get-payment-detail", {
        "uid": uid, "session_signature": sig, "plan_index": int(idx)
    }, request)

@app.post("/api/submit-utr")
async def api_submit_utr(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    utr = payload.get("utr")
    idx = payload.get("plan_index")
    if not uid or not sig or not utr or idx is None:
        raise HTTPException(400, "Missing required fields.")
    return await fwd("/submit-utr", {
        "uid": uid, "session_signature": sig,
        "plan_index": int(idx), "utr": str(utr).strip(),
    }, request)

@app.post("/api/account-status")
async def api_status(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/account-status", {"uid": uid, "session_signature": sig},
                     request, rl_limit=40)

@app.post("/api/profile")
async def api_profile(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/profile", {"uid": uid, "session_signature": sig},
                     request, rl_limit=40)

@app.post("/api/tg-start")
async def api_tg_start(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/tg-start", {"uid": uid, "session_signature": sig}, request)

@app.post("/api/tg-status")
async def api_tg_status(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/tg-status", {"uid": uid, "session_signature": sig},
                     request, rl_limit=40)

@app.post("/api/refresh-session")
async def api_refresh(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/refresh-session", {"uid": uid, "session_signature": sig}, request)

@app.post("/api/predict")
async def api_predict(payload: dict, request: Request):
    if not payload.get("uid") or not payload.get("session_signature"):
        raise HTTPException(400, "Missing uid or session_signature.")
    return await fwd("/calculate", payload, request)

# ── FREE PLAN ─────────────────────────────────
@app.post("/api/activate-free-plan")
async def api_activate_free(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    body = {"uid": uid, "session_signature": sig}
    if payload.get("plan_index") is not None:
        body["plan_index"] = payload["plan_index"]
    return await fwd("/activate-free-plan", body, request)

# ── PREDICTION (self-learning engine) — PAID ──
@app.post("/api/prediction")
async def api_prediction(payload: dict, request: Request):
    uid  = payload.get("uid")
    sig  = payload.get("session_signature")
    game = payload.get("game")
    tf   = payload.get("timeframe")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    if not game or not tf:
        raise HTTPException(400, "Missing game or timeframe.")
    body = {
        "uid": uid,
        "session_signature": sig,
        "game": str(game).strip(),
        "timeframe": str(tf).strip(),
    }
    if payload.get("target") is not None:
        body["target"] = str(payload["target"]).strip()
    # game page frequently polls -> thoda higher limit
    return await fwd("/get-prediction", body, request, rl_limit=40)

# ── PREMIUM (.drago) — RAW RESULTS (free + paid) ──
# Browser apni .drago file ke calculation se khud prediction banata hai;
# ye routes sirf raw lottery results (period+number) forward karte hain.
@app.post("/api/results-latest")
async def api_results_latest(payload: dict, request: Request):
    uid  = payload.get("uid")
    sig  = payload.get("session_signature")
    game = payload.get("game")
    tf   = payload.get("timeframe")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    if not game or not tf:
        raise HTTPException(400, "Missing game or timeframe.")
    return await fwd("/results-latest", {
        "uid": uid, "session_signature": sig,
        "game": str(game).strip(), "timeframe": str(tf).strip(),
    }, request, rl_limit=40)

@app.post("/api/results-history")
async def api_results_history(payload: dict, request: Request):
    uid  = payload.get("uid")
    sig  = payload.get("session_signature")
    game = payload.get("game")
    tf   = payload.get("timeframe")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    if not game or not tf:
        raise HTTPException(400, "Missing game or timeframe.")
    body = {
        "uid": uid, "session_signature": sig,
        "game": str(game).strip(), "timeframe": str(tf).strip(),
    }
    if payload.get("limit") is not None:
        try:
            body["limit"] = int(payload["limit"])
        except Exception:
            pass
    return await fwd("/results-history", body, request, rl_limit=40)

# ── GAMES (read) ──────────────────────────────
@app.post("/api/games")
async def api_games(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    body = {"uid": uid, "session_signature": sig}
    if payload.get("category") is not None:
        body["category"] = payload["category"]
    return await fwd("/games", body, request, rl_limit=40)

@app.post("/api/game-detail")
async def api_game_detail(payload: dict, request: Request):
    uid  = payload.get("uid")
    sig  = payload.get("session_signature")
    slug = payload.get("slug")
    if not uid or not sig or not slug:
        raise HTTPException(400, "Missing uid, session_signature or slug.")
    return await fwd("/game-detail", {
        "uid": uid, "session_signature": sig, "slug": str(slug).strip()
    }, request, rl_limit=40)

# ── GAMES (admin manage) ──────────────────────
@app.post("/api/admin/add-game")
async def api_admin_add_game(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    if not uid or not sig:
        raise HTTPException(400, "Missing uid or session_signature.")
    if not payload.get("name") or not payload.get("category"):
        raise HTTPException(400, "Missing name or category.")
    return await fwd("/admin/add-game", {
        "uid":      uid,
        "session_signature": sig,
        "name":     payload.get("name"),
        "image":    payload.get("image", ""),
        "link":     payload.get("link", ""),
        "category": payload.get("category"),
    }, request)

@app.post("/api/admin/update-game")
async def api_admin_update_game(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    gid = payload.get("game_id")
    if not uid or not sig or not gid:
        raise HTTPException(400, "Missing uid, session_signature or game_id.")
    body = {"uid": uid, "session_signature": sig, "game_id": gid}
    for f in ("name", "image", "link", "category"):
        if payload.get(f) is not None:
            body[f] = payload[f]
    return await fwd("/admin/update-game", body, request)

@app.post("/api/admin/delete-game")
async def api_admin_delete_game(payload: dict, request: Request):
    uid = payload.get("uid")
    sig = payload.get("session_signature")
    gid = payload.get("game_id")
    if not uid or not sig or not gid:
        raise HTTPException(400, "Missing uid, session_signature or game_id.")
    return await fwd("/admin/delete-game", {
        "uid": uid, "session_signature": sig, "game_id": gid
    }, request)
