# server.py — DRAGO Prediction Engine (SELF-LEARNING ONLY)
# ──────────────────────────────────────────────────────────────────────────
# Ye file main.py ke SAATH chalti hai (same FastAPI app).
# 3 independent streams:
#     • TRX-1m       (TrxWinGo 1 minute)
#     • Wingo-1m     (WinGo 1 minute)
#     • Wingo-30s    (WinGo 30 second)
#
# Har stream ke 2 servers:
#     • NOVA X  -> Color (GREEN / RED)
#     • PRIME X -> Size  (BIG / SMALL)
#
# Sirf SELF-LEARNING: koi fixed pattern-rule / AI-rule nahi.
# Engine har round ka result yaad rakhta hai, "is haalat me pehle kya aaya"
# dekhta hai, jo zyada baar sahi nikla wahi chunta hai, aur har guess ki
# accuracy track karke weight adjust karta hai. Loss-level se strictness.
#
# Data:  MongoDB (history + learning + latest prediction)
# Output: FastAPI router (prediction endpoints) — neeche `router`.
#
# NOTE (Premium .drago feature):
#   Stream ab raw results (period+number) bhi yaad rakhta hai (self.results).
#   read_results() helper se website ka client-side ".drago" engine yahi raw
#   data le sakta hai aur SAME algorithm browser me chala sakta hai.
# ──────────────────────────────────────────────────────────────────────────

import os
import math
import time
import json
import logging
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, HTTPException

logger = logging.getLogger("drago.engine")

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
# MongoDB connection string — env var se aata hai (hardcode mat karo).
# Git me asli credentials commit mat karo; VPS par MONGO_URI env set karo.
MONGO_URI      = os.getenv("MONGO_URI", "mongodb+srv://krishnavishwas011_db_user:OgktWrNR3KGzo2rj@datacenter.xuicoag.mongodb.net/ai_predictions?retryWrites=true&w=majority&appName=Datacenter")
ENGINE_DB_NAME = os.getenv("ENGINE_DB_NAME", "drago_final")

# Teeno streams ki config. `prefix` MongoDB collection naam ke liye.
STREAMS_CONFIG = {
    "trx_1m": {
        "label":    "TRX-1m",
        "prefix":   "trx1m",
        "api":      "https://draw.ar-lottery01.com/TrxWinGo/TrxWinGo_1M/GetHistoryIssuePage.json",
        "poll_sec": 3,
    },
    "wingo_1m": {
        "label":    "Wingo-1m",
        "prefix":   "wingo1m",
        "api":      "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json",
        "poll_sec": 3,
    },
    "wingo_30s": {
        "label":    "Wingo-30s",
        "prefix":   "wingo30s",
        "api":      "https://draw.ar-lottery01.com/WinGo/WinGo_30S/GetHistoryIssuePage.json",
        "poll_sec": 3,
    },
}

# (game, timeframe) -> stream_id  — endpoints ke liye mapping
ROUTE_MAP = {
    ("trx",   "1m"):  "trx_1m",
    ("wingo", "1m"):  "wingo_1m",
    ("wingo", "30s"): "wingo_30s",
}

# Self-learning ke context lengths (last N results ko "pattern" maanta hai)
# Chhote contexts (1,2) jaldi signal dete hain -> fast learning.
CONTEXT_LENGTHS = [1, 2, 3, 4]
MIN_OBSERVATIONS = 2     # bas 2 baar dekha ho to predict karega (fast)
DECAY = 0.96             # recency decay: purana data dheere-dheere bhulta hai
EWMA_ALPHA = 0.30        # prediction-accuracy fast adapt (galti pe jaldi sudhre)
CONF_FLOOR = 50          # minimum dikhne wala confidence
CONF_CAP = 92            # maximum confidence

# ═══════════════════════════════════════════════════════════════════════════
# MONGODB
# ═══════════════════════════════════════════════════════════════════════════
mongo_db = None
mongo_ok = False
if MONGO_URI and MONGO_URI != "PASTE_YOUR_MONGO_URI_HERE":
    try:
        from pymongo import MongoClient
        _mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        _mc.admin.command("ping")
        mongo_db = _mc[ENGINE_DB_NAME]
        mongo_ok = True
        logger.info("✅ [engine] MongoDB connected")
    except Exception as e:
        logger.error(f"❌ [engine] MongoDB connect failed: {e} — running in-memory only")
else:
    logger.warning("⚠️ [engine] MONGO_URI not set — running in-memory only (no persistence)")


# ═══════════════════════════════════════════════════════════════════════════
# GAME RULES  (WinGo / TrxWinGo standard)
# ═══════════════════════════════════════════════════════════════════════════
def get_size(n: int) -> str:
    return "BIG" if int(n) >= 5 else "SMALL"

def get_color(n: int) -> str:
    n = int(n)
    if n in (1, 3, 7, 9):
        return "GREEN"
    if n in (2, 4, 6, 8):
        return "RED"
    if n == 5:
        return "GREEN"   # GREEN/VIOLET -> GREEN side
    if n == 0:
        return "RED"     # RED/VIOLET   -> RED side
    return "?"

def color_letter(n: int) -> str:
    return "G" if get_color(n) == "GREEN" else "R"

def size_letter(n: int) -> str:
    return "B" if get_size(n) == "BIG" else "S"

def is_win_color(pred: str, actual_n: int) -> bool:
    return str(pred).upper().strip() == get_color(int(actual_n))

def is_win_size(pred: str, actual_n: int) -> bool:
    return str(pred).upper().strip() == get_size(int(actual_n))


# ═══════════════════════════════════════════════════════════════════════════
# LOSS-LEVEL TRACKER  (L1🟢 → L4🔴)
# Jitna loss, utna strict (higher accuracy + confidence chahiye).
# ═══════════════════════════════════════════════════════════════════════════
class LossLevel:
    MAX_LEVEL = 4
    # Ab ye SKIP force nahi karta. Sirf badge + 'switch_bias' deta hai:
    # jitni lagataar loss, utna engine recent galtiyon se ulta jaane ko taiyaar
    # (khud ko fast sudharta hai).
    STRATEGY = {
        1: {"switch_bias": 0.00, "badge": "🟢L1", "desc": "NORMAL"},
        2: {"switch_bias": 0.10, "badge": "🟡L2", "desc": "CAREFUL"},
        3: {"switch_bias": 0.22, "badge": "🟠L3", "desc": "ALERT"},
        4: {"switch_bias": 0.35, "badge": "🔴L4", "desc": "RECOVER"},
    }

    def __init__(self):
        self.level = 1
        self.consec_losses = 0
        self.total_wins = 0
        self.total_losses = 0

    def on_result(self, was_correct: bool):
        if was_correct:
            self.total_wins += 1
            self.consec_losses = 0
            self.level = 1
        else:
            self.total_losses += 1
            self.consec_losses += 1
            self.level = min(self.consec_losses + 1, self.MAX_LEVEL)

    def strategy(self):
        return self.STRATEGY[self.level]

    def badge(self):
        return self.STRATEGY[self.level]["badge"]

    def to_dict(self):
        return {
            "level": self.level,
            "consec_losses": self.consec_losses,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
        }

    def from_dict(self, d):
        self.consec_losses = d.get("consec_losses", 0)
        self.total_wins = d.get("total_wins", 0)
        self.total_losses = d.get("total_losses", 0)
        self.level = 1 if self.consec_losses == 0 else min(self.consec_losses + 1, self.MAX_LEVEL)


# ═══════════════════════════════════════════════════════════════════════════
# SELF-LEARNING SERVER  (ek target ke liye: 'color' ya 'size')
# pattern (last-N letters) -> next_letter -> {w, l, total}
#   total = historical observations (kitni baar dekha)
#   w / l = jab is pattern pe ASLI prediction ki thi, wo sahi/galat hui
# Best prediction = sabse accurate + sabse zyada observed.
# ═══════════════════════════════════════════════════════════════════════════
class SelfLearningServer:
    def __init__(self, stream_id: str, target: str):
        self.stream_id = stream_id
        self.target = target                 # 'color' | 'size'
        self.name = "NOVA_X" if target == "color" else "PRIME_X"
        self.sid = f"{stream_id}_{self.name}"

        # learning memory: pattern_str -> { next_letter -> stats }
        #   total : raw observation count
        #   wt    : recency-weighted observation count (purana fade)
        #   acc   : EWMA prediction-accuracy (0..1) jab is letter ko choose kiya
        #   plays : kitni baar is letter ko actually predict kiya
        self.mem = defaultdict(lambda: defaultdict(
            lambda: {"w": 0, "l": 0, "total": 0, "wt": 0.0, "acc": 0.5, "plays": 0}
        ))

        # recent letters (G/R or B/S)
        self.history = deque(maxlen=400)

        # win/loss tracker for this server
        self.wins = 0
        self.total = 0

        self.loss = LossLevel()

        # pending prediction (jiska result agle round me check hoga)
        self.pending = None        # {pattern_len, pattern, pred_letter, label, conf, level, period}
        self.last_period = None    # last result period jo seen hua

    # ── letter helpers ────────────────────────────────────────────────
    def _letter(self, n: int) -> str:
        return color_letter(n) if self.target == "color" else size_letter(n)

    def _label(self, letter: str) -> str:
        if self.target == "color":
            return "GREEN" if letter == "G" else "RED"
        return "BIG" if letter == "B" else "SMALL"

    def _is_win(self, label: str, n: int) -> bool:
        return is_win_color(label, n) if self.target == "color" else is_win_size(label, n)

    # ── 1) result check: pichli prediction sahi thi ya galat ──────────
    def _check_pending(self, actual_n: int):
        if not self.pending:
            return None
        p = self.pending
        correct = self._is_win(p["label"], actual_n)

        # server win-rate
        self.total += 1
        if correct:
            self.wins += 1

        # learning memory ko result batao (us pattern->pred ke w/l + EWMA)
        entry = self.mem[p["pattern"]][p["pred_letter"]]
        if correct:
            entry["w"] += 1
        else:
            entry["l"] += 1
        entry["plays"] += 1
        # EWMA accuracy: galti hote hi jaldi neeche girti hai -> khud sudharta
        entry["acc"] = (1 - EWMA_ALPHA) * entry["acc"] + EWMA_ALPHA * (1.0 if correct else 0.0)

        # loss-level update (win -> L1, loss -> level badhao)
        self.loss.on_result(correct)

        self.pending = None
        return correct

    # ── 2) learn: har context length pe observation record karo ───────
    #    Recency-weighted: purane observations DECAY se halke ho jaate hain,
    #    naya result poora weight (1.0) paata hai -> engine fast adapt karta.
    def _learn(self, actual_letter: str):
        for L in CONTEXT_LENGTHS:
            if len(self.history) >= L:
                pattern = "".join(list(self.history)[-L:])
                bucket = self.mem[pattern]
                # is pattern ke saare candidates ka weighted count thoda fade
                for st in bucket.values():
                    st["wt"] *= DECAY
                cell = bucket[actual_letter]
                cell["total"] += 1
                cell["wt"] += 1.0

    # ── 3) next prediction banao (sirf self-learning se) ──────────────
    #    HAMESHA ek prediction deta hai (skip nahi) jab tak thodi bhi
    #    history ho. Longer+weighted+accurate pattern ko prefer karta hai,
    #    aur loss-level switch_bias se recent galtiyon se ulta jhukta hai.
    def _predict_next(self):
        strat = self.loss.strategy()
        switch_bias = strat["switch_bias"]
        opp = self._opposite_letter()

        best = None
        best_score = -1.0

        for L in CONTEXT_LENGTHS:
            if len(self.history) < L:
                continue
            pattern = "".join(list(self.history)[-L:])
            counts = self.mem.get(pattern)
            if not counts:
                continue

            wt_all = sum(s["wt"] for s in counts.values()) or 1.0

            for letter, s in counts.items():
                if s["total"] < MIN_OBSERVATIONS:
                    continue

                # weighted frequency (recency) = "is haalat me ye kitna aata hai"
                freq = s["wt"] / wt_all
                # EWMA prediction-accuracy (galti pe jaldi girti)
                acc_ewma = s["acc"]
                plays = s["plays"]

                # prob = frequency + accuracy ka blend (plays badhne pe acc ko zyada weight)
                w_acc = min(plays, 8) / 8.0
                prob = (1 - w_acc) * freq + w_acc * (0.5 * freq + 0.5 * acc_ewma)

                # loss-recovery: agar ye letter recent loser jaisa hai to bias se gira do
                if letter != opp:
                    prob -= switch_bias * 0.5
                else:
                    prob += switch_bias * 0.5
                prob = max(0.01, min(prob, 0.99))

                # longer + zyada-weighted pattern ko thoda prefer
                score = prob * math.log(s["wt"] + 1.5) * (1 + L * 0.06)
                if score > best_score:
                    best_score = score
                    conf = int(max(CONF_FLOOR, min(prob * 100, CONF_CAP)))
                    best = {
                        "pattern_len": L,
                        "pattern": pattern,
                        "pred_letter": letter,
                        "label": self._label(letter),
                        "conf": conf,
                        "acc": round(prob * 100, 1),
                    }

        # FALLBACK: koi pattern match nahi -> recent trend / majority se predict
        if best is None and len(self.history) >= 1:
            best = self._fallback_predict(switch_bias, opp)
        return best

    # recent losses ke against jhukne ke liye "opposite of last seen" letter
    def _opposite_letter(self) -> str:
        if not self.history:
            return "G" if self.target == "color" else "B"
        last = self.history[-1]
        if self.target == "color":
            return "R" if last == "G" else "G"
        return "S" if last == "B" else "B"

    # bina pattern ke bhi kuch na kuch predict karo (kabhi WAIT nahi)
    def _fallback_predict(self, switch_bias: float, opp: str):
        recent = list(self.history)[-20:]
        a = "G" if self.target == "color" else "B"
        b = "R" if self.target == "color" else "S"
        ca = recent.count(a)
        cb = recent.count(b)
        # majority side; loss pe switch_bias se balance shift
        score_a = ca + (switch_bias * 10 if opp == a else 0)
        score_b = cb + (switch_bias * 10 if opp == b else 0)
        letter = a if score_a >= score_b else b
        tot = (ca + cb) or 1
        prob = max(ca, cb) / tot
        conf = int(max(CONF_FLOOR, min(prob * 100, CONF_CAP)))
        return {
            "pattern_len": 0,
            "pattern": "*",
            "pred_letter": letter,
            "label": self._label(letter),
            "conf": conf,
            "acc": round(prob * 100, 1),
        }

    # ── MAIN: ek naya result aaya (period, number) ────────────────────
    def on_new_result(self, period: str, number: int, learn_only: bool = False):
        """
        learn_only=True -> sirf history/memory build karo (bootstrap),
        koi pending-check ya new prediction nahi.
        """
        actual_letter = self._letter(number)

        if not learn_only:
            self._check_pending(number)      # 1) result check + level update

        self._learn(actual_letter)           # 2) learn (current history -> actual)
        self.history.append(actual_letter)   # history update
        self.last_period = period

        if learn_only:
            return None

        # 3) agle period ki prediction
        nxt = self._predict_next()
        next_period = self._next_period(period)
        if nxt:
            self.pending = {
                **nxt,
                "level": self.loss.level,
                "period": next_period,
            }
        else:
            self.pending = None

        self._save_latest(next_period)
        self._save_state()
        return self.pending

    @staticmethod
    def _next_period(period: str) -> str:
        try:
            return str(int(period) + 1)
        except Exception:
            return period

    # ── current public prediction (latest doc shape) ──────────────────
    def latest_payload(self, next_period: str):
        strat = self.loss.strategy()
        wr = round(self.wins / self.total * 100, 1) if self.total else 0.0
        if self.pending:
            return {
                "stream": self.stream_id,
                "server": self.name,
                "target": self.target,
                "period": self.pending["period"],
                "prediction": self.pending["label"],
                "confidence": self.pending["conf"],
                "accuracy": self.pending.get("acc", 0),
                "level": self.loss.level,
                "badge": self.loss.badge(),
                "level_desc": strat["desc"],
                "win_rate": wr,
                "wins": self.wins,
                "total": self.total,
                "status": "READY",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        # koi confident prediction nahi -> WAIT (skip)
        return {
            "stream": self.stream_id,
            "server": self.name,
            "target": self.target,
            "period": next_period,
            "prediction": "WAIT",
            "confidence": 0,
            "accuracy": 0,
            "level": self.loss.level,
            "badge": self.loss.badge(),
            "level_desc": strat["desc"],
            "win_rate": wr,
            "wins": self.wins,
            "total": self.total,
            "status": "SKIP",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── MongoDB: latest prediction (yahin website padhegi) ────────────
    def _save_latest(self, next_period: str):
        if not mongo_ok:
            return
        prefix = STREAMS_CONFIG[self.stream_id]["prefix"]
        doc = self.latest_payload(next_period)
        doc["_id"] = f"{prefix}_{self.target}_latest"
        try:
            mongo_db["latest_predictions"].replace_one({"_id": doc["_id"]}, doc, upsert=True)
            # history bhi rakho (audit)
            mongo_db["prediction_history"].insert_one({
                **{k: v for k, v in doc.items() if k != "_id"},
                "saved_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.error(f"[engine] save_latest {self.sid}: {e}")

    # ── MongoDB: learning state persist / restore ─────────────────────
    def _save_state(self):
        if not mongo_ok:
            return
        try:
            mongo_db["engine_state"].replace_one(
                {"_id": self.sid},
                {
                    "_id": self.sid,
                    "stream_id": self.stream_id,
                    "target": self.target,
                    "wins": self.wins,
                    "total": self.total,
                    "loss": self.loss.to_dict(),
                    "last_period": self.last_period,
                    # mem ko compact list me convert (sirf w/l/total>0)
                    "mem": [
                        {"p": pat, "n": nl, "w": s["w"], "l": s["l"], "t": s["total"],
                         "wt": round(s["wt"], 4), "acc": round(s["acc"], 4), "pl": s["plays"]}
                        for pat, vals in self.mem.items()
                        for nl, s in vals.items()
                        if s["total"] > 0
                    ],
                    "history": list(self.history),
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                upsert=True,
            )
        except Exception as e:
            logger.error(f"[engine] save_state {self.sid}: {e}")

    def load_state(self):
        if not mongo_ok:
            return
        try:
            doc = mongo_db["engine_state"].find_one({"_id": self.sid})
            if not doc:
                return
            self.wins = doc.get("wins", 0)
            self.total = doc.get("total", 0)
            self.loss.from_dict(doc.get("loss", {}))
            self.last_period = doc.get("last_period")
            for h in doc.get("history", []):
                self.history.append(h)
            for row in doc.get("mem", []):
                self.mem[row["p"]][row["n"]] = {
                    "w": row.get("w", 0),
                    "l": row.get("l", 0),
                    "total": row.get("t", 0),
                    "wt": row.get("wt", float(row.get("t", 0))),
                    "acc": row.get("acc", 0.5),
                    "plays": row.get("pl", 0),
                }
            logger.info(f"[engine] state restored: {self.sid} (hist={len(self.history)})")
        except Exception as e:
            logger.error(f"[engine] load_state {self.sid}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# STREAM  (ek game+timeframe — NOVA X + PRIME X dono yahin)
# ═══════════════════════════════════════════════════════════════════════════
class Stream:
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.cfg = STREAMS_CONFIG[stream_id]
        self.api = self.cfg["api"]
        self.poll_sec = self.cfg["poll_sec"]
        self.color = SelfLearningServer(stream_id, "color")   # NOVA X
        self.size = SelfLearningServer(stream_id, "size")     # PRIME X
        self.last_period = None
        self._bootstrapped = False
        # Premium .drago feature: raw results (period+number) yaad rakho taaki
        # website ka client-side engine inhe le kar SAME algorithm chala sake.
        self.results = deque(maxlen=300)

    def _record_result(self, period: str, number: int):
        """Raw result store karo (duplicate period skip)."""
        if self.results and self.results[-1]["period"] == period:
            return
        self.results.append({"period": period, "number": int(number)})

    def _fetch(self):
        try:
            r = requests.get(self.api, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                return []
            d = r.json()
            lst = (d.get("data") or {}).get("list") or []
            out = []
            for rec in lst:
                period = str(rec.get("issueNumber", "")).strip()
                num = rec.get("number")
                if period and num is not None:
                    try:
                        out.append({"period": period, "number": int(num)})
                    except Exception:
                        continue
            # API newest-first deta hai -> chronological (oldest-first) karo
            out.sort(key=lambda x: x["period"])
            return out
        except Exception as e:
            logger.warning(f"[engine] fetch {self.stream_id}: {e}")
            return []

    def _bootstrap(self, records):
        # restore saved learning state, fir purane results learn-only feed karo
        self.color.load_state()
        self.size.load_state()
        seen = self.color.last_period
        for rec in records:
            # raw result hamesha store karo (recent history website ke liye)
            self._record_result(rec["period"], rec["number"])
            if seen and rec["period"] <= seen:
                continue
            self.color.on_new_result(rec["period"], rec["number"], learn_only=True)
            self.size.on_new_result(rec["period"], rec["number"], learn_only=True)
            self.last_period = rec["period"]
        self._bootstrapped = True
        logger.info(f"[engine] {self.cfg['label']} bootstrapped @ period {self.last_period}")

    def tick(self):
        records = self._fetch()
        if not records:
            return
        if not self._bootstrapped:
            self._bootstrap(records)
            return
        # naye results process karo (chronological)
        for rec in records:
            if self.last_period and rec["period"] <= self.last_period:
                continue
            self._record_result(rec["period"], rec["number"])
            self.color.on_new_result(rec["period"], rec["number"])
            self.size.on_new_result(rec["period"], rec["number"])
            self.last_period = rec["period"]
            logger.info(
                f"[engine] {self.cfg['label']} result {rec['period']} -> {rec['number']} "
                f"| NOVA {self.color.loss.badge()} | PRIME {self.size.loss.badge()}"
            )

    def run_loop(self):
        logger.info(f"🚀 [engine] stream started: {self.cfg['label']}")
        while True:
            try:
                self.tick()
            except Exception as e:
                logger.error(f"[engine] {self.stream_id} loop error: {e}")
            time.sleep(self.poll_sec)


# ═══════════════════════════════════════════════════════════════════════════
# ENGINE BOOT  (main.py ke lifespan se call hota hai)
# ═══════════════════════════════════════════════════════════════════════════
STREAMS: dict[str, Stream] = {}
_started = False
_start_lock = threading.Lock()

def start_prediction_engine():
    """Teeno streams ko background threads me chalu karta hai (idempotent)."""
    global _started
    with _start_lock:
        if _started:
            logger.info("[engine] already started — skipping")
            return
        for sid in STREAMS_CONFIG:
            STREAMS[sid] = Stream(sid)
            threading.Thread(
                target=STREAMS[sid].run_loop,
                daemon=True,
                name=f"engine-{sid}",
            ).start()
        _started = True
        logger.info("✅ [engine] all 3 prediction streams started")


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC HELPERS  (main.py inhe auth ke baad call karta hai)
# ═══════════════════════════════════════════════════════════════════════════
def resolve_stream(game: str, tf: str) -> str | None:
    """('trx','1m') / ('wingo','30s') -> stream_id, warna None."""
    return ROUTE_MAP.get((str(game).lower().strip(), str(tf).lower().strip()))


def _read_latest(stream_id: str, target: str):
    """Pehle live in-memory se, warna MongoDB se latest prediction."""
    st = STREAMS.get(stream_id)
    if st:
        srv = st.color if target == "color" else st.size
        nxt = srv._next_period(srv.last_period) if srv.last_period else ""
        return srv.latest_payload(nxt)
    # fallback: mongo (agar thread abhi boot ho rahi ho)
    if mongo_ok:
        prefix = STREAMS_CONFIG[stream_id]["prefix"]
        doc = mongo_db["latest_predictions"].find_one({"_id": f"{prefix}_{target}_latest"})
        if doc:
            doc.pop("_id", None)
            return doc
    raise HTTPException(503, "Prediction engine not ready yet.")


def read_prediction(game: str, tf: str, target: str) -> dict:
    """Ek target (color/size) ki latest prediction. (validations included)"""
    target = str(target).lower().strip()
    if target not in ("color", "size"):
        raise HTTPException(400, "target must be 'color' or 'size'")
    stream_id = resolve_stream(game, tf)
    if not stream_id:
        raise HTTPException(404, "Unknown game/timeframe. Use trx/1m, wingo/1m, wingo/30s.")
    return _read_latest(stream_id, target)


def read_both(game: str, tf: str) -> dict:
    """Color + Size dono ek saath."""
    stream_id = resolve_stream(game, tf)
    if not stream_id:
        raise HTTPException(404, "Unknown game/timeframe. Use trx/1m, wingo/1m, wingo/30s.")
    return {
        "stream": stream_id,
        "color": _read_latest(stream_id, "color"),
        "size": _read_latest(stream_id, "size"),
    }


# ═══════════════════════════════════════════════════════════════════════════
# PREMIUM (.drago) DATA HELPERS
# Browser ka client-side ".drago" engine yahi RAW results le kar SAME algorithm
# device par chalata hai. Yahan koi prediction/calculation nahi — sirf raw data.
# ═════��═════════════════════════════════════════════════════════════════════
def read_results(game: str, tf: str, limit: int = 200) -> dict:
    """Kisi stream ke recent raw results (period+number) chronological."""
    stream_id = resolve_stream(game, tf)
    if not stream_id:
        raise HTTPException(404, "Unknown game/timeframe. Use trx/1m, wingo/1m, wingo/30s.")
    st = STREAMS.get(stream_id)
    if not st or not st.results:
        raise HTTPException(503, "Result engine not ready yet.")
    try:
        limit = max(1, min(int(limit), 300))
    except Exception:
        limit = 200
    items = list(st.results)[-limit:]
    return {
        "stream": stream_id,
        "count": len(items),
        "results": items,
        "last_period": st.last_period,
    }


def read_latest_result(game: str, tf: str) -> dict:
    """Sabse naya raw result + agle period ka number."""
    stream_id = resolve_stream(game, tf)
    if not stream_id:
        raise HTTPException(404, "Unknown game/timeframe. Use trx/1m, wingo/1m, wingo/30s.")
    st = STREAMS.get(stream_id)
    if not st or not st.results:
        raise HTTPException(503, "Result engine not ready yet.")
    latest = st.results[-1]
    try:
        next_period = str(int(latest["period"]) + 1)
    except Exception:
        next_period = latest["period"]
    return {
        "stream": stream_id,
        "period": latest["period"],
        "number": latest["number"],
        "next_period": next_period,
    }


def engine_status_payload() -> dict:
    """Saare streams ka health/status."""
    out = {"mongo": mongo_ok, "streams": {}}
    for sid, st in STREAMS.items():
        out["streams"][sid] = {
            "label": st.cfg["label"],
            "last_period": st.last_period,
            "bootstrapped": st._bootstrapped,
            "results": len(st.results),
            "nova": {"badge": st.color.loss.badge(), "win_rate": round(st.color.wins / st.color.total * 100, 1) if st.color.total else 0},
            "prime": {"badge": st.size.loss.badge(), "win_rate": round(st.size.wins / st.size.total * 100, 1) if st.size.total else 0},
        }
    return out


# ═══════════════════════════════════════════════════════════════════════════
# OPTIONAL DIRECT ROUTER  (sirf VPS-local debugging ke liye; production me
# website BRIDGE -> main.py /get-prediction se data leti hai, ye nahi)
# ═══════════════════════════════════════════════════════════════════════════
router = APIRouter(prefix="/api/prediction", tags=["prediction"])

@router.get("/{game}/{tf}/{target}")
def _dbg_one(game: str, tf: str, target: str):
    return read_prediction(game, tf, target)

@router.get("/{game}/{tf}")
def _dbg_both(game: str, tf: str):
    return read_both(game, tf)

@router.get("")
def _dbg_status():
    return engine_status_payload()
