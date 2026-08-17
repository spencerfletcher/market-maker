"""
bot/core/config.py
──────────────────
All configuration is read from environment variables (optionally via a `.env` file).

⚠️ Every default in this file is ILLUSTRATIVE. It is the value that makes the module importable
and the tests runnable — not the value any deployment runs. Read a running process's real
configuration with `python -m scripts.show_config`, never off these literals.

Two conventions worth knowing before editing:

  * `DRY_RUN` defaults to **True**. Arming is an explicit, deliberate act (and the maker shims
    additionally require `--i-understand-real-money`), so the failure mode of a forgotten
    environment variable is "places no orders", never "places orders".
  * Operational file paths default to ABSOLUTE paths under the repo root, via `repo_path`. A
    relative operational default is a real defect class: a process launched from another working
    directory silently reads a file that does not exist, and a loss cap that reads an empty ledger
    is a loss cap that never fires. An env override may be relative — that is the operator's
    choice — but the default must not be able to go wrong.
"""
import os
from dotenv import load_dotenv

# `repo_path` only. bot.core.durable imports nothing from bot, so this cannot cycle — and it must
# not, because the operational rails have to be importable while config itself is loading.
from bot.core.durable import repo_path

load_dotenv()


def _optional(key: str, default: str = "") -> str:
    """Return env var value or a default (no error if missing)."""
    return os.getenv(key, default)


def _bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).lower() in ("1", "true", "yes")


def _int(key: str, default: int = 0) -> int:
    return int(os.getenv(key, str(default)))


def _float(key: str, default: float = 0.0) -> float:
    return float(os.getenv(key, str(default)))


# ── Global posture ────────────────────────────────────────────────────────────
# The master safety flag. True ⇒ no order is ever sent to a venue.
DRY_RUN: bool = _bool("DRY_RUN", True)
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Path to a file the operator can create to pause quoting without killing the process.
# `touch pause.json` pauses; `rm pause.json` resumes. ⚠️ This default is RELATIVE, so it resolves
# against the launching process's cwd — `show_config` prints the raw value plus a cwd caveat for
# exactly that reason. A control that reads as armed but names the wrong file is worse than none.
KILL_SWITCH_FILE: str = _optional("KILL_SWITCH_FILE", "pause.json")

# Maximum net realized loss (USD) allowed per UTC day before the process stops opening new
# exposure. 0 = disabled. The maker additionally carries its own durable, cross-process loss-cap
# ratchet — see bot/core/maker_state.py.
DAILY_LOSS_LIMIT: float = _float("DAILY_LOSS_LIMIT", 0.0)
# Hard kill on CUMULATIVE realized cost (from the realized-P&L ledger): monotonic, no auto-resume.
# 0 = off. See bot/core/safety.py.
KALSHI_EXEC_COST_BUDGET: float = _float("KALSHI_EXEC_COST_BUDGET", 0.0)


# ── Polymarket US ─────────────────────────────────────────────────────────────
POLYMARKET_US_KEY_ID: str = _optional("POLYMARKET_US_KEY_ID")
POLYMARKET_US_SECRET_KEY: str = _optional("POLYMARKET_US_SECRET_KEY")
# Transport for the Poly US live price feed: "raw" (our own JSON websocket: explicit ping/pong +
# heartbeat-stop reconnect; default) or "sdk" (vendor SDK WebSocket — its `is_connected` can lie on
# a zombie socket, which once froze prices for minutes while reporting healthy). Raw requires
# pynacl.
POLY_US_FEED_SOURCE: str = _optional("POLY_US_FEED_SOURCE", "raw").lower()
# The maker's per-cycle book READ source. "rest" (default) = a cache-busted book fetch per book per
# cycle. "ws" = read the maintained WS order-book cache, freshness-gated, with a single-book REST
# backstop — this removes the per-book request cost that otherwise bounds how many books one
# process can quote. ⛔ "ws" requires the shim to construct and run the feed; with no feed running
# it degrades to REST rather than reading a stale cache.
MAKER_BOOK_SOURCE: str = _optional("MAKER_BOOK_SOURCE", "rest").lower()


# ── Kalshi ────────────────────────────────────────────────────────────────────
KALSHI_API_KEY: str = _optional("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY_PATH: str = _optional("KALSHI_PRIVATE_KEY_PATH", "")
# ⚠️ DEFAULT IS "demo" — a deployment MUST set KALSHI_ENV=prod explicitly. Same principle as
# DRY_RUN: forgetting the variable must land on the harmless side.
KALSHI_ENV: str = _optional("KALSHI_ENV", "demo")   # "demo" or "prod"
# Illustrative series list. `KalshiOrderBookCache` uses it as a HARD prefix filter on inbound WS
# messages, so a market outside this set is invisible to the feed no matter what you point the
# maker at. Set it to the series you actually want to quote.
KALSHI_SERIES: list[str] = [
    s.strip()
    for s in _optional("KALSHI_SERIES", "KXNBAGAME,KXMLBGAME").split(",")
    if s.strip()
]
# Source for Kalshi prices: "ticker" (top-of-book quote channel — can lag the real book during fast
# moves, producing prices that are not executable) or "orderbook" (the real book, maintained from
# orderbook_delta messages).
KALSHI_PRICE_SOURCE: str = _optional("KALSHI_PRICE_SOURCE", "ticker").lower()
# Min seconds between divergence-triggered resnapshots (each re-subscribes ALL book tickers).
# Without this, one chronically-divergent ticker triggers a resnapshot every audit → a storm that
# floods the WS and causes more sequence gaps. The divergent ticker stays suspect (untradeable)
# regardless; the throttle only stops the re-snapshotting.
KALSHI_RESNAP_THROTTLE_SECONDS: float = _float("KALSHI_RESNAP_THROTTLE_SECONDS", 30.0)
# Book-trust gate: after a sequence gap / REST divergence / resnapshot, treat the affected
# ticker(s) as untrustworthy for this many seconds. On a gappy feed this approaches a halt — which
# is the intended direction: quoting into a book you cannot reconstruct is the expensive failure.
KALSHI_BOOK_SUSPECT_SECONDS: float = _float("KALSHI_BOOK_SUSPECT_SECONDS", 5.0)


# ── WS receive-loop timing diagnostics (default OFF — zero cost when off) ─────
# When True, instruments the WS receive loops with per-message inline handler time
# (parse+cache+callback-scheduling), inter-arrival gap and rolling message rate, plus an
# independent event-loop-lag probe. Sampled 1-in-N and buffered to a SEPARATE diagnostic CSV.
# Pure observability: cache and decision logic are byte-for-byte identical on or off.
# See bot/core/ws_timing.py.
WS_LOOP_TIMING: bool = _bool("WS_LOOP_TIMING", False)
WS_LOOP_TIMING_SAMPLE_N: int = _int("WS_LOOP_TIMING_SAMPLE_N", 1)   # buffer 1 row per N msgs
WS_LOOP_TIMING_FLUSH_N: int = _int("WS_LOOP_TIMING_FLUSH_N", 200)   # flush buffer every N rows


# ── Notifications ─────────────────────────────────────────────────────────────
# DISCORD_WEBHOOK_URL is the alerts channel AND the fallback: an unset specialised channel
# degrades to it, never to a dropped message.
DISCORD_WEBHOOK_URL: str = _optional("DISCORD_WEBHOOK_URL")
DISCORD_FILLS_WEBHOOK_URL: str = _optional("DISCORD_FILLS_WEBHOOK_URL")
DISCORD_REPORTS_WEBHOOK_URL: str = _optional("DISCORD_REPORTS_WEBHOOK_URL")
# Whether to send notifications even when DRY_RUN is true.
DISCORD_NOTIFY_DRY_RUN: bool = _bool("DISCORD_NOTIFY_DRY_RUN", False)
# Pushover push notifications (phone alerts).
PUSHOVER_USER_KEY: str = _optional("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN: str = _optional("PUSHOVER_API_TOKEN")


# ── Operational rails (see bot/core/{alert_health,heartbeat,maker_state,memguard}.py) ──
# ⛔ EVERY DEFAULT BELOW IS AN ABSOLUTE PATH under the repo root, via `repo_path` — see the module
# docstring for why.

# Where alert-delivery outcomes are recorded, so a dead webhook is discoverable without asking the
# webhook. A send failure is never reported through the channel that just failed.
ALERT_HEALTH_FILE: str = _optional("ALERT_HEALTH_FILE") or repo_path("logs", "alert_health.json")

# One heartbeat file per long-running process.
HEARTBEAT_DIR: str = _optional("HEARTBEAT_DIR") or repo_path("logs", "heartbeat")

# Crash-durable maker state: resting orders, inventory, and loss-to-date, written as they change
# so they survive SIGKILL.
MAKER_STATE_FILE: str = _optional("MAKER_STATE_FILE") or repo_path("logs", "maker_state.json")

# Memory headroom, in MB. 0 disables an individual check. Sized for a small-memory host: warn well
# above a healthy process's normal peak, halt well above the kill zone. The availability HALT is a
# disjunction — MIN_AVAIL floors COMBINED MemAvailable+SwapFree (distance to the OOM killer; a
# RAM-only floor false-halts a healthy process on a host with plenty of swap free), while
# HARD_RAM_FLOOR floors RAM availability alone, because a teardown must not run entirely out of a
# swapfile. WARN_AVAIL is the non-halting paging warning.
MEMGUARD_ENABLED: bool = _bool("MEMGUARD_ENABLED", True)
MEMGUARD_WARN_RSS_MB: float = _float("MEMGUARD_WARN_RSS_MB", 400.0)
MEMGUARD_HALT_RSS_MB: float = _float("MEMGUARD_HALT_RSS_MB", 700.0)
MEMGUARD_MIN_AVAIL_MB: float = _float("MEMGUARD_MIN_AVAIL_MB", 256.0)
MEMGUARD_WARN_AVAIL_MB: float = _float("MEMGUARD_WARN_AVAIL_MB", 120.0)
MEMGUARD_HARD_RAM_FLOOR_MB: float = _float("MEMGUARD_HARD_RAM_FLOOR_MB", 80.0)
# Where /tmp is a RAM-backed tmpfs, files written there consume the same memory the process needs.
# Warn only.
MEMGUARD_TMPFS_WARN_MB: float = _float("MEMGUARD_TMPFS_WARN_MB", 400.0)
