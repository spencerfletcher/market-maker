"""
bot/config.py
─────────────
Loads all configuration from environment variables (via .env file).
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


# ── Polymarket venue selection ──────────────────────────────────────────────
# "us" (polymarket.us, polymarket-us SDK, API-key auth, CFTC-regulated) is the ONLY supported
# venue — the legacy global CLOB stack (wallet auth, py-clob-client) was removed 2026-07-17
# (archived on the poly-global branch). BotRunner raises on any other POLY_VENUE value.
POLY_VENUE: str = _optional("POLY_VENUE", "us").lower()
POLYMARKET_US_KEY_ID: str = _optional("POLYMARKET_US_KEY_ID")
POLYMARKET_US_SECRET_KEY: str = _optional("POLYMARKET_US_SECRET_KEY")

#: Logged verbatim by both `poly_us.feed` transports when the price WS is disabled for want of
#: credentials — one string so the two paths cannot drift apart.
POLY_CREDS_MISSING_LOG = ("poly_us_feed: no API keys — live price WebSocket disabled. "
                          "Set POLYMARKET_US_KEY_ID/SECRET_KEY to enable live prices.")


def poly_creds_present() -> bool:
    """Both Poly US API credentials are set. Read at call time, so a test that patches the
    module attributes is seen."""
    return bool(POLYMARKET_US_KEY_ID and POLYMARKET_US_SECRET_KEY)


# Poly US sport series IDs to scan (from sports.list(): nba=4, nhl=6, mlb=15, World Cup=69,
# wnba=49, mls=10, ucl=12, epl=11, ligamx=111, npb=82, kbo=81, peruLiga1=119). Chosen to overlap
# with KALSHI_SERIES. Override with a comma-separated env var. (ucl/epl list only futures until
# their seasons open — UCL ~Sep, EPL ~Aug. ligamx/npb/kbo/peru are in-season with live per-game
# markets.)
POLYMARKET_US_SERIES: list[str] = [
    s.strip()
    for s in _optional("POLYMARKET_US_SERIES", "69,15,4,6,49,10,12,11,111,82,81,119").split(",")
    if s.strip()
]
# Transport for the Poly US live price feed: "raw" (our own JSON websocket: explicit
# ping/pong + heartbeat-stop reconnect; default) or "sdk" (polymarket-us SDK WebSocket;
# its is_connected can lie on a zombie socket — once froze prices 11 min and produced
# phantom edges). Raw validated live 2026-06-19 (sustained 400–800 book changes/min vs
# the SDK's repeated 45s freezes); set "sdk" in .env to fall back. Raw signs with `cryptography`.
POLY_US_FEED_SOURCE: str = _optional("POLY_US_FEED_SOURCE", "raw").lower()
# The LOCAL address every Poly US REST request and WebSocket connection binds to. BLANK (default)
# = the default route, i.e. the box's primary IP. The venue's limits are per API KEY (authenticated,
# 20/s) and per SOURCE IP (public, 20/s) plus Cloudflare's own per-IP edge, so moving the collectors
# onto a secondary IP gives them their own public budget and their own ban surface — see
# the private design notes § Second IP for collectors, and `bot/core/venue_budget.py` (the shared bucket is
# per source address, keyed on this value).
POLY_SOURCE_IP: str = _optional("POLY_SOURCE_IP").strip()
# WS-maker book feed [2026-08-12]: the POLY maker's per-cycle book READ source.
# "rest" (default) = cache-busted _fetch_book per book per cycle (today). "ws" = read the WS
# order-book cache (freshness-gated, single-book REST backstop) — removes the per-book req/s
# cost. ⛔ "ws" requires the shim to construct + run the feed; with no feed it degrades to REST.
# Real-money ws starts are not marker-gated (the N1 shadow marker gate was removed 2026-09-06).
MAKER_BOOK_SOURCE: str = _optional("MAKER_BOOK_SOURCE", "rest").lower()


# ── Bot behaviour ─────────────────────────────────────────────────────────────
DRY_RUN: bool = _bool("DRY_RUN", True)
MAX_POSITION_USD: float = _float("MAX_POSITION_USD", 500.0)
# CROSS_ARB_SIZE_TIERS + _edge_size_multiplier DELETED 2026-07-15. The breakpoints
# (0.005/0.01) were sportsbook-era and sat BELOW the live fire floor KALSHI_ARB_MIN_EDGE=0.02,
# so the multiplier was a constant 1.0 on every live path — while CLAUDE.md, README and
# position_management.md all credited it for the 5-10 share ramp. The ramp is MAX_POSITION_USD
# alone; raising it 5->500 is an unmodulated 100x step with nothing to damp it.
# Don't re-add edge-scaled sizing without re-tuning ABOVE the floor, and never size UP on fat
# edges: cross-venue a fat edge means the venues DISAGREE (one is stale) = peak strand risk.
# See the private design notes S1.
# Max total USD deployed across ALL trades in a single scan cycle
MAX_TOTAL_EXPOSURE: float = _float("MAX_TOTAL_EXPOSURE", 2000.0)
# Cooldown: don't re-enter the same event within this many seconds
EVENT_COOLDOWN_SECONDS: int = _int("EVENT_COOLDOWN_SECONDS", 300)
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Kalshi (second prediction market — fully automated cross-platform arb) ────
KALSHI_API_KEY: str = _optional("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY_PATH: str = _optional("KALSHI_PRIVATE_KEY_PATH", "")
# ⚠️ DEFAULT IS "demo" — production MUST set KALSHI_ENV=prod in .env (else it trades the demo venue).
KALSHI_ENV: str = _optional("KALSHI_ENV", "demo")   # "demo" or "prod"
KALSHI_SERIES: list[str] = [
    s.strip()
    for s in _optional("KALSHI_SERIES", "KXNBAGAME,KXNHLGAME,KXMLBGAME,KXNPBGAME,KXKBOGAME,KXWCGAME,KXMLSGAME,KXLIGAMXGAME,KXUCLGAME,KXEPLGAME,KXWNBAGAME,KXPERLIGA1GAME").split(",")
    if s.strip()
]
# Min edge to fire a cross-venue arb. This is the REAL divergence-tail gate: a bad
# settlement divergence loses ~the full stake regardless of price, so break-even
# divergence rate ≈ the edge itself. 0.02 → only take edges that survive a ~2%
# ambiguous-settlement rate (~2x margin over the ~0.9% rate research flagged), and it
# still captures real edges like the Cleveland 2.2%. Server .env should match (0.02).
KALSHI_ARB_MIN_EDGE: float = _float("KALSHI_ARB_MIN_EDGE", 0.02)
# Implausible-edge (cross-venue PRICE-divergence) sanity ceiling — reject any edge above this as a
# phantom (one venue's price is stale/wrong; a real arb is a small, cents-scale edge). Empirical-
# WITH-HEADROOM, NOT a proven bound: largest real edge observed ~0.33 on thin data, freeze-recovery
# phantoms cluster 0.68-0.75; 0.50 sits clear of both. Tune DOWN as phantom data accrues. (Distinct
# from KALSHI_ARB_MIN_EDGE, the lower bound, and from the settlement-divergence void tail.)
KALSHI_MAX_PLAUSIBLE_EDGE: float = _float("KALSHI_MAX_PLAUSIBLE_EDGE", 0.50)

# ⚠️ DEAD 2026-07-15 — the ladder EXECUTION path (bot/runner/ladder.py) was deleted. A
# single-venue BTC arb in a sports cross-arb bot: it never found an arb, was wired live
# with no on/off flag, had ZERO test coverage of its executor, and carried two silent
# naked-position bugs (unwind response discarded; a partial fill classified as "neither
# leg filled" -> position held, unrecorded, invisible to the exposure cap and
# has_stranded). Subscribing these series also neutered the frozen-book watchdog.
# The KALSHI_LADDER_* knobs are removed; a stale one in .env is simply ignored.
# bot/kalshi/ladder.py survives for classify_market, which macro_pairs uses.
KALSHI_MACRO_SERIES: list[str] = [
    s.strip()
    for s in _optional("KALSHI_MACRO_SERIES", "KXFED,KXCPI").split(",")
    if s.strip()
]
KALSHI_MACRO_MIN_EDGE: float = _float("KALSHI_MACRO_MIN_EDGE", 0.01)
# Minimum-profit floor on the KALSHI limit — the slice of the edge we refuse to give up.
# ⚠️ RAISING THIS TIGHTENS, IT DOES NOT LOOSEN. Kalshi's limit is breakeven-MINUS-buf, so a bigger
# buf demands more profit and fills LESS often. It buys no room to fill through a fast move.
# Not a Poly knob and not "2 × this" of joint tolerance: the Poly leg bids the raw ask flat since
# S4 (2026-07-16, its cushion rescued 0/14 measured misses), and the 2×buf model was wrong before
# that (corrected 2026-07-15). Kalshi is now the ONLY reader. See _fok_buffer's docstring.
KALSHI_FOK_BUFFER: float = _float("KALSHI_FOK_BUFFER", 0.005)
# Edge-proportional allowance: buf = max(KALSHI_FOK_BUFFER, edge * fraction / 2), capped at
# 0.45*edge. ⚠️ It does NOT make fat-edge spikes "fill through fast moves" on the Kalshi leg — a
# bigger buf TIGHTENS that limit (see _fok_buffer). 0 = fixed floor only.
KALSHI_FOK_EDGE_FRACTION: float = _float("KALSHI_FOK_EDGE_FRACTION", 0.35)
# Source for Kalshi prices: "ticker" (top-of-book quote channel, can lag the real
# book during fast moves → phantom edges) or "orderbook" (real executable book via
# orderbook_delta). Code default "ticker"; DEPLOYED = "orderbook" since 2026-07-18 (crossed-book float-dust fixed).
KALSHI_PRICE_SOURCE: str = _optional("KALSHI_PRICE_SOURCE", "ticker").lower()
# Orderbook-mode integrity check: if the WS-maintained best yes-bid drifts from
# the REST book by more than this (dollars), the local book has a stale level →
# force a resnapshot. The REST poll does the comparison off the hot path.
KALSHI_BOOK_MAX_DIVERGENCE: float = _float("KALSHI_BOOK_MAX_DIVERGENCE", 0.03)
# Min seconds between divergence-triggered resnapshots (each re-subscribes ALL book
# tickers). Without this, a single chronically-divergent ticker triggers a resnapshot
# every audit → a storm that floods the WS and causes more seq gaps. The divergent
# ticker stays suspect (untradeable) regardless; we just stop re-snapshotting for it.
KALSHI_RESNAP_THROTTLE_SECONDS: float = _float("KALSHI_RESNAP_THROTTLE_SECONDS", 30.0)
# Seconds between REST polls (price refresh in ticker mode; book-integrity audit in
# orderbook mode). Lower = stale book levels heal faster. Token cost is trivial.
KALSHI_AUDIT_INTERVAL: int = _int("KALSHI_AUDIT_INTERVAL", 3)

# ── WS receive-loop timing diagnostics (default OFF — zero cost when off) ──────────
# When True, instruments the Poly US + Kalshi WS receive loops with per-message inline
# handler time (parse+cache+callback-scheduling), inter-arrival gap, and rolling msg rate,
# plus an independent event-loop-lag probe — to decide whether the loop keeps up under
# game-time load. Sampled 1-in-N and buffered, flushed to logs/ws_loop_timing.csv (a
# SEPARATE diagnostic file; never an existing log). Pure observability: detection/cache/
# fire logic is byte-for-byte identical on or off. See bot/core/ws_timing.py.
WS_LOOP_TIMING: bool = _bool("WS_LOOP_TIMING", False)
WS_LOOP_TIMING_SAMPLE_N: int = _int("WS_LOOP_TIMING_SAMPLE_N", 1)   # buffer 1 row per N msgs
WS_LOOP_TIMING_FLUSH_N: int = _int("WS_LOOP_TIMING_FLUSH_N", 200)   # flush buffer every N rows
# Skip arbs whose Poly leg is a longshot priced below this. A <n> leg means the other
# leg is <n>: if the cheap leg misses, the expensive leg strands — the asymmetry that
# turns a miss into a big loss. Higher = safer, fewer opportunities.
KALSHI_MIN_POLY_PRICE: float = _float("KALSHI_MIN_POLY_PRICE", 0.10)
# Persistence confirm: an edge must stay >= KALSHI_ARB_MIN_EDGE continuously for this many
# seconds before the bot fires. This is the S11 LEVER: 0 ⇒ confirm at FIRST sight = fire-at-
# detection (the ~300ms persistence wait is gone; the fresh-REST verify in _execute_kalshi_arb is
# the real phantom filter, so the wait is only a rate-limit / weak adverse-selection proxy). A
# positive value reinstates the gate — DEPLOYED = 0.0 (fire-at-detection, DRY). ⚠️ Do NOT set 0 in LIVE without the
# exec_order_probe adverse-selection data (firing on <0.3s edges may lose to the ~100ms fill).
# (NOT proof of profit — see settlement gate + backtest.) See TODO.md S11.
KALSHI_CONFIRM_SECONDS: float = _float("KALSHI_CONFIRM_SECONDS", 1.5)
# §5 inter-leg-window probe (DRY-only, observability): after a would-fire the probe waits
# this long — a STAND-IN for the real poly-fill→kalshi-fire round-trip — then re-reads the
# Kalshi leg's fillable depth to measure how often it would have dropped below `shares` in
# the window (a simulated FOK kill). Touches no decision. See the private design notes §5.
# CALIBRATED 2026-06-26 to <region> latency from scripts/latency_probe.py + TCP/origin-read
# probes: network leg to the Poly ORDER host (api.polymarket.us) is ~1ms (sub-ms TCP, colocated);
# the Poly origin app round-trip (cf=MISS read) is ~22ms. NO script captures the order place→fill
# path, so the true order RTT is ≥ that + matching-engine time — confirmable only via live
# fills.log. **50ms CONFIRMED CORRECT [VERIFIED 2026-07-14 via scripts/order_rtt_probe on PROD]** —
# do NOT "fix" it to 61. The real Poly ORDER place→engine→response RTT (this constant's calibration
# target: poly_first fires Poly, then Kalshi, so the window ≈ the Poly order RTT) measured
# median 61ms (min 37, max 89, n=6). This constant is the SLEEP, not the modelled window: the probe
# sleeps it, then reads the book, so the book's server-side state lands at sleep + one-way (~9ms;
# the read's full RTT is ~18ms, visible as the logged delay_ms ≈ 65-76 at sleep=50). Effective window
# ≈ 50 + 9 = ~59ms vs the 61ms target — within 3%. The prior guess (22ms read RTT + cushion) was good.
#   Trap: `delay_ms` in interleg_probe.csv is the MEASURED elapsed (sleep + read RTT), NOT this
#   constant — segment the log on it, don't assume. Rows at delay_ms ≈ 265-278 are the OLD 250ms
#   placeholder era (a ~4x too-long window → they over-count kills); rows at ≈ 65-90 are this era.
#   Any §5 rate pooled across both is a mongrel — filter to the 65-90 band.
#   Kalshi's own order RTT is 17ms (min 14, max 24) — 3.6x faster than Poly, which is why
#   kalshi_first would carry a ~3.6x SHORTER inter-leg window (see the TODO execution-order item).
KALSHI_INTERLEG_PROBE_DELAY_MS: int = _int("KALSHI_INTERLEG_PROBE_DELAY_MS", 50)
# Book-trust gate: after a seq gap / REST divergence / resnapshot, treat the affected
# ticker(s) as untrustworthy for this many seconds (no trading until the book restabilizes).
# Tune after observing the server's gap rate (Phase 4.5) — on a gappy feed this ≈ halt.
KALSHI_BOOK_SUSPECT_SECONDS: float = _float("KALSHI_BOOK_SUSPECT_SECONDS", 5.0)

# ── Settlement-backtest pre-committed gate (scripts/settlement_backtest.py) ──────────
# Falsification harness constants — fixed BEFORE looking at data. The gate can only
# KILL / return PROVISIONAL / INSUFFICIENT — never "deploy."
SETTLEMENT_MIN_HEDGED_N: int = _int("SETTLEMENT_MIN_HEDGED_N", 30)   # on POST-haircut count
SETTLEMENT_F_PASS: float = _float("SETTLEMENT_F_PASS", 0.3)          # PASS requires net(F_PASS)>0
# Tail RATES are PLACEHOLDERS pending grounding from tournament history (count WC/major
# group-stage voids over last N tournaments; objective-scoreline divergences rarer still).
# Deliberately NOT the old 0.02/0.005 (an order too high → false-fail). The backtest prints
# break-even P_DIV / W_VOID so you see how close the verdict is to flipping on these. STAMP
# with source+date when grounded.
SETTLEMENT_P_VOID: float = _float("SETTLEMENT_P_VOID", 0.005)        # PLACEHOLDER — ground from history
SETTLEMENT_P_DIV: float = _float("SETTLEMENT_P_DIV", 0.002)          # PLACEHOLDER — ground from history
# Per-share $ wedge between Poly LFMP and Kalshi fair-price marks on a void. The single
# UNOBSERVED constant (never held a voided position) — flagged as the weakest in the report.
SETTLEMENT_W_VOID: float = _float("SETTLEMENT_W_VOID", 0.10)         # ASSUMPTION, unobserved
# Origin-frozen-book age threshold (seconds). A Poly book whose server transactTime is older
# than this is treated as a stale-book phantom, NOT a live quote: the verdict (scripts/
# subsecond_calibration.py + scripts/data_audit.py) EXCLUDES such both_fills from the
# settled∩both_fill denominator (a frozen-book fill is a manufactured edge, same defect class
# as a kalshi_fillable=0 phantom). 30 matches the Poly CDN max-age. SINGLE source of truth:
# the G1 fire-path freshness gate (when built) MUST import THIS constant so "fired" and
# "counted" use the same boundary — never a second literal. See the private design notes.
FROZEN_BOOK_AGE_S: float = _float("FROZEN_BOOK_AGE_S", 30.0)
# Origin-freeze fire-gate (G1): the firing book being stale (age > FROZEN_BOOK_AGE_S) is
# AMBIGUOUS alone (frozen vs legitimately-illiquid — the §5 trap), so it's only a FLAG. The
# non-ambiguous discriminator is CROSS-MARKET: an origin freeze stalls many books at once. Reject
# a freeze-suspect fire only when at least this many OTHER tracked markets are simultaneously
# stale; a LONE stale book is NOT rejected (preserve the real illiquid-edge tail). See
# bot/runner/kalshi_arb.py origin_freeze_suspect + bot/poly_us/feed.py count_stale_books.
ORIGIN_FREEZE_MIN_PEERS: int = _int("ORIGIN_FREEZE_MIN_PEERS", 2)
# Absolute stale-book ceiling (seconds), regardless of peers. G1 (above) preserves a LONE stale
# book as possibly-illiquid — but even a thin market churns quotes within seconds (measured:
# book-age p95≈14s across 411 would-fires; the only rows above 60s were confirmed freeze
# episodes), so a book whose transactTime has not moved in this long is FROZEN, not quiet, and is
# rejected on its own — closing the lone-straggler gap (a recovering cluster's last-still-frozen
# market slips G1's peer requirement). > FROZEN_BOOK_AGE_S. Reject reason: "frozen_book_stale".
FROZEN_BOOK_ABS_AGE_S: float = _float("FROZEN_BOOK_ABS_AGE_S", 60.0)
# Feed-wide-freeze peer threshold for a FRESH firing book. G1's cross-market check only runs when
# THIS book's own transactTime is stale, so it cannot see a freeze-RECOVERY phantom: the origin
# republishes a fresh transactTime over a still-stale price (fresh book age, but last_trade/OI
# ~30min old), which is indistinguishable from a legitimately-thin market by content-age alone.
# The non-ambiguous signal is that the FEED as a whole is frozen — so if at least this many OTHER
# in-window markets are simultaneously stale, reject even a fresh-looking book (it likely just
# recovered with stale content). Set ABOVE ORIGIN_FREEZE_MIN_PEERS: a fresh book demands stronger
# cross-market evidence than a self-stale one. Reject reason: "feed_wide_freeze_suspect".
FEED_WIDE_FREEZE_MIN_PEERS: int = _int("FEED_WIDE_FREEZE_MIN_PEERS", 3)
# Execution order. "poly_first" (default): fire the liquid Poly leg first, then the
# Kalshi FOK leg sized to the ACTUAL Poly fill; a Kalshi miss unwinds Poly (cheap),
# never strands the illiquid leg. "kalshi_first": fire Kalshi FOK first (zero cost if
# it kills), then complete Poly. Switchable for the live flatten-vs-strand A/B.
KALSHI_EXEC_ORDER: str = _optional("KALSHI_EXEC_ORDER", "poly_first").lower()
# Hard kill: if CUMULATIVE realized loss (flatten/strand cost + settled void/divergence
# losses, from execution_pnl.csv) exceeds this dollar budget, halt — independent of any
# booked/marked edge, monotonic, no auto-resume. See safety.is_exec_cost_cap_hit. 0 = off.
KALSHI_EXEC_COST_BUDGET: float = _float("KALSHI_EXEC_COST_BUDGET", 0.0)

# ── API endpoints ─────────────────────────────────────────────────────────────
# GAMMA_HOST is LIVE — team-name normalization (core/matcher.py) + macro-pair validation
# (kalshi/macro_pairs.py) hit gamma-api directly. (CLOB_HOST/CHAIN_ID removed with the global stack.)
GAMMA_HOST: str = "https://gamma-api.polymarket.com"

# ── Daily loss cap ────────────────────────────────────────────────────────────
# Maximum net loss (USD) allowed per UTC day before the bot stops taking new
# trades. Computed from guaranteed_profit in trades.log. 0 = disabled.
DAILY_LOSS_LIMIT: float = _float("DAILY_LOSS_LIMIT", 0.0)

# ── Kill switch ───────────────────────────────────────────────────────────────
# Path to a file the operator can create to pause trade execution without
# killing the process.  `touch pause.json` pauses; `rm pause.json` resumes.
KILL_SWITCH_FILE: str = _optional("KILL_SWITCH_FILE", "pause.json")

# ── Notifications ─────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL: str = _optional("DISCORD_WEBHOOK_URL")
# Channel split [2026-08-06, operator-requested]: fills/position changes and the 30-min
# live report get their own channels; DISCORD_WEBHOOK_URL stays the alerts channel AND
# the fallback — an unset var degrades to it, never to a dropped message. Routing lives
# in scripts.poly_fill_watch._notify(channel=...); Pushover fires for alerts only.
DISCORD_FILLS_WEBHOOK_URL: str = _optional("DISCORD_FILLS_WEBHOOK_URL")
DISCORD_REPORTS_WEBHOOK_URL: str = _optional("DISCORD_REPORTS_WEBHOOK_URL")
# PROBE-STATS channel [2026-08-26, operator-approved] — the probe lane's own feed: per fill,
# per completed round trip, latch/tripwire events, and the end-of-run EV-bar verdict block.
# ⛔ NO FALLBACK to DISCORD_WEBHOOK_URL: this is a stats firehose and unset must mean SILENTLY
# OFF, never per-fill chatter on the alerts channel the operator's phone mirrors (the same
# rule DISCORD_SIGNIFICANT_EDGE_WEBHOOK_URL carries below). The short spelling is accepted
# because it is the name the feature was requested under, and a channel that is silently off
# on a name mismatch is exactly the footgun this feature exists to remove.
# Routing + the rate/failure contract live in bot/core/probe_notify.py.
DISCORD_PROBE_WEBHOOK_URL: str = (_optional("DISCORD_PROBE_WEBHOOK_URL")
                                  or _optional("DISCORD_PROBE_WEBHOOK"))
# Separate channel for "proper edge" alerts: every would-fire (an edge that passed
# EVERY gate — fillable + fresh, not a phantom). Falls back to the main webhook if
# unset; set to a distinct channel's webhook to keep the edge feed clean.
DISCORD_EDGE_WEBHOOK_URL: str = _optional("DISCORD_EDGE_WEBHOOK_URL")
# Third channel: a CLEAN, low-volume feed of only the edges worth acting on. Every edge still
# hits DISCORD_EDGE_WEBHOOK_URL (the exhaustive firehose); this one ALSO gets an edge only when
# it clears BOTH size thresholds below. Unset ⇒ no significant-edge alerts — NO fallback, so it
# can never pollute the errors/warnings or firehose channels. Duration is not a filter (edges
# fire at detection when KALSHI_CONFIRM_SECONDS=0, so there is no dwell time to measure).
DISCORD_SIGNIFICANT_EDGE_WEBHOOK_URL: str = _optional("DISCORD_SIGNIFICANT_EDGE_WEBHOOK_URL")
DISCORD_SIGNIFICANT_EDGE_MIN_EDGE: float = _float("DISCORD_SIGNIFICANT_EDGE_MIN_EDGE", 0.05)
DISCORD_SIGNIFICANT_EDGE_MIN_PROFIT: float = _float("DISCORD_SIGNIFICANT_EDGE_MIN_PROFIT", 0.0)
# Whether to send Discord notifications even when DRY_RUN is true
DISCORD_NOTIFY_DRY_RUN: bool = _bool("DISCORD_NOTIFY_DRY_RUN", False)
# Pushover push notifications (phone alerts for urgent arbs)
PUSHOVER_USER_KEY: str = _optional("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN: str = _optional("PUSHOVER_API_TOKEN")

# ── Venue-vs-tracker position reconciliation ────────────────────────────────────────────────
# The tracker is a LOCAL BELIEF assembled from order responses — and everything keys off it
# (the strand global-pause, the alert, the exposure caps). Nothing used to ask either venue what
# we ACTUALLY hold, so a lost order response (30s httpx timeout, 502, connection reset AFTER the
# engine filled) or a crash between fill and add_position left a real position that was invisible
# to us: no alert, no pause, no cap. This loop closes that blind spot.
RECONCILE_ENABLED: bool = _bool("RECONCILE_ENABLED", True)
RECONCILE_INTERVAL_S: int = _int("RECONCILE_INTERVAL_S", 60)
# Kalshi's portfolio GET is eventually consistent (it can 404 right after a create), so ONE
# divergent poll proves nothing. Escalate only after this many CONSECUTIVE divergent polls.
RECONCILE_CONFIRM_POLLS: int = _int("RECONCILE_CONFIRM_POLLS", 2)
# Operator-acknowledged positions ("this is also mine and I know about it"). NOT an ignore-list:
# entries record an EXACT (venue, market, qty), so they ADD to what we know we hold rather than
# punching a hole in the check — if an acked position later CHANGES, it is no longer what was
# acknowledged and alerts again. `touch logs/reconcile_ack` writes the current divergence here.
RECONCILE_WHITELIST_FILE: str = _optional("RECONCILE_WHITELIST_FILE", "logs/reconcile_whitelist.json")
RECONCILE_ACK_FLAG: str = _optional("RECONCILE_ACK_FLAG", "logs/reconcile_ack")

# ── settlement-aware exposure release ───────────────────────────────────────────────────────
# remove_position had no caller, so total_exposure() never dropped on settlement → MAX_TOTAL_EXPOSURE
# behaved as a LIFETIME budget and the bot wedged at the cap after a few arbs. This loop asks each
# non-stranded position's venue whether it settled and frees the exposure. Games settle in ~3h, so a
# 5-minute cadence is ample; the read is fail-closed (unreadable → keep → under-trade).
POSITION_RELEASE_ENABLED: bool = _bool("POSITION_RELEASE_ENABLED", True)
POSITION_RELEASE_INTERVAL_S: int = _int("POSITION_RELEASE_INTERVAL_S", 300)

# ── kalshi_first mirror probe (§5's mirror) ─────────────────────────────────────────────────
# KALSHI_INTERLEG_PROBE_DELAY_MS models poly_first's window (fire Poly → 61ms → fire Kalshi, so
# the KALSHI book is exposed). This models kalshi_first's: fire Kalshi → 17ms → fire Poly, so the
# POLY book is exposed. 17 = the MEASURED Kalshi ORDER RTT (median 17ms, min 14, max 24,
# scripts/order_rtt_probe on prod 2026-07-14) — 3.6x shorter than Poly's 61ms.
# ⚠️ Same trap as its sibling: this is the SLEEP, not the modelled window. The probe sleeps it,
# THEN reads the book, so the logged `delay_ms` is the MEASURED elapsed (sleep + read RTT) and is
# what analysis must segment on — never assume rows equal this constant.
KALSHI_MIRROR_PROBE_DELAY_MS: int = _int("KALSHI_MIRROR_PROBE_DELAY_MS", 17)

# ── Operational rails (see bot/core/{alert_health,heartbeat,maker_state,memguard,backup}.py) ──
# ⛔ EVERY DEFAULT BELOW IS AN ABSOLUTE PATH under the repo root, via `repo_path`. A relative
# operational default is a known, expensive defect here: `logs/execution_pnl.csv` is relative, so
# any launch from another cwd gives FileNotFoundError → 0.0 loss → BOTH loss caps permanently and
# silently inert (the private design notes 1.4). An env override may still be absolute or
# relative — that is the operator's choice; the DEFAULT must not be able to go wrong.

# Where alert-delivery outcomes are recorded, so a dead webhook is discoverable without asking the
# webhook. A send failure is never reported through the channel that just failed.
ALERT_HEALTH_FILE: str = _optional("ALERT_HEALTH_FILE") or repo_path("logs", "alert_health.json")

# One heartbeat file per long-running process; `scripts/opswatch.py` reads the directory.
HEARTBEAT_DIR: str = _optional("HEARTBEAT_DIR") or repo_path("logs", "heartbeat")

# Crash-durable maker state: resting orders, inventory, and loss-to-date, written as they change
# so they survive SIGKILL. Read at startup by `scripts/maker_recover.py`.
MAKER_STATE_FILE: str = _optional("MAKER_STATE_FILE") or repo_path("logs", "maker_state.json")

# Memory headroom. 0 disables an individual check. Defaults are sized for a small box — warn well
# above normal, halt well below the kill zone. The availability HALT is a disjunction: MIN_AVAIL
# floors COMBINED MemAvailable+SwapFree (OOM distance — a RAM-only floor false-halted real runs
# with swap still free), HARD_RAM_FLOOR floors RAM avail alone (the teardown must not run entirely
# from a swapfile). WARN_AVAIL is the non-halting paging warn on RAM avail alone.
MEMGUARD_ENABLED: bool = _bool("MEMGUARD_ENABLED", True)
MEMGUARD_WARN_RSS_MB: float = _float("MEMGUARD_WARN_RSS_MB", 400.0)
MEMGUARD_HALT_RSS_MB: float = _float("MEMGUARD_HALT_RSS_MB", 700.0)
MEMGUARD_MIN_AVAIL_MB: float = _float("MEMGUARD_MIN_AVAIL_MB", 256.0)
MEMGUARD_WARN_AVAIL_MB: float = _float("MEMGUARD_WARN_AVAIL_MB", 120.0)
MEMGUARD_HARD_RAM_FLOOR_MB: float = _float("MEMGUARD_HARD_RAM_FLOOR_MB", 80.0)
# /tmp is a RAM-backed tmpfs on this box, so files there consume the same memory. Warn only.
MEMGUARD_TMPFS_WARN_MB: float = _float("MEMGUARD_TMPFS_WARN_MB", 400.0)

# Tier-1 (irreplaceable) log backup. UNSET = NO BACKUP EXISTS, which is the status quo and is
# reported as a PROBLEM by opswatch rather than silently accepted. `PMB_BACKUP_BUCKET` is the
# older name and still works so an already-installed unit keeps going.
PMB_BACKUP_DEST: str = _optional("PMB_BACKUP_DEST") or _optional("PMB_BACKUP_BUCKET")
# A backup older than this is stale. 26h ⇒ a daily timer may miss one run before complaining.
BACKUP_MAX_AGE_H: float = _float("BACKUP_MAX_AGE_H", 26.0)
