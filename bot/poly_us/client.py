"""
bot/poly_us_client.py
─────────────────────
Authenticated Polymarket US client (polymarket-us SDK) with dry-run support.

Polymarket US is a separate, CFTC-regulated exchange from global Polymarket: API-key auth
(key_id + secret_key), NOT wallet/EIP-712 signing; custodial balances (no Polygon wallet); markets
identified by slug (e.g. "atc-fwc-mex-rsa-2026-06-11-mex"), not token IDs.

This client presents the same surface the rest of the bot expects from PolymarketClient
(get_usdc_balance / get_best_ask / place_limit_fok), so the matcher, arb math and executor can treat
either venue uniformly. The opaque "market_slug" string carried in MarketPair.token_* fields is
interpreted here.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from typing import Optional, Sequence

from polymarket_us import AsyncPolymarketUS

from bot.core import config
from bot.core import venue_backoff
from bot.core import venue_budget
from bot.core.money import complement, from_float, parse_wire
from bot.core.logger import get_logger
from bot.core.redact import safe_exc
from bot.poly_us.sides import parse_token

log = get_logger(__name__)

_RECOVERY_PUBLIC_READER: ContextVar[Optional[tuple[object, object, "PolyUSClient"]]] = ContextVar(
    "poly_recovery_public_reader", default=None)

# One-shot capture flags [belief-recovery addendum §2]: the first STRUCTURED and the first
# UNSTRUCTURED "order not found" body each go on record once — an unstructured body would mean the
# not_found verdict is unreachable and the whole probe chain cannot arm.
_NOT_FOUND_BODY_LOGGED = False
_UNSTRUCTURED_NF_LOGGED = False

#: Seconds a caller's WS witness must POST-DATE the crossing guard's REST book before it may
#: override a would-cross refusal [see place_limit_gtc § THE NEWER-WITNESS RULE]. Two books stamped
#: within a few seconds of each other disagree about the market, not about time, and that
#: disagreement must keep refusing. Chosen from a read of recorded divergences:
#: real REST/WS divergences lag a few seconds, frozen-origin lags start later — the bound separates every recorded case.
GUARD_WITNESS_MARGIN_S = 60.0  # placeholder — production value withheld


def _guard_side_key(side: str) -> str:
    """`bid` / `ask` — the BOOK SIDE a crossing-guard alert state is keyed on.

    ⛔ NOT the intent token. One book side is spelled `sell` (open a short) or `sell_long` (dispose
    of a long) depending on inventory, so keying the state on `side` verbatim alternates keys
    mid-episode and each alternation reads as a first refusal. The alert is about the SIDE OF THE
    BOOK."""
    return "bid" if side == "buy" else "ask"


def _amount_to_float(amount: Optional[dict]) -> Optional[float]:
    """Parse a polymarket-us Amount ({'value': str, 'currency': 'USD'}) to float."""
    if not amount:
        return None
    try:
        return float(amount["value"])
    except (KeyError, TypeError, ValueError):
        return None


def _decimal_or_none(raw: object) -> Optional[Decimal]:
    """One BARE venue metadata number → `Decimal`, or None for "could not tell".

    ⛔ `Decimal(str(raw))`, never `Decimal(raw)` on a float — the latter launders a float's error
    into the Decimal and defeats the point. None in ⇒ None out, and an unparseable value is None
    too: a metadata field nobody can read is exactly as unknown as one the venue omitted.
    """
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None


_WIRE_PRICE_GRID = Decimal("0.0001")   # every Poly price field ships as a 4dp decimal string


def _wire_quantity(size: "float | int | Decimal") -> int | str:
    """The order-create quantity, exactly. Integral → int (the maker's proven wire shape,
    byte-identical to every real order ever sent); fractional → the fixed-point STRING of the
    Decimal. Never `int(round(size))`, which silently reshaped fractional closes (12.80 → 13, the
    overshoot-through-flat class).

    `format(q, "f")` never emits exponent notation — `str(Decimal("1E-7"))` would ship "1E-7".

    ⚠️ The fractional shape is `[UNVERIFIED-BY-FILL]` on the SELL side: the 2026-08-06 box probe
    echoed fractional quantities unrounded under `ORDER_INTENT_BUY_LONG` only, while `PolyMaker`
    ships fractional `ORDER_INTENT_SELL_LONG` orders whose evidence is the `--sell-held` PREVIEW
    alone. `scripts/poly_close.py:FRACTIONAL_CLOSE_ENABLED` still governs `poly_close`."""
    q = size if isinstance(size, Decimal) else Decimal(str(size))
    if not q.is_finite() or q <= 0:
        # The retired int(round()) RAISED on NaN/Infinity; format() would ship the
        # literal string. Programmatic callers bypass parse_close's regex, so guard here.
        raise PreSendRefusal(f"_wire_quantity: {size!r} is not a positive finite quantity")
    return int(q) if q == q.to_integral_value() else format(q, "f")


class PreSendRefusal(ValueError):
    """A pre-send caller-bug refusal from an order method: raised BEFORE any venue call, so the
    caller KNOWS nothing was placed (unlike a generic exception, where the order may rest).
    `bot/poly_us/maker._place` clears its durable intent on this type only — a phantom intent
    otherwise reads as `maybe_live_orders` and blocks a sibling lane's start. FOUR raise sites, all
    in the GTC path: unknown side, `::short` token, non-finite price, non-positive/non-finite
    quantity. Do not raise it after the venue call is issued."""


def _wire_price(price: Decimal | float) -> Decimal:
    """A caller's price snapped to the 4dp WIRE format — the ONE rounding, not a tick check.

    **A `Decimal` caller price quantizes DIRECTLY — no float round-trip**: routing it through
    `from_float` re-parses via `repr(float(x))`, the laundering the repo's Decimal rule forbids.
    Non-finite inputs raise the TYPED refusal.

    ⚠️ **4dp is the transport, NOT the grid this venue trades on.** The tradeable tick is per-market
    (0.01 on ~88% of Poly US, 0.005 and 0.001 elsewhere, varying *within* a series), so a price this
    returns can be perfectly 4dp and still OFF-tick; Poly then FLOORS the limit rather than rejecting
    it [VERIFIED /v1/order/preview 2026-06-24]. **On-tick-ness is the CALLER's job.**

    Rounding here, once, before anything reads the number, is what lets the crossing guard compare
    the value that will really be placed — two roundings of one price disagree exactly at the touch.
    Byte-identical to `f"{price:.4f}"` for every price on any tick ≥ 0.0001 [MEASURED 2026-07-27]."""
    d = price if isinstance(price, Decimal) else from_float(price)
    if not d.is_finite():
        # The TYPE is the point: an opaque InvalidOperation falls into `_place`'s generic except and
        # KEEPS the durable intent (phantom `maybe_live_orders`); the typed refusal proves nothing
        # was sent, so the intent is cleared. Mirrors `_wire_quantity`.
        raise PreSendRefusal(f"_wire_price: {price!r} is not a finite price")
    return d.quantize(_WIRE_PRICE_GRID, rounding=ROUND_HALF_EVEN)


def _level_px(level: dict) -> Decimal:
    """A book level's price as an exact Decimal. The venue sends `px` either as a bare number or as
    an {value,currency} Amount, so both shapes are read.

    Through `parse_wire`, the repo's named ingestion boundary — NOT `Decimal(raw)`. An unquoted JSON
    price arrives as a float, and `Decimal(0.005)` is 0.005000000000000000104083408558…, which
    equals no tick and would silently drop levels from the `px == best` comparison below."""
    raw = level["px"]["value"] if isinstance(level.get("px"), dict) else level["px"]
    return parse_wire(raw)


def touch_from_md(
    md: Optional[dict],
) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """(best_bid, best_ask, qty resting AT best_bid) from an order book's `marketData`, exact.

    THE touch parser: `place_limit_gtc`'s crossing guard and `scripts/poly_rebate_probe._fresh_touch`
    both read a book through this, so the guard and the probe cannot drift.

    Each side is independently None when empty or unparseable. **None means "we could not tell",
    NEVER "nothing to cross"** — every placement guard must read it as a refusal.

    max over bid prices and min over ask prices, never `bids[0]`/`offers[0]`: the SDK's book has no
    documented ordering. The bid TOB qty is the SUM at the best price — the queue ahead of us.
    Decimal throughout, and never raises: a shape change fails closed to all-None."""
    if not isinstance(md, dict):
        return None, None, None
    try:
        bids = [lv for lv in (md.get("bids") or []) if isinstance(lv, dict)]
        offers = [lv for lv in (md.get("offers") or []) if isinstance(lv, dict)]
        bid = max((_level_px(lv) for lv in bids), default=None)
        ask = min((_level_px(lv) for lv in offers), default=None)
        tob = (sum((parse_wire(lv.get("qty", "0")) for lv in bids if _level_px(lv) == bid),
                   Decimal(0)) if bid is not None else None)
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None, None, None
    return bid, ask, tob


# Phantom-vs-real liquidity/activity from a book's marketData.stats — None for each absent field.
_EMPTY_BOOK_STATS: dict = {
    "open_interest": None, "oi_age_s": None, "last_trade_px": None, "last_trade_qty": None,
    "last_trade_age_s": None, "shares_traded": None, "notional_traded": None,
}


def _parse_book_stats(md: Optional[dict], now: float) -> dict:
    """Liquidity/activity from a Poly book's `marketData.stats` — FREE (same fetch as the quote),
    LOGGING-ONLY. Every field is optional → None when absent; this MUST NOT raise (it rides the
    fire-path read). ⚠️ **UNITS — MEASURED 2026-07-24, do not infer them from the `currency: "USD"`
    tag**: `sharesTraded` and a trade frame's `quantity` are SHARE counts, while `notionalTraded` is
    in **CENTS** (so 44274039.23 is <n>). It is passed through UNCONVERTED — any ANALYSIS of
    `poly_notional_traded` must divide by 100."""
    s = md.get("stats") if isinstance(md, dict) else None
    if not isinstance(s, dict):
        return dict(_EMPTY_BOOK_STATS)

    def _num(x) -> Optional[float]:
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    return {
        "open_interest": _num(s.get("openInterest")),
        "oi_age_s": transact_age_s(s.get("openInterestSetTime"), now),
        "last_trade_px": _amount_to_float(s.get("lastTradePx")),
        "last_trade_qty": _num(s.get("lastTradeQty")),
        "last_trade_age_s": transact_age_s(s.get("lastTradeSetTime"), now),
        "shares_traded": _num(s.get("sharesTraded")),
        "notional_traded": _amount_to_float(s.get("notionalTraded")),
    }


# Public alias — the maker tapes these per quote cycle: the underscore would otherwise lie about
# external consumers, and an "unused private" cleanup would break the tape.
parse_book_stats = _parse_book_stats


def transact_age_s(transact_time: Optional[str], now: float) -> Optional[float]:
    """Seconds between `now` (epoch) and a Polymarket book transactTime (ISO-8601, ns
    precision). None on missing/unparseable input — fail-open, so a logging field can never
    break the fire path. Truncates the fraction to microseconds (datetime.fromisoformat
    rejects 9 fractional digits) and strips a trailing 'Z'."""
    if not transact_time:
        return None
    s = transact_time.rstrip("Z")
    if "." in s:
        head, frac = s.split(".", 1)
        s = f"{head}.{frac[:6]}"
    try:
        dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return now - dt.timestamp()
    except ValueError:
        return None


def quote_from_md(
    md: dict, is_short: bool
) -> tuple[Optional[float], str, list[tuple[float, float]], Optional[str], dict]:
    """The ask-space read of a book's `marketData`. See PolyUSClient.get_fill_quote, which is a
    fetch wrapped around this, for what every returned value means.

    Split out so a caller ALREADY HOLDING a book can read the same quote without fetching again: the
    Poly limit is ~1 req/s sustained and over-limit is THROTTLED rather than rejected, so a second
    fetch for data in hand degrades the freshness of the reads around it. NOT pure — the stats ages
    are stamped off time.time()."""
    state = md.get("state", "?")
    transact_time = md.get("transactTime")
    stats = _parse_book_stats(md, time.time())
    raw = md.get("bids", []) if is_short else md.get("offers", [])
    ask_levels: list[tuple[float, float]] = []
    for lvl in (raw or []):
        p = _amount_to_float(lvl.get("px")) if lvl.get("px") else None
        if p is None:
            continue
        ask_price = complement(p) if is_short else p       # exact; normalize short → ask space
        ask_levels.append((ask_price, float(lvl.get("qty") or 0)))
    if not ask_levels:
        return None, state, [], transact_time, stats
    ask = min(p for p, _ in ask_levels)
    return (ask if ask > 0 else None), state, ask_levels, transact_time, stats


def order_is_filled(resp: Optional[dict]) -> bool:
    """Return True only if a CreateOrderResponse shows a fully-filled order.

    A fully-filled FOK reports state == "ORDER_STATE_FILLED" OR cumQuantity >= quantity; a
    killed/rejected/expired one reports neither, so a non-None response alone does NOT mean filled.
    CAVEAT: without `synchronousExecution` the response can return before the FOK resolves (empty
    executions / NEW state) even though it fills async — no parsing here can detect that.
    """
    if not isinstance(resp, dict):
        return False
    for ex in resp.get("executions", []) or []:
        order = ex.get("order", {}) if isinstance(ex, dict) else {}
        if order.get("state") == "ORDER_STATE_FILLED":
            return True
        cum, qty = order.get("cumQuantity"), order.get("quantity")
        if isinstance(cum, int) and isinstance(qty, int) and qty > 0 and cum >= qty:
            return True
    return False


def order_filled_qty(resp: Optional[dict]) -> float:
    """Return how many contracts actually filled (cumQuantity).

    Poly US coerces our FOK to IOC, so an order can PARTIAL-fill. Return the max cumQuantity seen
    across executions (it is cumulative/monotonic). 0 = no fill.

    ⚠️ The max() is LOAD-BEARING — `executions[0]` LIES [VERIFIED 2026-07-15 on a real 255/300
    partial: the first execution reported `cumQuantity: 0`]. Reading it reports "no fill" on a leg
    that filled, which under poly_first is a naked, UNRECORDED Poly position.
    """
    if not isinstance(resp, dict):
        return 0.0
    best = 0.0
    for ex in resp.get("executions", []) or []:
        order = ex.get("order", {}) if isinstance(ex, dict) else {}
        cum = order.get("cumQuantity")
        try:
            if cum is not None:
                best = max(best, float(cum))
        except (TypeError, ValueError):
            continue
    return best


# The venue's four order intents, mapped to which POSITION each one concerns — NOT which side it
# ends up on. Poly's names are {verb}_{POSITION} with the verb acting ON the position, so
# `SELL_SHORT` DISPOSES of a short and ends up long while still concerning the SHORT position; the
# caller's `is_short` means the position, so the position reading is the one the guard needs. An
# UNRECOGNISED value must read as no-information, never silently as long. Source: the SDK's
# OrderIntent Literal (polymarket_us/types/orders.py).
_INTENT_IS_SHORT: dict[str, bool] = {
    "ORDER_INTENT_BUY_LONG": False,
    "ORDER_INTENT_SELL_LONG": False,
    "ORDER_INTENT_BUY_SHORT": True,
    "ORDER_INTENT_SELL_SHORT": True,
}
# `poly_avg_fill_cost` prices OPENING legs. A disposal has no defined value under its contract
# ("cost per share of the side we bought"), so it is refused rather than silently priced.
_OPENING_INTENTS = frozenset({"ORDER_INTENT_BUY_LONG", "ORDER_INTENT_BUY_SHORT"})


def poly_avg_fill_cost(resp: Optional[dict], *, is_short: bool) -> Optional[float]:
    """ACTUAL fee-inclusive cost per share from a Poly create, or None if unreadable.

    ⛔ `is_short` COMES FROM THE CALLER'S TOKEN, NOT FROM THE VENUE'S `intent` FIELD. `avgPx` is in
    YES space for both short intents, so a short's cost is the cost of the NO side,
    `(1 − avgPx) + fee`. A MISSING `intent` would resolve to the long formula, and below yes 0.5
    that flips the booked flatten loss NEGATIVE, which `bot/core/safety.py` clamps to $0.00 — a real
    loss, invisible to both caps. `intent` CAN be absent (the SDK's `Order` is `total=False`, and
    the one captured real create-response omits it on the max-cum execution this reads).

    When the venue DOES send a RECOGNISED `intent` that contradicts the caller, return None (CANNOT
    READ) rather than guess. ⚠️ `is_short` is REQUIRED, deliberately — an omission at a short call
    site must be a TypeError rather than a silently mirrored price.

    Poly reports both halves, so the Θ·p·(1−p) model isn't needed here: `avgPx` (the real VWAP) and
    `commissionNotionalTotalCollected` (the real commission, for the ORDER). [VERIFIED on the real
    2026-07-15 fill: 255 @ 0.0090, commission <n> → 0.009549/share.] Reads the MAX-cumQuantity
    execution, not executions[0] — that one LIES.

    None = CANNOT READ; the caller falls back explicitly. Never 0.0 (that would book a free fill).
    """
    if not isinstance(resp, dict):
        return None
    best_cum, best = 0.0, None
    for ex in resp.get("executions", []) or []:
        order = ex.get("order", {}) if isinstance(ex, dict) else {}
        try:
            cum = float(order.get("cumQuantity") or 0)
        except (TypeError, ValueError):
            continue
        if cum <= 0 or cum < best_cum:
            continue
        px = _amount_to_float(order.get("avgPx"))
        comm = _amount_to_float(order.get("commissionNotionalTotalCollected"))
        if px is None or comm is None:
            continue
        # ⛔ `avgPx` IS IN YES SPACE EVEN FOR A SHORT INTENT — a short's cost is `(1 − avgPx) + fee`,
        # not `avgPx + fee` [PROVEN on three real fills 2026-07-28,
        # the private design notes].
        # The value returned is the per-share cost OF THE SIDE WE BOUGHT, in that side's own space.
        # The venue's own `intent`, when present, must AGREE with the caller: guessing past a
        # contradiction books a real position at a mirrored price.
        # WHITELIST, not a suffix test — an unrecognised intent is NO INFORMATION (a suffix test
        # would make every short fill refuse on a proto3 zero value).
        intent = order.get("intent") or ""
        venue_short = _INTENT_IS_SHORT.get(intent)
        if venue_short is not None and venue_short != is_short:
            log.error(
                f"poly_avg_fill_cost: caller says is_short={is_short} but the venue reports "
                f"{intent!r} — refusing to price this fill. CANNOT READ, not a guess.")
            return None
        if intent and venue_short is None:
            log.warning(
                f"poly_avg_fill_cost: unrecognised intent {intent!r} — treating as NO INFORMATION "
                f"and trusting the caller's is_short={is_short}, not as a contradiction.")
        # A DISPOSAL is refused, not priced. This function's contract is "cost per share of the side
        # we BOUGHT"; a disposal has no defined value under it, and every caller reaches here from a
        # BUY-only path (`place_limit_fok` sends BUY_LONG/BUY_SHORT exclusively), so a `SELL_*`
        # intent IS a contradiction.
        # RECOGNISED and non-opening. An unrecognised intent is no-information (handled above) and
        # must not be read as a disposal.
        if intent in _INTENT_IS_SHORT and intent not in _OPENING_INTENTS:
            log.error(
                f"poly_avg_fill_cost: {intent!r} is a DISPOSAL, but this prices opening legs only. "
                f"Refusing rather than returning proceeds under a cost-shaped name.")
            return None
        fee = comm / cum
        best_cum, best = cum, ((1.0 - px) + fee) if is_short else (px + fee)
    return best


def bind_source_ip(sdk: object, source_ip: str) -> None:
    """Bind this SDK instance's HTTP transport to `source_ip` (blank = the default route).

    ⛔ THE VENUE'S LIMITS ARE PER KEY AND PER SOURCE IP, so which address a request leaves from is
    a budget decision, not a networking detail (the private design notes § Second IP for collectors). The SDK
    builds its own `httpx.AsyncClient` in its constructor and takes no transport argument, so the
    bound client REPLACES it — before any request has been issued, so there is no connection pool
    to drain. `local_address` is httpx's spelling of the local bind; the SDK is httpx, not aiohttp.
    """
    if not source_ip:
        return
    import asyncio
    import httpx
    timeout = getattr(sdk, "timeout", 30.0)
    previous = getattr(sdk, "_http", None)
    sdk._http = httpx.AsyncClient(                     # type: ignore[attr-defined]
        timeout=timeout,
        transport=httpx.AsyncHTTPTransport(local_address=source_ip),
    )
    if isinstance(previous, httpx.AsyncClient):
        # It has issued nothing (we replace it in the constructor), so there is no pool to drain —
        # but closing it is free and keeps the "unclosed client" warning out of the journal. No
        # running loop (a sync CLI constructing the client) means nothing to schedule it on; GC
        # reclaims an unused client.
        try:
            asyncio.get_running_loop().create_task(previous.aclose())
        except RuntimeError:
            pass


def _prio_from_env(prio: int) -> int:
    """`PMB_VENUE_PRIO` applied to a COLLECTOR-class `prio`, so a unit file can name a producer
    seat-bearing without editing its script (`Environment=PMB_VENUE_PRIO=2`).

    ⛔ CLAMPED TO THE COLLECTOR CLASSES, BOTH WAYS. An env var must never promote a collector into
    the maker's or the operator's class: a mis-set variable would let a bulk pass outrank a
    quoting maker's cancel. A maker/operator `prio` ignores the variable entirely.
    """
    if prio < venue_budget.PRIO_COLLECTOR_HIGH:
        return prio
    raw = os.environ.get("PMB_VENUE_PRIO", "").strip()
    try:
        wanted = int(raw)
    except ValueError:
        return prio
    return wanted if wanted in (venue_budget.PRIO_COLLECTOR_HIGH,
                                venue_budget.PRIO_COLLECTOR) else prio


def _reader_of(obj: object) -> Optional["PolyUSClient"]:
    """The client `obj`'s PUBLIC GETs go out on, or None to make them on `obj` itself.

    ⛔ A MODULE FUNCTION READING `getattr`, not a method and not attribute access: several fakes in
    the suite BORROW `PolyUSClient`'s read methods onto an object that is not one (`_FilledClient`
    in tests/test_poly_rebate_probe.py), where neither the class attribute nor a helper METHOD
    exists. The answer there is "no reader" — the same behaviour as an unwired production client.
    A self-referential wiring also reads as None, so a delegate can never recurse.
    """
    active = _RECOVERY_PUBLIC_READER.get()
    reader = (active[2] if active is not None and active[0] is asyncio.current_task()
              and active[1] is obj else getattr(obj, "public_reader", None))
    return reader if reader is not None and reader is not obj else None


class PolyUSClient:
    """Polymarket US CLOB client with dry-run support. Async-native."""

    #: The client every PUBLIC GET of this one goes out on; None = make them itself. The maker's
    #: launcher sets it to `collector_reader()`, so a public book/metadata read uses the
    #: collector source route instead of the maker's source route. Every PRIVATE call
    #: (place, cancel, open orders, positions, activities, balances) stays on `self`
    #: whatever this is. Class-level so a client built without `__init__` also has it.
    public_reader: Optional["PolyUSClient"] = None

    def __init__(self, *, obey_venue_backoff: bool = False,
                 prio: int = venue_budget.PRIO_OPERATOR,
                 beside_maker: bool = False,
                 allow_during_ban: bool = False,
                 source_ip: Optional[str] = None,
                 api_key_id: Optional[str] = None,
                 api_secret_key: Optional[str] = None) -> None:
        """`obey_venue_backoff` is the PRODUCER opt-in for the shared venue-health latch.

        `prio`/`beside_maker` are the SHARED VENUE BUDGET's class for this client
        (`bot/core/venue_budget.py`): every request this client makes waits for a token in the
        box-wide bucket, and a collector yields to a waiting maker. Unlike the latch above, the
        budget is NOT opt-in — Cloudflare bans the IP, not the process, and the default
        `PRIO_OPERATOR` only means "a hand tool, which outranks a collector and yields to a
        quoting maker". The maker passes `PRIO_MAKER`; timer producers pass `PRIO_COLLECTOR`,
        which also refuses to draw at all beside a live real maker unless `beside_maker=True`.
        A collector-class `prio` is overridden by `PMB_VENUE_PRIO` when the unit file sets it
        (`_prio_from_env`, clamped to the collector classes).

        ⛔ `allow_during_ban` IS THE BOX-BAN EXEMPTION AND DEFAULTS TO FALSE — including for
        `PRIO_OPERATOR`, because that is also what an unattended producer gets when nobody names a
        class. It is passed by the SIX hand tools that move or verify a position (`poly_close`,
        `poly_cancel`, `poly_lane_close`, `poly_pos_guard`, `poly_cert_reconcile`,
        `poly_positions`): a rate limit must never make an open position unmanageable. Such a call
        is still metered.

        `api_key_id`/`api_secret_key` override the credentials this client authenticates with;
        `None` (the default, and what the maker and every tool pass) takes `config`. The ONE caller
        that overrides them is `collector_reader`, which builds a public-read client on the
        collector source route. Public GETs use the unauthenticated gateway; the key does not
        add public quota.

        `source_ip` is the LOCAL address every request binds to; `None` takes
        `config.POLY_SOURCE_IP` and blank means the default route. It also selects the budget's
        bucket, because both the venue's public limit and Cloudflare's edge are per IP
        (the private design notes § Second IP for collectors).

        ⛔ **DEFAULT FALSE, AND THE DEFAULT IS THE MONEY PATH.** The live maker, every order tool and
        every operator read construct this client with no arguments and therefore NEVER consult
        `logs/venue_backoff.json`: a producer's observation must not stop a real-money run from
        reading its own book, cancelling its own order, or verifying its own inventory. Producers
        pass `True` so their bulk reads raise `venue_backoff.VenueBackoff` instead.
        """
        self._obey_venue_backoff = bool(obey_venue_backoff)
        #: TAPE-ONLY: the LAST book `place_limit_gtc`'s crossing guard read,
        #: per slug — {"bid","ask","age_s","src","refused"}. It decides NOTHING: no branch in this
        #: module reads it.
        self.last_guard_book: dict[str, dict] = {}
        #: TAPE-ONLY [I2]: the LAST create ACK latency in ms, per slug — the wall span across
        #: `self._sdk.orders.create` alone, i.e. request send → venue ack. NOT the guard's book
        #: read (that request happens earlier in `place_limit_gtc` and is deliberately outside
        #: the span). Written only when an ack ARRIVED; a raise leaves the previous value, and
        #: the maker pops it, so an unacked send tapes blank. It decides NOTHING.
        self.last_ack_lag_ms: dict[str, float] = {}
        #: ALERT-LEVEL STATE ONLY: (slug, side) → (first_refusal_ts, count) for the crossing
        #: guard. It gates the LOG LEVEL of a refusal and nothing else.
        self._guard_refusal_state: dict[tuple[str, str], tuple[float, int]] = {}
        self._dry_run = config.DRY_RUN
        _key_id = config.POLYMARKET_US_KEY_ID if api_key_id is None else str(api_key_id).strip()
        _secret = (config.POLYMARKET_US_SECRET_KEY if api_secret_key is None
                   else str(api_secret_key).strip())
        if not self._dry_run and (not _key_id or not _secret):
            raise ValueError(
                "Polymarket US requires POLYMARKET_US_KEY_ID and "
                "POLYMARKET_US_SECRET_KEY (set them in .env, or enable DRY_RUN)"
            )
        self._sdk = AsyncPolymarketUS(
            key_id=_key_id or None,
            secret_key=_secret or None,
        )
        self._source_ip = config.POLY_SOURCE_IP if source_ip is None else str(source_ip).strip()
        bind_source_ip(self._sdk, self._source_ip)
        log.info(f"poly_us: source address {self._source_ip or 'default'}")
        # ⛔ THE ONE METERING POINT FOR THE WHOLE TREE. It wraps the SDK's `_request`, so it also
        # covers the producers that page `client._sdk.markets.list` directly. WS is untouched.
        venue_budget.install(self._sdk, prio=_prio_from_env(prio), beside_maker=beside_maker,
                             source_ip=self._source_ip, allow_during_ban=allow_during_ban)
        log.info("Polymarket US client initialized")
        if self._dry_run:
            log.info("DRY RUN mode enabled — no real orders will be placed")

    async def close(self) -> None:
        await self._sdk.close()

    @asynccontextmanager
    async def recovery_public_reads(self) -> AsyncIterator[None]:
        """Use a ban-exempt collector reader only in this task's flatten phase."""
        if self.public_reader is None:
            yield
            return
        reader = collector_reader(allow_during_ban=True)
        if reader is None:
            yield
            return
        token = _RECOVERY_PUBLIC_READER.set((asyncio.current_task(), self, reader))
        try:
            yield
        finally:
            _RECOVERY_PUBLIC_READER.reset(token)
            await reader.close()

    def check_venue_backoff(self) -> None:
        """Raise `venue_backoff.VenueBackoff` if this client obeys the latch and it is active.

        A NO-OP unless `obey_venue_backoff=True` was passed to `__init__`, so the maker's fire path
        cannot reach the file at all. Public because the bulk producers page `/v1/events` and
        `markets.list` through `client._sdk` directly and call this before each page, so a latch
        armed by ANOTHER process stops the pass mid-crawl.
        """
        # `getattr`, not attribute access: the suite exercises parsing via `__new__`, which never
        # runs `__init__`, and defaulting to False there is the maker-path (SAFE) behaviour.
        if getattr(self, "_obey_venue_backoff", False):
            venue_backoff.raise_if_active()

    async def get_usdc_balance(self, force_real: bool = False) -> float:
        """Return free-to-trade USD balance (buyingPower). -1.0 on error.

        In DRY_RUN this returns a 9999 sentinel so the executor's pre-trade check always "affords"
        simulated trades. `force_real=True` bypasses that and fetches the actual balance (e.g. for
        FBAR/tax logging) — a read-only query, safe in any mode.
        """
        if self._dry_run and not force_real:
            return 9999.0
        try:
            resp = await self._sdk.account.balances()
            balances = resp.get("balances", []) if isinstance(resp, dict) else []
            for bal in balances:
                if bal.get("currency") == "USD":
                    return float(bal.get("buyingPower") or 0.0)
            return 0.0
        except Exception as exc:
            # Trim: a venue 5xx returns a multi-KB HTML error page (e.g. Cloudflare's ~50-line 504),
            # which floods stdout if logged raw. Collapse whitespace + cap to one short line.
            msg = " ".join(str(exc).split())[:200]
            log.error(f"PolyUSClient.get_usdc_balance failed: {msg}")
            return -1.0

    async def get_best_ask(self, market_slug: str) -> Optional[float]:
        """Return best ask for a market-side slug, or None if no ask / error.

        ⛔ **CDN-CACHED — NEVER PRICE OR GUARD AN ORDER OFF THIS.** `markets.bbo` is a plain public
        GET, and every Poly public GET is Cloudflare `max-age=30`, so this can be half a minute old
        with nothing in the response saying so. Fine for reporting/diagnostic reads where 30s does
        not matter; anything a placement depends on must cache-bust via `_fetch_book(fresh=True)` +
        `touch_from_md`."""
        reader = _reader_of(self)
        if reader is not None:
            return await reader.get_best_ask(market_slug)
        try:
            resp = await self._sdk.markets.bbo(market_slug)
            md = resp.get("marketData", {}) if isinstance(resp, dict) else {}
            return _amount_to_float(md.get("bestAsk"))
        except Exception as exc:
            log.debug(f"PolyUSClient.get_best_ask({market_slug}): {exc}")
            return None

    async def get_best_bid(self, market_slug: str) -> Optional[float]:
        """Return best bid for a market-side slug, or None if no bid / error.

        The mirror of get_best_ask, read off the SAME bbo call and the same `marketData` envelope,
        and carrying the same ⛔ CDN-CACHED warning — see there. It has **no caller in the repo**;
        route anything that decides an order through `_fetch_book(fresh=True)` + `touch_from_md`.
        Same fail-to-None contract: an unreadable book must never resolve to a number a crossing
        check would then pass."""
        reader = _reader_of(self)
        if reader is not None:
            return await reader.get_best_bid(market_slug)
        try:
            resp = await self._sdk.markets.bbo(market_slug)
            md = resp.get("marketData", {}) if isinstance(resp, dict) else {}
            return _amount_to_float(md.get("bestBid"))
        except Exception as exc:
            log.debug(f"PolyUSClient.get_best_bid({market_slug}): {exc}")
            return None

    async def get_market_meta(self, slug: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """`(tick, minimum trade quantity)` for ONE market, from ONE metadata request.

        Both come out of the SAME `markets.retrieve_by_slug` payload the maker's `prepare()` already
        paid for per book, so reading the minimum needs no second call.

        ⛔ **`minimumTradeQty` IS PER-MARKET, NOT A VENUE CONSTANT** — 0.01 on an `aec-*` game book,
        1 on 684 `tec-*` futures. **Either value may be None, and None means "could not tell", never
        a default**: the tick half keeps `get_market_tick`'s doctrine (REFUSE), while the min-qty
        half falls back to the documented `MIN_TRADE_QTY` and says so per book, so an unreadable
        minimum only makes the dust rail conservative. `Decimal` parsed from `str()`.
        """
        md = await self._market_payload(slug)
        if md is None:
            return None, None
        return _decimal_or_none(md.get("orderPriceMinTickSize")), \
            _decimal_or_none(md.get("minimumTradeQty"))

    async def _market_payload(self, slug: str) -> Optional[dict]:
        """ONE market's metadata body, unwrapped from its `{"market": {...}}` envelope, or None.

        The same `markets.retrieve_by_slug` request `get_market_meta` and `get_market_status` both
        read. One method, because a second copy of the envelope unwrapping is a second thing to get
        wrong. None means "could not tell" — never a default. A PUBLIC GET, so it goes out on
        `self.public_reader` when one is wired — see `_fetch_book`.
        """
        reader = _reader_of(self)
        if reader is not None:
            return await reader._market_payload(slug)
        try:
            resp = await self._sdk.markets.retrieve_by_slug(slug)
        except Exception as exc:
            log.debug(f"PolyUSClient._market_payload({slug}): {exc}")
            return None
        md = resp.get("market", resp) if isinstance(resp, dict) else None
        return md if isinstance(md, dict) else None

    async def get_market_status(self, slug: str) -> Optional[str]:
        """The venue's own `status` enum for ONE market (`MARKET_STATUS_*`), or None.

        **None means "could not tell", and every caller must treat it as NOT settled** — an
        unreadable status is the fail-closed direction for the teardown's settled carve-out
        (`maker.SETTLED_MARKET_STATUSES`), which zeroes a belief on the strength of this string.
        A non-string or empty value is the same answer as an error: None.
        """
        md = await self._market_payload(slug)
        if md is None:
            return None
        status = md.get("status")
        return status if isinstance(status, str) and status else None

    async def get_market_tick(self, slug: str) -> Optional[Decimal]:
        """The market's minimum price increment (`orderPriceMinTickSize`), or None.

        **None means REFUSE, never "use a default."** The tick varies per market (0.01 on 88% of
        this venue, 0.005 on 243 markets, 0.001 on ~1,039 incl. the midterm-control markets) and
        WITHIN a series (MLB 0.005 vs NBA/WNBA 0.01, VERIFIED 2026-07-16), so a defaulted tick is
        right often enough to look correct while silently mispricing the markets that differ.

        Returns a `Decimal` parsed from the venue's value via `str()` — this is the grid every quote
        price is built on. The venue sends a BARE number here, not the {value,currency} Amount its
        neighbours use, and wraps the payload as {"market": {...}} [both VERIFIED live 2026-07-26].
        ⛔ THE PARSE LIVES IN `get_market_meta`; errors resolve to None at debug level."""
        tick, _min_qty = await self.get_market_meta(slug)
        return tick


    async def _fetch_book(self, slug: str, *, fresh: bool):
        """Single choke-point for every order-book GET, so the cached-vs-live decision lives in ONE
        place. fresh=True appends a nonce query param so the read bypasses Polymarket's 30s
        Cloudflare /book cache (cf=MISS → origin) — REQUIRED for anything feeding a live decision,
        sizing, or the strand-unwind; fresh=False stays cacheable for bulk/discovery. Same SDK
        request either way, only the cache key differs. Also the choke-point for the venue-backoff
        check, so no caller can silently spend a request into a live CF ban.

        ⛔ AND THE CHOKE-POINT FOR THE SOURCE: when `self.public_reader` is set, the GET is made BY THE
        READER, on the collector source route (the guard's REST arm, the WS re-verify, the startup
        sweep, the flatten pricing and `get_book`/`get_book_depth` all route here). A refusal from
        the reader's bucket (`VenueBanned`/`VenueRefused`) RAISES to the caller exactly as our own
        would: retrying on the maker source would re-create the trip this split prevents."""
        # The latch is THIS client's opt-in, so it is honoured before the delegation: a producer
        # with `obey_venue_backoff=True` must not read around its own hold via a reader. `is not
        # self` so a self-referential wiring degrades to a direct read, never to recursion.
        self.check_venue_backoff()
        reader = _reader_of(self)
        if reader is not None:
            return await reader._fetch_book(slug, fresh=fresh)
        if fresh:
            return await self._sdk.get(
                f"/v1/markets/{slug}/book",
                query={"_": str(int(time.time() * 1000))},   # nonce → CF cache-bust
            )
        return await self._sdk.markets.book(slug)

    async def get_fill_quote(
        self, token: str, *, fresh: bool = False
    ) -> tuple[Optional[float], str, list[tuple[float, float]], Optional[str], dict]:
        """Fire-time (ask, market_state, ask_levels, transact_time, stats) for the token's
        tradeable side, from the order BOOK (which carries `state`; bbo does not). One book fetch
        yields all five — no extra round-trip.

        ask_levels is the tradeable side normalized to ASK space (long=offers as-is;
        short=(1−bid_px, qty)) so the caller can sum FILLABLE-AT-LIMIT depth; best-level-only depth
        would overstate fillable size in thin books. transact_time is the book's server-side
        mutation timestamp, or None. fresh=True bypasses the 30s Cloudflare cache.

        market_state matters: after a goal Poly SUSPENDS the market, freezing a stale price that is
        NOT tradeable, so the caller fires only when state == MARKET_STATE_OPEN. Errors return
        (None, '?', [], None, empty-stats) → fails closed. stats is LOGGING-ONLY, from the SAME
        fetch, and never raises into the fire path."""
        slug, is_short = parse_token(token)
        # BEFORE the try, deliberately: this method fails every error to a closed (None, "?", …)
        # tuple, which would swallow a VenueBackoff into "no quote". A hold has to be visible to the
        # caller to be a hold.
        self.check_venue_backoff()
        try:
            book = await self._fetch_book(slug, fresh=fresh)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_fill_quote({token}): {exc}")
            return None, "?", [], None, dict(_EMPTY_BOOK_STATS)
        md = book.get("marketData", {}) if isinstance(book, dict) else {}
        return quote_from_md(md, is_short)

    async def get_book(self, market_slug: str) -> Optional[dict]:
        """Return raw order book dict (bids/offers) for a market-side slug."""
        self.check_venue_backoff()      # before the fail-to-None try — see get_fill_quote
        try:
            return await self._fetch_book(market_slug, fresh=False)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_book({market_slug}): {exc}")
            return None

    async def get_book_depth(self, token: str) -> Optional[float]:
        """Authoritative REST depth (shares) at the tradeable side's best level — for the
        would-fire SAMPLER to log alongside the freeze-prone WS depth, so phantom depth can
        be caught by ground truth instead of inference. long=offers@min-px, short=bids@max-px.
        None on error/empty. Deliberately NOT used in the fire path (that fix is deferred)."""
        slug, is_short = parse_token(token)
        self.check_venue_backoff()      # before the fail-to-None try — see get_fill_quote
        try:
            book = await self._fetch_book(slug, fresh=False)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_book_depth({token}): {exc}")
            return None
        md = book.get("marketData", {}) if isinstance(book, dict) else {}
        levels = md.get("bids", []) if is_short else md.get("offers", [])
        pxs = [p for p in (_amount_to_float(l.get("px")) for l in (levels or []) if l.get("px"))
               if p is not None]
        if not pxs:
            return None
        best = max(pxs) if is_short else min(pxs)
        return sum(
            float(l.get("qty") or 0)
            for l in levels
            if l.get("px") and _amount_to_float(l.get("px")) == best
        )

    async def get_settlement(self, token: str) -> Optional[float]:
        """Return the market's LONG-side settlement price (1.0=long won, 0.0=long lost,
        intermediate=void/LFMP fair-value mark), or None if not settled / on error.

        Side-agnostic: settlementPrice is a property of the slug, so the short/long
        suffix is irrelevant here — the caller (bot.kalshi.settlement) applies the side.
        Read-only (a GET); safe in any mode, including DRY_RUN."""
        reader = _reader_of(self)
        if reader is not None:
            return await reader.get_settlement(token)
        slug, _is_short = parse_token(token)
        try:
            resp = await self._sdk.markets.settlement(slug)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_settlement({slug}): {exc}")
            return None
        if not isinstance(resp, dict):
            return None
        # The live API returns a plain numeric `settlement` (e.g. {"slug":..,"settlement":0}),
        # NOT the `settlementPrice: Amount` the SDK type claims. Accept either, and a bare
        # number. Use `is not None` — a definite long-loss settles at 0 (falsy but valid),
        # only a MISSING field means not-yet-settled.
        raw = resp.get("settlement")
        if raw is None:
            raw = resp.get("settlementPrice")
        if raw is None:
            return None
        if isinstance(raw, dict):
            return _amount_to_float(raw)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    async def place_limit_fok(
        self, token: str, price: float, size: float, label: str = ""
    ) -> Optional[dict]:
        """
        Place a FOK (Fill-or-Kill) limit BUY on the given market token.

        For a moneyline game ONE slug carries both teams: a bare slug buys the long side; a
        "<slug>::short" token buys the short side via BUY_SHORT. `price` is that side's price
        (long ask, or short = 1 − bid). DRY_RUN only logs.

        ⚠️ MISNOMER — **Polymarket US does not honor FOK: it silently REWRITES tif FILL_OR_KILL →
        IMMEDIATE_OR_CANCEL** [VERIFIED 2026-07-15 on a REAL order: FOK for 300 into 255 of depth
        came back IOC and PARTIAL-FILLED 255/300]. The venue DOCS are wrong. So the leg is IOC and
        **CAN PARTIAL-FILL**: the stranded-leg invariant is preserved NOT by the tif but by callers
        sizing the opposite leg off the ACTUAL fill this returns.
        """
        market_slug, is_short = parse_token(token)
        intent = "ORDER_INTENT_BUY_SHORT" if is_short else "ORDER_INTENT_BUY_LONG"
        # ⚠️ CALLER CONVENTION vs WIRE CONVENTION — they differ on a short, and this is where the two
        # meet. Callers pass `price` in the TOKEN's own space (short = `1 − yes_bid`); the venue
        # reads a BUY_SHORT price in YES space as a SELL limit [PROVEN 2026-07-28], so it is
        # complemented back HERE, once. Sent unconverted the fill was still right but the price
        # BOUND was not there. `complement`, not `1.0 - price`: the float subtraction is
        # 0.44999999999999996 for 0.55 and this value goes straight to a 4dp wire format.
        wire_price = complement(price) if is_short else price
        tag = "[DRY RUN] " if self._dry_run else ""
        log.info(
            f"{tag}ORDER(US)  slug={market_slug}  side={'short' if is_short else 'long'}  "
            f"price={price:.4f}  wire={wire_price:.4f}  size={size:.0f} shares  "
            f"cost=${price * size:.2f}  {label}"
        )
        if self._dry_run:
            return {"status": "dry_run", "market_slug": market_slug,
                    "price": price, "size": size}
        try:
            return await self._sdk.orders.create({
                "marketSlug": market_slug,
                "intent": intent,
                "type": "ORDER_TYPE_LIMIT",
                "price": {"value": f"{wire_price:.4f}", "currency": "USD"},
                "quantity": int(round(size)),
                # IOC, not FOK. Poly does not honor FOK — it silently rewrites it to IOC [VERIFIED
                # 2026-07-15 on a real order: 255/300 partial]. Naming what we actually get also
                # makes us independent of Poly's roadmap: if they ever SHIP real FOK, an order
                # asking for FOK would silently become all-or-nothing. And IOC is what we would
                # choose anyway: a partial fill is a smaller arb, which beats the nothing FOK
                # returns (measured: 87 events where the book held a median 3 against a median-7
                # target).
                "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
                "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
                # Block until the FOK is terminal so the response carries the real
                # executions/state. Without this, create() can return before the
                # order resolves and a real fill reads as a miss → stranded leg.
                "synchronousExecution": True,
            })
        except Exception as exc:
            # AN ERROR IS NOT AN OUTCOME — never `return None`, which is how an order whose fate we
            # do not know became "filled nothing" (`order_filled_qty` → 0.0 → "MISSED (no fill)").
            # A genuine IOC kill is a 200 with cumQuantity 0; this path is ONLY timeout / 502 /
            # reset — exactly the case where the engine may already have filled, since
            # `synchronousExecution` blocks the order ~61ms server-side. Callers must distinguish
            # "the venue said zero" from "we do not know", and raising is the only way to say the
            # second.
            log.error(f"PolyUSClient order failed for {market_slug}: {exc}")
            raise

    async def preview_order(
        self, token: str, price: float, size: float,
        tif: str = "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    ) -> Optional[dict]:
        """Preview a BUY via POST /v1/order/preview — server-side validation that PLACES NOTHING.
        Returns the venue's echoed Order, or None on error.

        Mirrors place_limit_fok's order shape so the preview validates EXACTLY what the fire path
        would send. ⚠️ It validates HANDLING ONLY — `cumQuantity`/`avgPx`/`commission*` come back 0,
        so never read a fill or cost estimate off it. Deliberately NOT DRY-gated (it places
        nothing); zero-capital, pre-flight only."""
        market_slug, is_short = parse_token(token)
        intent = "ORDER_INTENT_BUY_SHORT" if is_short else "ORDER_INTENT_BUY_LONG"
        # Mirror place_limit_fok's SHORT-space→yes-space conversion too, or the preview validates
        # the MIRROR of the real order for a `::short` token and the "EXACTLY" above is a lie.
        wire_price = complement(price) if is_short else price
        try:
            return await self._sdk.orders.preview({
                "request": {
                    "marketSlug": market_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{wire_price:.4f}", "currency": "USD"},
                    "quantity": int(round(size)),
                    "tif": tif,
                    "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
                }
            })
        except Exception as exc:   # an off-tick / malformed reject surfaces here — report, don't raise
            log.warning(f"PolyUSClient preview failed for {market_slug} @ {price:.4f}: {exc!r}")
            return None

    def _tape_guard_book(self, slug: str, bid: Optional[Decimal], ask: Optional[Decimal],
                         age_s: Optional[float], src: str, *, refused: bool,
                         ws: Optional[tuple[Decimal, Decimal, float]] = None) -> str:
        """Record what the crossing guard saw, and return the one-line fingerprint for its log.

        ⛔ TAPE-ONLY. Called on EVERY guard outcome after the touch is parsed and
        never before a comparison, so no decision in `place_limit_gtc` can depend on it. `src` is
        the read that produced it (`rest_fresh`, `unread`, `ws_over_stale_rest`); `ws` is the
        overriding witness and is passed on the override path ONLY — `bid`/`ask`/`age_s` stay the
        REST book we refused to believe, because that frozen body is the measurement."""
        self.last_guard_book[slug] = {
            "bid": bid, "ask": ask, "age_s": age_s, "src": src, "refused": bool(refused),
            "ws_bid": None if ws is None else ws[0],
            "ws_ask": None if ws is None else ws[1],
            "ws_age_s": None if ws is None else ws[2],
        }
        return (f"[guard {src} bid={'' if bid is None else bid} ask={'' if ask is None else ask} "
                f"age_s={'' if age_s is None else f'{age_s:.1f}'}]")

    def _guard_refused(self, slug: str, side: str, msg: str) -> None:
        """Log one crossing-guard refusal at ALERT level only on the STATE CHANGE.

        ⛔ LOG LEVEL ONLY — the refusal decision, the `refused` tape flag and the return value are
        the caller's. A refusal repeats every requote (~4 s) while the condition holds and the
        Discord handler dedups on EXACT text while the suffix carries a moving `age_s=…`, so the
        FIRST refusal after a pass (or ever) on this (slug, side) pages at `warning` and every
        repeat goes to the log at `info`. The clear is `_guard_cleared`."""
        key = (slug, _guard_side_key(side))
        first_ts, count = self._guard_refusal_state.get(key, (time.time(), 0))
        self._guard_refusal_state[key] = (first_ts, count + 1)
        (log.warning if count == 0 else log.info)(msg)

    def _guard_cleared(self, slug: str, side: str, guard: str) -> None:
        """Page ONCE when a (slug, side) placement passes the guard after ≥1 refusal, then forget.

        A no-op when that (slug, side) was not refusing, so the steady state (every placement
        passing) pages nothing at all."""
        _side = _guard_side_key(side)
        st = self._guard_refusal_state.pop((slug, _side), None)
        if st is None:
            return
        first_ts, count = st
        log.warning(f"cross guard cleared {slug} {_side}: refused {count} placements over "
                    f"{time.time() - first_ts:.0f} s {guard}")

    def _guard_override(self, slug: str, side: str, msg: str) -> None:
        """INFO once on ENTERING the ws-over-stale-REST state; silent while it persists.

        Same one-line-per-state-change discipline as `_guard_refused`, one level down: the override
        never pages (the quote goes up, post_only on), but a run that starts distrusting its own
        REST reads should say so once. Its own key namespace, so entering and leaving the override
        does not disturb a refusal episode."""
        key = (slug, f"{_guard_side_key(side)}:ws_witness")
        first_ts, count = self._guard_refusal_state.get(key, (time.time(), 0))
        self._guard_refusal_state[key] = (first_ts, count + 1)
        if count == 0:
            log.info(msg)

    def _guard_override_reset(self, slug: str, side: str) -> None:
        """Forget the ws-over-stale-REST state, so the next entry into it logs again. Called on a
        placement the REST book cleared on its own."""
        self._guard_refusal_state.pop((slug, f"{_guard_side_key(side)}:ws_witness"), None)

    @staticmethod
    def _newer_witness_clears(
        side: str, yes_px: Decimal, rest_ts: Optional[float],
        ws_touch: Optional[tuple[Decimal, Decimal, float]], post_only: bool,
    ) -> Optional[float]:
        """Seconds by which the caller's WS witness POST-DATES the guard's REST book, when that
        witness may override a would-cross refusal — else None (refuse, as before).

        ⛔ STATIC because it reads no instance state and ONE copy of this rule may exist: the
        teardown flatten asks the same question about the same two books (`PolyMaker.
        _teardown_flatten`) and must not carry a second implementation of it.

        ⛔ MONEY PATH. Every arm below is a REFUSAL that stays a refusal:
          • no `ws_touch` — the caller offered no witness.
          • `post_only=False` — the guard is that caller's ONLY protection, and the override is
            safe precisely because the venue closes the race atomically.
          • the REST book carries no `transactTime` — an unstamped body cannot be SHOWN to be older.
          • the witness is not `GUARD_WITNESS_MARGIN_S` newer — two books stamped together disagree
            about the market, not about time.
          • the witness ALSO crosses — both books agree the price takes."""
        if ws_touch is None or not post_only or rest_ts is None:
            return None
        ws_bid, ws_ask, ws_ts = ws_touch
        if ws_bid is None or ws_ask is None:
            return None
        # ⛔ BOTH EPOCHS ARE THE VENUE'S `transactTime`s, compared directly — no second
        # `time.time()` between the tape write and this comparison.
        newer_by = ws_ts - rest_ts
        if newer_by < GUARD_WITNESS_MARGIN_S:
            return None
        clears = yes_px > ws_bid if side in ("sell", "sell_long") else yes_px < ws_ask
        return newer_by if clears else None

    async def place_limit_gtc(
        self, token: str, price: Decimal | float, size: float, label: str = "",
        *, post_only: bool = False, side: str = "buy",
        good_till_time: Optional[str] = None,
        ws_touch: Optional[tuple[Decimal, Decimal, float]] = None,
    ) -> Optional[dict]:
        """Place a RESTING (maker) limit order — `TIME_IN_FORCE_GOOD_TILL_CANCEL`.

        ── `good_till_time` — the OPTIONAL venue-side dead man (GTD) ────────────────────────────
        `None` (the default) is today's behaviour, **byte-identical on the wire**: tif
        `TIME_IN_FORCE_GOOD_TILL_CANCEL` and no `goodTillTime` field. An RFC-3339 stamp switches the
        tif to `TIME_IN_FORCE_GOOD_TILL_DATE` and puts the deadline on the wire, so an order
        outlives neither its deadline nor a maker that dies without cancelling it.

        ⛔ **A DEAD-MAN'S BRAKE, NOT A KEEPALIVE — nothing renews a deadline.** Do not re-place an
        order merely to refresh its stamp: that trades the queue position this venue's fills depend
        on for nothing.

        ⚠️ **ECHO IS NOT ENFORCEMENT.** What is PROVEN is only that the create *preview* echoes
        `goodTillTime` and preserves `tif=GOOD_TILL_DATE` (`scripts/poly_gtd_preview.py`). Whether a
        resting order actually DISAPPEARS at its deadline is unobserved, so no caller may treat a
        deadline as a substitute for its own cancel. `PolyMaker._check_ttl` is the runtime rail.

        ⛔ **THERE IS NO MODIFY PATH IN THIS CLIENT** — the maker's REPLACE is `_cancel` → `_place`,
        so every re-quote carries a brand-new deadline. A real `orders.modify` path **MUST re-send
        `goodTillTime` on every amend**. The value is passed through VERBATIM (a second formatting
        of one timestamp is a second chance to disagree, as `_wire_price` documents for the price);
        a non-string or empty value raises `PreSendRefusal` before any venue call.

        ── `post_only` ─────────────────────────────────────────────────────────────────────────
        Poly's `participateDontInitiate` — "order must rest on the book prior to matching (maker
        only)" [VERIFIED in the official SDK]. It is a venue-side backstop BEHIND the crossing guard
        below: it closes the read-vs-place RACE the guard cannot, since only the venue can act
        atomically. Default False keeps every existing caller's payload byte-identical.
        ✅ **PROVEN ENFORCED 2026-07-19** by a two-arm real-money A/B (n=1 per arm): WITH the flag a
        buy priced through a 616k-deep ask RESTED; WITHOUT it the same order FILLED in 0.2s.
        **⚠️ NOT the documented behaviour** — a would-match order is ACCEPTED and RESTS, not
        rejected. Do not write code that expects a rejection.

        ── the crossing guard ──────────────────────────────────────────────────────────────────
        This method reads the live book and REFUSES to place through the touch — at or above the
        best ask on a buy, at or below the best bid on a sell.

        **THE WITNESS-ONLY ARM (`ws_touch` + `post_only`) — NO ORIGIN READ.** When the caller hands
        a witness AND `post_only=True`, the guard runs on that witness ALONE and makes no book
        request: the witness already passed the maker's freshness rails
        (`PolyMaker._ws_guard_witness`). A live run saw many origin reads per cycle, several holds and
        thousands of `fresh book read FAILED` refusals; that does not establish a limiter threshold.
        clears rule and refusal apply, taped `guard_src="ws_fresh"` with the witness's own
        `transactTime` age. No witness, a witness with an unreadable side, or `post_only=False`
        → the ORIGIN read below,
        unchanged.

        **THE NEWER-WITNESS RULE (`ws_touch`)** — the REST arm only, and the teardown flatten's own
        use of `_newer_witness_clears`. The refusal yields to a STRICTLY NEWER witness and
        to nothing else: the caller passes `ws_touch=(bid, ask, ts)`, the REST read says CROSS, that
        WS stamp is at least `GUARD_WITNESS_MARGIN_S` newer than the REST book's `transactTime`, the
        WS touch does NOT cross, and `post_only=True` — then the order is PLACED, taped
        `guard_src="ws_over_stale_rest"`. Every other case refuses as before: no `ws_touch`, a
        witness that is not newer, a WS touch that also crosses, an unreadable touch, an unread book
        (**an unreadable witness is not a stale one**), or `post_only=False` (an unprotected caller
        keeps the guard as its only protection). WHY, because this weakens a money-path refusal: the
        cache-busted REST read reaches ORIGIN and origin itself FREEZES (measured 2026-09-02/03,
        books frozen 300 s+ while WS moved), so the guard was refusing legitimate quotes on a dead
        witness. A stamp 5 s newer is evidence the REST body is stale; nothing else here is.
       

        ⚠️ **"LIVE BOOK" MEANS CACHE-BUSTED.** The touch comes from `_fetch_book(slug, fresh=True)`
        (the repo's single cache-busting choke-point), parsed by `touch_from_md` — the same parser
        the rebate probe uses. NOT `get_best_ask`/`get_best_bid`, which route through the CDN-cached
        `markets.bbo`: a stale-HIGH ask clears a buy that now crosses, i.e. it fails OPEN in the
        direction that costs money. **Rate cost:** one origin GET per placement from Poly's ~1 req/s
        budget — ZERO on the witness-only arm above; do not add a second read to this path.

        ⚠️ **BOTH sides FAIL CLOSED on an unreadable touch** — a missing side means "we could not
        tell", never "nothing to cross". Callers do NOT transparently retry
        (`scripts/poly_rebate_probe` records `CLIENT_REFUSED_CROSS` and ABORTS).

        ⚠️ **ONE CANONICAL ROUNDING (`_wire_price`)** before either guard runs, and that same value
        ships: two roundings of one number disagreed exactly at the touch (0.44996 passed the
        raw-float compare against a 0.45 ask and shipped as "0.4500").

        [VERIFIED 2026-07-19 zero-capital via preview_order: the venue ACCEPTS
        TIME_IN_FORCE_GOOD_TILL_CANCEL and echoes it back unrewritten — unlike FILL_OR_KILL, which
        Poly silently rewrites to IOC. So a GTC order genuinely rests.]

        ── `side="sell"` — the maker's ASK, as a SYNTHETIC short. PLUMBING ONLY, NO CALLER. ──────
        **Polymarket has no native resting ask**; an opening ask is a resting
        `ORDER_INTENT_BUY_SHORT`. ⚠️ **The price stays in YES SPACE on the wire — there is NO
        complement conversion**: the venue reads a BUY_SHORT `price` as a yes-space SELL limit
        ("fill me here or better"), so an ask at 0.853 ships as 0.853 [PROVEN 2026-07-28 on four
        real orders — the private design notes].
        ⚠️ **`SELL_SHORT` IS NOT THE ASK — it is a BUY.** Poly's intents are {verb}_{POSITION}, so
        `BUY_LONG` and `SELL_SHORT` both END UP LONG yes [MEASURED on 327,673 real prints: SELL_SHORT
        is `ORDER_SIDE_BUY` and prints ABOVE the yes mid 98.9%; BUY_SHORT hits the yes bid 97.8%;
        1:1, zero exceptions]. Do not re-derive this from the names.
        Crossing guard: refuse a yes-space ask at or BELOW the best yes bid. **UNUSED until a live
        Poly maker run is approved** — treat its first caller as a money-path change.

        ── `side="sell_long"` — the PASSIVE CLOSE OF A HELD LONG. ─────────────────────
        Same wire shape as `side="sell"` but the intent is `ORDER_INTENT_SELL_LONG`: same yes-space
        price, same rounding, same guard, same `post_only`.
        ⛔ **"THE TWO ARE NOT INTERCHANGEABLE" IS `[UNPROVEN]`, AND THE REPO CONTRADICTS ITSELF ON
        IT.** THE CLAIM: `sell` OPENS a short, `sell_long` DISPOSES of a long, so a `BUY_SHORT` sent
        to close a long would leave the long untouched and open a short beside it — basis, the
        intent NAMES. ⚠️ THE ONE REAL OBSERVATION POINTS THE OTHER WAY:
        `.claude/skills/venue-reference/SKILL.md` § intents, VERIFIED on a real fill where a
        `SELL_SHORT` *reduced* an existing short ("never the intent string"). `SELL_LONG` is correct
        under BOTH readings, so it is the safe ship, and the consequence of being wrong is
        asymmetric. ⛔ WHAT SETTLES IT: the `--sell-held` PREVIEW echo, then the first real fill —
        record it here AND in the skill; do not delete either claim before then. The FRACTIONAL half
        is `[UNVERIFIED-BY-FILL]`.
        ⛔ **REDUCE-ONLY IS ENFORCED BY THE CALLER, NOT THE VENUE.** `CreateOrderParams` has no
        `reduceOnly` field: size a `sell_long` at or below the held long, or it flips through flat.

        ── `::short` tokens are REFUSED ON BOTH SIDES — yes-space quoting only. ─────────────────
        One slug carries ONE book, in yes space, so a `::short` buy would compare a short-space
        price against the LONG side's ask — the wrong touch, and a guard reading the wrong touch is
        worse than none. **Not missing functionality**: an opening ask on the yes side IS a resting
        bid on the no side, which is what `side="sell"` on the LONG token sends.

        Refusal shapes differ ON PURPOSE: a would-cross or unreadable touch returns None (a market
        condition — retry), while an unknown `side`, a `::short` token, or a non-finite
        price/quantity raises `PreSendRefusal` BEFORE any venue call (a caller bug — nothing is
        placed). The TYPE is load-bearing: `_place` clears its durable intent on `PreSendRefusal`
        only.

        Returns the create response, or None if the price would cross (nothing placed). DRY logs only.
        """
        if side not in ("buy", "sell", "sell_long"):
            raise PreSendRefusal(
                f"place_limit_gtc: side must be 'buy', 'sell' or 'sell_long', got {side!r}")
        if good_till_time is not None and (
                not isinstance(good_till_time, str) or not good_till_time.strip()):
            # A caller bug, not a market condition — same class as an unknown `side`, so the same
            # type. Silently dropping a malformed deadline would send a GTC order under a caller
            # that believes it placed a self-expiring one.
            raise PreSendRefusal(
                f"place_limit_gtc: good_till_time must be a non-empty RFC-3339 string when set, "
                f"got {good_till_time!r}")
        market_slug, is_short = parse_token(token)
        if is_short:
            raise PreSendRefusal(
                f"place_limit_gtc: refusing the short token {token!r} — yes-space quoting only. "
                f"One slug carries ONE book, in yes space, so the crossing guard would compare a "
                f"short-space price against the LONG side's touch. A short-space quote is "
                f"side='sell' on the LONG token, which already sends BUY_SHORT in yes space "
                f"(see the docstring).")
        # ONE rounding, before anything reads the price: the guard, the log and the wire all see the
        # same 4dp number that will really be placed. See _wire_price.
        yes_px = _wire_price(price)
        # Quantity guard here too — BEFORE the DRY short-circuit, symmetric with the crossing
        # guard below, so a dry run exercises every refusal a real one would [round-3 nit].
        wire_qty = _wire_quantity(size)
        # Crossing check FIRST — before any DRY short-circuit, so a dry run exercises the same guard.
        wire_px = yes_px
        # ⛔ THE WITNESS-ONLY ARM. A caller-supplied `ws_touch`
        # plus `post_only` means NO origin read at all: the witness already passed the maker's own
        # freshness rails (`PolyMaker._ws_guard_witness` — content age <= `ws_stale_s`, socket
        # live, feed up, book not `_ws_dead_books`). In a live run a full slate spent many origin book
        # reads per cycle, the route was held repeatedly with a growing ladder, and under a hold EVERY
        # placement refused `fresh book read FAILED`. This observation establishes no limiter threshold.
        # ⛔ FAIL DIRECTION: any doubt falls to the REST arm below — no witness, either side of it
        # None, or `post_only=False` (that caller has no venue-side backstop, so the guard is its
        # only protection). There is no arm that places without a guard.
        _ws_only = (ws_touch is not None and post_only
                    and ws_touch[0] is not None and ws_touch[1] is not None)
        _rest_ts: Optional[float] = None    # the REST body's own stamp; None on the witness arm
        if _ws_only and ws_touch is not None:
            # ONE clock read, same discipline as the REST arm: the age on the tape and the
            # comparison come from the same `now`.
            _now = time.time()
            bid, ask = ws_touch[0], ws_touch[1]
            _guard_age_s: Optional[float] = max(0.0, _now - ws_touch[2])
            _g = self._tape_guard_book(market_slug, bid, ask, _guard_age_s, "ws_fresh",
                                       refused=False)
        else:
            # THE touch read, and it must reach ORIGIN. NOT get_best_ask/get_best_bid: those route
            # through the CDN-cached `markets.bbo`, so the guard could clear a placement against a
            # touch half a minute old, and the bias is fail-OPEN. One fetch carries BOTH touches.
            try:
                book = await self._fetch_book(market_slug, fresh=True)
            except Exception as exc:
                _g = self._tape_guard_book(market_slug, None, None, None, "unread", refused=True)
                self._guard_refused(
                    market_slug, side,
                    f"REFUSING GTC {side} {market_slug} @ {yes_px}: fresh book read FAILED "
                    f"({safe_exc(exc)}), so the crossing check cannot run. A guard that cannot "
                    f"run refuses. {_g}")
                return None
            _md = book.get("marketData") if isinstance(book, dict) else None
            bid, ask, _tob = touch_from_md(_md)
            # The guard's own book, taped before any comparison. `refused` is
            # re-taped by whichever branch below actually refuses; this write is the pass case.
            # ONE clock read for the whole guard run: the age on the tape and the epoch the witness
            # is compared against come from the SAME `now`.
            _now = time.time()
            _guard_age_s = transact_age_s(
                (_md or {}).get("transactTime") if isinstance(_md, dict) else None, _now)
            _rest_ts = None if _guard_age_s is None else _now - _guard_age_s
            _g = self._tape_guard_book(market_slug, bid, ask, _guard_age_s, "rest_fresh",
                                       refused=False)
        _ws_over = False        # did a newer WS witness override a would-cross REST refusal?
        if side in ("sell", "sell_long"):
            if bid is None:
                self.last_guard_book[market_slug]["refused"] = True
                self._guard_refused(
                    market_slug, side,
                    f"REFUSING GTC sell {market_slug} @ {yes_px}: best bid UNREADABLE, so the "
                    f"crossing check cannot run. A guard that cannot run refuses. {_g}")
                return None
            if yes_px <= bid:
                _newer = self._newer_witness_clears(side, yes_px, _rest_ts, ws_touch, post_only)
                if _newer is None:
                    self.last_guard_book[market_slug]["refused"] = True
                    self._guard_refused(
                        market_slug, side,
                        f"REFUSING GTC sell {market_slug} @ {yes_px}: best bid is {bid}, so this "
                        f"would CROSS and execute as a taker. Price behind the touch. {_g}")
                    return None
                _ws_over = True
                _g = self._tape_guard_book(
                    market_slug, bid, ask, _guard_age_s, "ws_over_stale_rest", refused=False,
                    ws=(ws_touch[0], ws_touch[1], max(0.0, _now - ws_touch[2])))
                self._guard_override(
                    market_slug, side,
                    f"GTC sell {market_slug} @ {yes_px}: REST bid {bid} would CROSS but is STALE "
                    f"— the caller's WS bid {ws_touch[0]} is {_newer:.0f} s newer and does not "
                    f"cross; placing post_only. {_g}")
            # ⛔ THE PRICE STAYS IN YES SPACE. Do NOT complement it. [PROVEN 2026-07-28 on four real
            # orders — the private design notes] For BUY_SHORT the venue
            # treats `price` as a YES-space SELL limit; `Decimal(1) - yes_px` made an ask intended at
            # 0.853 go out as "sell YES at anything down to 0.147". It hid because every other
            # BUY_SHORT here is AGGRESSIVE, and a sell limit far too LOW still fills at the touch.
            # ⛔ `sell_long` IS A DIFFERENT ORDER, not a spelling of `sell` — that claim is
            # `[UNPROVEN]` and is contradicted by `.claude/skills/venue-reference/SKILL.md`
            # § intents; both readings are in this method's docstring § `side="sell_long"`.
            # ⚠️ PRICE SPACE and the crossing guard are the same on both; only the intent differs,
            # and REDUCE-ONLY is the caller's obligation (no `reduceOnly` payload field).
            intent = ("ORDER_INTENT_SELL_LONG" if side == "sell_long"
                      else "ORDER_INTENT_BUY_SHORT")
            wire_px = yes_px
        else:
            if ask is None:
                self.last_guard_book[market_slug]["refused"] = True
                self._guard_refused(
                    market_slug, side,
                    f"REFUSING GTC buy {market_slug} @ {yes_px}: best ask UNREADABLE, so the "
                    f"crossing check cannot run. A guard that cannot run refuses. {_g}")
                return None
            if yes_px >= ask:
                _newer = self._newer_witness_clears(side, yes_px, _rest_ts, ws_touch, post_only)
                if _newer is None:
                    self.last_guard_book[market_slug]["refused"] = True
                    self._guard_refused(
                        market_slug, side,
                        f"REFUSING GTC buy {market_slug} @ {yes_px}: best ask is {ask}, so this "
                        f"would CROSS and execute as a taker. Price behind the touch. {_g}")
                    return None
                _ws_over = True
                _g = self._tape_guard_book(
                    market_slug, bid, ask, _guard_age_s, "ws_over_stale_rest", refused=False,
                    ws=(ws_touch[0], ws_touch[1], max(0.0, _now - ws_touch[2])))
                self._guard_override(
                    market_slug, side,
                    f"GTC buy {market_slug} @ {yes_px}: REST ask {ask} would CROSS but is STALE "
                    f"— the caller's WS ask {ws_touch[1]} is {_newer:.0f} s newer and does not "
                    f"cross; placing post_only. {_g}")
            intent = "ORDER_INTENT_BUY_LONG"
        # The guard PASSED: if this (slug, side) was refusing, page the clear exactly once.
        # Log level only — nothing below reads the state [2026-09-03].
        self._guard_cleared(market_slug, side, _g)
        if not _ws_over:
            # A placement the REST book cleared on its own ends the override state, so the next
            # entry into it says so again.
            self._guard_override_reset(market_slug, side)
        tag = "[DRY RUN] " if self._dry_run else ""
        log.info(f"{tag}GTC(US)  slug={market_slug}  {side}  "
                 f"leg={'short' if side == 'sell' else 'long'}  "
                 f"price={yes_px}  wire={wire_px}  size={size}  "
                 f"post_only={post_only}  "
                 f"{('gtd=' + good_till_time + '  ') if good_till_time is not None else ''}"
                 f"{label}")
        if self._dry_run:
            return {"status": "dry_run", "market_slug": market_slug, "price": price, "size": size}
        body: dict = {
            "marketSlug": market_slug,
            "intent": intent,
            "type": "ORDER_TYPE_LIMIT",
            # `str` of a value already quantized to the 4dp grid — NOT a second `f"{x:.4f}"`, which
            # would be a rounding the guard never saw.
            "price": {"value": str(wire_px), "currency": "USD"},
            # ⛔ The `int(round(size))` cast is RETIRED [box probe 2026-08-06]: it silently reshaped
            # a 12.80 close into 13 — the overshoot-through-flat class. Integral sizes still ship as
            # int (the maker's proven payload); a fractional size ships as the exact STRING of its
            # Decimal, never a float relaunder.
            "quantity": wire_qty,
            "tif": ("TIME_IN_FORCE_GOOD_TILL_CANCEL" if good_till_time is None
                    else "TIME_IN_FORCE_GOOD_TILL_DATE"),
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            # NOT synchronousExecution: a resting order has no terminal state to wait for.
        }
        if good_till_time is not None:
            # OMITTED (not None, not "") when unset — same rule as `participateDontInitiate` below:
            # omission is the only shape with a proven track record, and it keeps the GTC payload
            # byte-identical to every order this repo has ever sent.
            body["goodTillTime"] = good_till_time
        if post_only:
            # Omitted (not False) when unset: every real order ever sent omitted it, and the venue
            # SILENTLY ACCEPTS unknown/false-y fields. ⚠️ Enforcement semantics are the PROVEN ones,
            # not the documented ones: a would-cross post-only order RESTS (it is not rejected).
            body["participateDontInitiate"] = True
        # ⛔ INSTRUMENTATION ONLY [I2]: the span brackets the create call and nothing else, so the
        # number is send → ack and not the guard's book read above. `perf_counter` because a wall
        # clock can step; recorded ONLY on the acked path (a raise is not an ack).
        _ack_t0 = time.perf_counter()
        try:
            _resp = await self._sdk.orders.create(body)
            _ack_ms = (time.perf_counter() - _ack_t0) * 1000.0
        except Exception as exc:
            # Same rule as place_limit_fok: an error is NOT an outcome. We do not know whether the
            # order rested, so the caller must treat this as "unknown" and sweep open orders.
            log.error(f"PolyUSClient GTC order failed for {market_slug}: {exc}")
            raise
        # ⛔ STORED OUTSIDE THE `try:`. A raise from this line
        # inside the block would be logged and re-raised as a CREATE FAILURE, so an order that
        # really rested would come back to `_place` as an orphan suspect — an instrumentation
        # write must never be able to reclassify a successful ack.
        self.last_ack_lag_ms[market_slug] = _ack_ms
        return _resp

    @staticmethod
    def _order_verdict(exc: Exception) -> str:
        """`not_found` | `banned` | `error` for order reads/cancels [belief-recovery v5 §2].

        ⛔ The SDK raises NotFoundError for ANY 404 — a renamed route or base-URL drift
        included — and its message degrades to the bare reason-phrase when the response
        body is not JSON. So the venue's ORDER-SCOPED answer is distinguished by a
        STRUCTURED body: NotFoundError + dict body ⇒ `not_found`; anything else ⇒
        `error`, never terminal. A substring never makes this decision."""
        global _NOT_FOUND_BODY_LOGGED
        # `banned`: the BOX budget refused the request in `venue_budget.try_take` — nothing left
        # the box, so it is not a venue answer. Checked before the
        # SDK import: the verdict must not depend on the SDK being present.
        if isinstance(exc, venue_budget.VenueRefused):
            return "banned"
        try:
            from polymarket_us import NotFoundError
        except ImportError:                        # SDK absent (some test envs)
            return "error"
        global _UNSTRUCTURED_NF_LOGGED
        if (isinstance(exc, NotFoundError)
                and not isinstance(getattr(exc, "body", None), dict)
                and "order not found" in str(exc).lower()
                and not _UNSTRUCTURED_NF_LOGGED):
            _UNSTRUCTURED_NF_LOGGED = True
            log.warning(f"first UNSTRUCTURED 'order not found' 404 body captured: "
                        f"type={type(getattr(exc, 'body', None)).__name__} "
                        f"repr={getattr(exc, 'body', None)!r} — if this is the venue's "
                        f"real shape, the structured not_found verdict is UNREACHABLE and "
                        f"the probe chain cannot arm (the C1 negative answer).")
        if isinstance(exc, NotFoundError) and isinstance(getattr(exc, "body", None), dict):
            if not _NOT_FOUND_BODY_LOGGED:
                # [v5 addendum §2] Until a real structured-404 dict body is captured, the not_found
                # verdict is treated as possibly-unreachable and retirement rides the cancel-probe
                # chain. Once per process, WARNING so it survives log rotation triage.
                _NOT_FOUND_BODY_LOGGED = True
                log.warning(f"first structured not_found body captured: "
                            f"type={type(exc.body).__name__} repr={exc.body!r}")
            return "not_found"
        return "error"

    async def get_order_ex(self, order_id: str) -> tuple[Optional[dict], str]:
        """`get_order` with a TYPED verdict: (body, "ok"|"not_found"|"error").

        `not_found` is the venue's final word about THIS order (structured body);
        `error` covers transport failures AND bare-reason-phrase 404s (route drift) —
        callers must treat only `not_found` as potentially terminal, and only for
        orders old enough to be past the create-lag window (the venue's own order
        store lags creates by ~60-90s)."""
        try:
            body = await self._sdk.orders.retrieve(order_id)
            return body, "ok"
        except Exception as exc:
            verdict = self._order_verdict(exc)
            # ⛔ LOG LEVEL is decided by the ROUTINE-NESS of the message, not by the verdict: the
            # structured dict body was first observed live 2026-08-24. Gating INFO on
            # verdict=="not_found" alone made every routine order-scoped 404 a WARNING, which the
            # logger mirrors to Discord every 30s per distinct order id. The substring decides the
            # LEVEL only; the verdict stays structured-body-only.
            routine = verdict == "not_found" or "order not found" in str(exc).lower()
            level = log.info if routine else log.warning
            level(f"PolyUSClient get_order_ex {verdict} for {order_id}: {exc}")
            return None, verdict

    async def cancel_order_ex(self, order_id: str, market_slug: str) -> tuple[bool, str]:
        """`cancel_order` with the same typed verdict — the belief-recovery cancel-probe
        retires an order ONLY on a structured `not_found` answer; a bool False that
        collapses not-found/502/auth into one value cannot make that decision."""
        if self._dry_run:
            log.info(f"[DRY RUN] Poly cancel {order_id}")
            return True, "ok"
        try:
            await self._sdk.orders.cancel(order_id, {"marketSlug": market_slug})
            return True, "ok"
        except Exception as exc:
            verdict = self._order_verdict(exc)
            level = log.info if verdict == "not_found" else log.error
            level(f"PolyUSClient cancel_ex {verdict} for {order_id} ({market_slug}): {exc}")
            return False, verdict

    async def cancel_order(self, order_id: str, market_slug: str) -> bool:
        """Cancel a resting Poly order. True if the venue accepted the cancel.

        `CancelOrderParams` requires `marketSlug` alongside the id (SDK types/orders.py). DRY
        short-circuits. A failure is logged and returned False rather than raised — cancel runs in
        cleanup paths where a raise would abort the remaining cancels."""
        if self._dry_run:
            log.info(f"[DRY RUN] Poly cancel {order_id}")
            return True
        try:
            await self._sdk.orders.cancel(order_id, {"marketSlug": market_slug})
            return True
        except Exception as exc:
            log.error(f"PolyUSClient cancel failed for {order_id} ({market_slug}): {exc}")
            return False

    async def get_open_orders(self, slugs: Optional[Sequence[str]] = None, *,
                              strict: bool = False) -> list[dict]:
        """Resting Poly orders — account-wide, or SERVER-SIDE scoped to `slugs` when given.

        Used to sweep strays whose create-response was lost. Scoping at the source [M0.5b-a]: the
        envelope carries no pagination token, so a silent server-side cap cannot be ruled out on an
        account-wide read. Callers that hunt strays ANYWHERE (the preflights) stay account-wide.

        RAISES on an unreadable shape rather than returning [] — "no open orders" and "we could not
        tell" must never be the same answer. ⚠️ ONE EXCEPTION, and `strict` closes it [review C1]:
        an EMPTY-OBJECT 200 (`{}`, no `orders` key at all) reads as no-orders by default, which the
        sweep wants — but a degraded venue serving `{}` would then read as CONFIRMED FLAT. A caller
        whose decision needs proof of flatness passes `strict=True` and gets a raise instead."""
        params = {"slugs": [str(s) for s in slugs]} if slugs else None
        resp = await self._sdk.orders.list(params) if params else await self._sdk.orders.list()
        orders = resp.get("orders") if isinstance(resp, dict) else None
        if orders is None and isinstance(resp, dict) and not resp:
            if strict:
                raise RuntimeError(
                    "Poly open orders: EMPTY envelope ({}) — refusing to report 'no open "
                    "orders' to a strict caller")
            return []
        if not isinstance(orders, list):
            raise RuntimeError(
                f"Poly open orders: unrecognised shape (keys={sorted(resp) if isinstance(resp, dict) else type(resp)}) "
                f"— refusing to report 'no open orders'")
        return [o for o in orders if isinstance(o, dict)]

    async def get_activities_page(self, cursor: str = "") -> tuple[list[dict], str]:
        """ONE page of the trade-activities ledger — the recovery walk's read unit
        [belief-recovery v5 §5]. Returns (activities, nextCursor); "" cursor = page 1
        (newest first). ⛔ Deliberately NOT `scripts.capital_ledger.page_poly_activities`:
        that helper loops pages with sleeps, and the walk's budget is ONE request per quote
        cycle — pagination policy belongs to the caller. Raises on transport failure (the
        walk treats an exception as no-coverage, never as an empty ledger)."""
        params: dict = {"types": ["ACTIVITY_TYPE_TRADE"], "limit": 50}
        if cursor:
            params["cursor"] = cursor
        resp = await self._sdk.portfolio.activities(params)
        if not isinstance(resp, dict):
            raise RuntimeError(
                f"Poly activities: expected an object, got {type(resp).__name__} — "
                f"refusing to read it as an empty ledger")
        acts = resp.get("activities") or []
        return ([a for a in acts if isinstance(a, dict)],
                str(resp.get("nextCursor") or ""))

    async def get_order(self, order_id: str) -> Optional[dict]:
        """Read one order back, including its realised commission.

        This is how the MAKER REBATE gets verified: Poly's docs claim 0.0125*C*p*(1-p) per fill, but
        that is [UNVERIFIED, docs-sourced]. `commissionNotionalTotalCollected` here is the
        authoritative number (it pinned the Poly TAKER fee against 9 real fills); a rebate should
        surface as a NEGATIVE commission, and if it never appears the docs are wrong and Poly maker
        is merely fee-free, not paid.

        Via the SDK's `orders.retrieve`. Response shape [VERIFIED live 2026-07-26]:
        {"order": {..., "cumQuantity": int, "state": …, "commissionNotionalTotalCollected":
        {"value": "0.0000", "currency": "USD"}, "avgPx": {...}}}."""
        try:
            return await self._sdk.orders.retrieve(order_id)
        except Exception as exc:
            # "Order not found" is a ROUTINE venue answer (post-only reject / purge of a cancelled
            # order) and every caller already escalates loudly itself if the miss matters. WARNING
            # here mirrored each poll to the Discord alerts channel.
            # "order not found" NARROWLY: a bare "Not Found" (reason-phrase of ANY 404 — e.g. a
            # renamed route breaking every read) must stay WARNING; only the venue's order-scoped
            # answer is routine.
            level = log.info if "order not found" in str(exc).lower() else log.warning
            level(f"PolyUSClient get_order failed for {order_id}: {exc}")
            return None

    async def sell_back(
        self, token: str, size: float, label: str = ""
    ) -> tuple[Optional[float], float]:
        """
        Emergency sell of a stranded position at the best available price.

        Returns **(vwap_price, sold_qty)** — `(None, 0.0)` if nothing sold.

        Long leg → SELL_LONG at the best bid. Short leg ("<slug>::short") → SELL_SHORT at the best
        yes ASK, **in yes space** (it buys the yes side back). ⚠️ NOT `1 − best ask`: that made a
        short flatten impossible, not merely mispriced. Then retries <n> through it for WHATEVER IS
        STILL UNSOLD — −<n> on a long, +<n> on a short.

        ⚠️ NOT a fill-or-kill sell despite the tif (Poly rewrites FOK → IOC), so a sell **CAN
        PARTIAL-FILL** — which is why this returns the sold QUANTITY as well as a price.

        The reported price is the qty-weighted mean of the LIMITS transacted at, conservative in
        BOTH directions but in DIFFERENT ones: on a LONG disposal we RECEIVE at-least this, on a
        SHORT disposal we PAY at-most this. Callers must not share one formula.
        """
        market_slug, is_short = parse_token(token)
        side = "short" if is_short else "long"
        intent = "ORDER_INTENT_SELL_SHORT" if is_short else "ORDER_INTENT_SELL_LONG"
        tag = "[DRY RUN] " if self._dry_run else ""
        log.warning(f"{tag}SELL-BACK(US)  slug={market_slug}  side={side}  size={size:.2f}  {label}")
        if self._dry_run:
            return 0.50, float(size)

        try:
            # fresh=True: the unwind price MUST come from the live book, not Polymarket's
            # 30s Cloudflare cache — a stale price would let the FOK miss the real book and
            # strand the leg (the exact failure this unwind prevents).
            book = await self._fetch_book(market_slug, fresh=True)
            # Real response nests the book under "marketData" (like bbo) — the
            # SDK's MarketBook type wrongly claims top-level "bids". Reading the
            # wrong path returned [] every time → "no bids" → unwind always
            # failed and stranded the leg. Read marketData.{bids,offers}.
            md = book.get("marketData", {}) if isinstance(book, dict) else {}
            if is_short:
                # Disposing of a short BUYS the yes side back, so it crosses the LONG offers and the
                # limit is the best (lowest) yes ask — IN YES SPACE, NOT the complement. [PROVEN live: a
                # SELL_SHORT sent in yes space against the yes ask FILLED.]
                # ⛔ As `1.0 - min(ask_prices)` this made the short flatten IMPOSSIBLE rather than
                # merely mispriced: it sent 0.147 when yes was offered at 0.853, so it could never
                # fill. This is the strand-unwind path.
                offers = md.get("offers", []) if isinstance(md, dict) else []
                ask_prices = [
                    _amount_to_float(lvl.get("px")) for lvl in offers
                    if _amount_to_float(lvl.get("px")) is not None
                ]
                if not ask_prices:
                    log.error(f"No offers available to sell-back (short) {market_slug}")
                    return None, 0.0
                best_price = min(ask_prices)
            else:
                bids = md.get("bids", []) if isinstance(md, dict) else []
                bid_prices = [
                    _amount_to_float(lvl.get("px")) for lvl in bids
                    if _amount_to_float(lvl.get("px")) is not None
                ]
                if not bid_prices:
                    log.error(f"No bids available to sell-back {market_slug}")
                    return None, 0.0
                best_price = max(bid_prices)
        except Exception as exc:
            log.error(f"PolyUSClient.sell_back book fetch failed for {market_slug}: {exc}")
            return None, 0.0

        async def _try_sell(price: float, qty: float) -> float:
            """Sell `qty` at `price`; return the qty ACTUALLY sold (0.0 on miss/error).

            Sizing per-attempt (not off the closure) is what stops the retry overselling.
            Reads the real fill via order_filled_qty — NOT order_is_filled, which demands a
            FULL fill and so reports a genuine partial sale as "nothing happened"."""
            try:
                resp = await self._sdk.orders.create({
                    "marketSlug": market_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{price:.4f}", "currency": "USD"},
                    "quantity": int(round(qty)),
                    # IOC (Poly rewrites FOK→IOC anyway) — and IOC is right for an unwind:
                    # selling PART of a stranded leg beats selling none. The caller strands only
                    # the unsold remainder.
                    "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
                    "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
                    "synchronousExecution": True,
                })
                # synchronousExecution → the response is authoritative for the real fill.
                sold = order_filled_qty(resp)
                # A response can report terminal FILLED without echoing cumQuantity. FILLED means the
                # WHOLE requested qty filled by definition, so trust it rather than read 0.
                # Direction matters: UNDER-reporting a sale strands shares we already sold and
                # re-offers them on the retry.
                if sold <= 0 and order_is_filled(resp):
                    sold = float(int(round(qty)))
                return sold
            except Exception as exc:
                # ⛔ AN ERROR IS NOT AN OUTCOME — the same rule `place_limit_fok` states for the BUY
                # leg. `return 0.0` told the retry loop those shares were still HELD, so it
                # re-offered them <n> lower; `synchronousExecution` blocks ~61ms server-side, so a
                # timeout/502/reset lands INSIDE the fill window (reproduced: NET SHORT 100 booked
                # as a clean flatten). Raising is the only way to say "we do not know" —
                # `_unwind_poly_excess` maps it to a full strand → pause → alert → reconciler, at
                # the cost of an operator `touch logs/resume`. A genuine IOC kill is NOT this path.
                log.error(f"Sell-back attempt at {price:.4f} FAILED — outcome UNKNOWN: {safe_exc(exc)}")
                raise

        # Best price first, then <n> THROUGH it — each attempt sized to what is STILL HELD.
        # ⛔ "Through" is a different DIRECTION on each side: a LONG disposal concedes by accepting
        # LESS (−<n>), a SHORT disposal by paying MORE (+<n>). One `best_price - 0.02` ladder moved
        # the short retry further from marketable.
        # ⚠️ The clamp must never land BEHIND the first attempt, so clamp to the wire bound and then
        # take the more aggressive of (clamped, first attempt).
        if is_short:
            retry_price = max(min(0.99, best_price + 0.02), best_price)
        else:
            retry_price = min(max(0.01, best_price - 0.02), best_price)
        remaining, sold_total, proceeds = float(size), 0.0, 0.0
        for i, price in enumerate((best_price, retry_price)):
            if remaining < 1:
                break
            if i:
                log.warning(
                    # "discount" is only right on a long; a short concedes by paying MORE.
                    f"Retrying sell-back(US) for the unsold {remaining:.0f} at "
                    f"{'premium' if is_short else 'discount'}: {price:.4f}"
                )
            filled = await _try_sell(price, remaining)
            sold_total += filled
            proceeds += price * filled
            remaining -= filled

        if sold_total <= 0:
            log.error(f"Sell-back(US) failed for {market_slug} ({side}) — leg remains stranded")
            return None, 0.0
        vwap = proceeds / sold_total
        if remaining >= 1:
            # Sold SOME. The caller must strand only `remaining`, and book the real proceeds.
            log.critical(
                f"Sell-back(US) PARTIAL for {market_slug} ({side}): sold {sold_total:.0f}/{size:.0f} "
                f"@ ~{vwap:.4f} — {remaining:.0f} still HELD"
            )
        else:
            log.info(f"Sell-back(US) succeeded for {market_slug} ({side}) at {vwap:.4f}")
        return vwap, sold_total


#: Where `collector_reader` looks for the collector key when the `PMB_COLLECTOR_*` variables are
#: not set: the same file the collector units already load (`EnvironmentFile=.env.collectors`),
#: overridable with `PMB_COLLECTOR_ENV_FILE`. Its VALUES are never logged.
COLLECTOR_ENV_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    ".env.collectors")

#: `.env.collectors` name → the `collector_reader` argument it feeds. The file uses the SAME
#: variable names as the maker's own env (different values), which is why the process-env route
#: gets its own `PMB_COLLECTOR_*` names: one process can then carry both keys.
_COLLECTOR_FILE_NAMES = {
    "POLYMARKET_US_KEY_ID": "key_id",
    "POLYMARKET_US_SECRET_KEY": "secret_key",
    "POLY_SOURCE_IP": "source_ip",
}


def _collector_env_file(path: str) -> Optional[dict[str, str]]:
    """The three collector fields read out of a KEY=VALUE env file; {} if it cannot be read, and
    **None if one of the three lines is MALFORMED** — the caller then builds no reader at all.

    Deliberately not a general dotenv parser: `KEY=VALUE` lines, `#` comments, optional surrounding
    quotes, `export ` prefix tolerated. ⛔ A value with INNER WHITESPACE or a `#` (a shell inline
    comment, which `EnvironmentFile=` does NOT strip) is MALFORMED, not a credential: guessing
    where such a value ends would authenticate on half a key, and half a key trips the maker's own
    read path with a 401 instead of reading a book. ⛔ NEVER LOG A VALUE OR A FRAGMENT OF ONE from
    here — the warning names the path and the LINE NUMBER only.
    """
    out: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for n, line in enumerate(fh, start=1):
                line = line.strip()
                if line.startswith("export "):
                    line = line[len("export "):].strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, _, raw = line.partition("=")
                field = _COLLECTOR_FILE_NAMES.get(name.strip())
                if not field:
                    continue
                value = raw.strip().strip("'\"").strip()
                if not value or "#" in value or any(c.isspace() for c in value):
                    log.warning(f"collector reader: {path} line {n} is malformed (inner "
                                f"whitespace or '#') — public reads stay on the maker key")
                    return None
                out[field] = value
    except OSError as exc:
        log.debug(f"collector reader: {path} unreadable ({exc})")
        return {}
    return out


def collector_reader(*, prio: int = venue_budget.PRIO_OPERATOR,
                     allow_during_ban: bool = False) -> Optional[PolyUSClient]:
    """A client on the collector source route for public reads, or None without credentials.

    Public GETs use the unauthenticated gateway; the collector source route keeps those reads off
    the maker's source route. The key alone adds no public quota. Private requests (place, cancel,
    open orders, positions, activities) stay on the maker client. Wire this as
    `client.public_reader`.

    Credentials come from `PMB_COLLECTOR_KEY_ID` / `PMB_COLLECTOR_SECRET_KEY` /
    `PMB_COLLECTOR_SOURCE_IP`, falling back per field to the env FILE named by
    `PMB_COLLECTOR_ENV_FILE` (default `COLLECTOR_ENV_FILE`). ⛔ ALL THREE ARE REQUIRED: the source
    IP also selects the budget bucket, and a reader on the maker's IP would share the maker's
    bucket. Missing any → ONE warning and None, i.e. exactly today's
    maker-source behaviour; the caller prints which source its public reads use.

    `PRIO_OPERATOR`, never `PRIO_MAKER`: the maker class belongs to the maker KEY's client, while
    this one draws on the collector bucket, where OPERATOR outranks every collector and takes no
    collector lease. Ordinary reads obey a box ban. Pass `allow_during_ban=True` only for an
    explicit recovery reader; those requests remain metered.
    """
    # PMB_COLLECTOR_KEY_ID / _SECRET_KEY / _SOURCE_IP → key_id / secret_key / source_ip.
    fields = {field: os.environ.get(f"PMB_COLLECTOR_{field.upper()}", "").strip()
              for field in _COLLECTOR_FILE_NAMES.values()}
    if not all(fields.values()):
        path = os.environ.get("PMB_COLLECTOR_ENV_FILE", "").strip() or COLLECTOR_ENV_FILE
        from_file = _collector_env_file(path)
        if from_file is None:      # malformed: warned with the line number, build nothing
            return None
        for field, value in fields.items():
            if not value:
                fields[field] = from_file.get(field, "")
    if not all(fields.values()):
        log.warning("collector reader: no collector credentials — public reads stay on the maker key")
        return None
    return PolyUSClient(prio=prio, source_ip=fields["source_ip"],
                        api_key_id=fields["key_id"], api_secret_key=fields["secret_key"],
                        allow_during_ban=allow_during_ban)
