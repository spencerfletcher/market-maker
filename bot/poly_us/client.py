"""
bot/poly_us/client.py
─────────────────────
Authenticated Polymarket US client (polymarket-us SDK) with dry-run support.

Polymarket US is a separate, CFTC-regulated exchange from global Polymarket:
  - API-key auth (key_id + secret_key), NOT wallet/EIP-712 signing
  - Custodial balances (no Polygon wallet)
  - Markets identified by slug (e.g. "atc-fwc-mex-rsa-2026-06-11-mex"), not token IDs

This client presents the same surface the rest of the bot expects from
PolymarketClient (get_usdc_balance / get_best_ask / place_limit_fok), so the
matcher, arb math, and executor can treat either venue uniformly. The opaque
"market_slug" string carried in MarketPair.token_* fields is interpreted here.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from typing import Optional, Sequence

from polymarket_us import AsyncPolymarketUS

from bot.core import config
from bot.core.money import complement, from_float, parse_wire
from bot.core.logger import get_logger
from bot.core.redact import safe_exc
from bot.poly_us.sides import parse_token

log = get_logger(__name__)

# One-shot capture flags: the first STRUCTURED not_found body goes on record (the original
# specimen recovered before its body could be read) — and so does the first UNSTRUCTURED
# "order not found" body, because a string body would mean the not_found verdict is
# unreachable and the whole belief-recovery chain cannot arm. That is a NEGATIVE answer to a
# design question, and it must not be discoverable only by grepping raw logs.
_NOT_FOUND_BODY_LOGGED = False
_UNSTRUCTURED_NF_LOGGED = False


def _amount_to_float(amount: Optional[dict]) -> Optional[float]:
    """Parse a polymarket-us Amount ({'value': str, 'currency': 'USD'}) to float."""
    if not amount:
        return None
    try:
        return float(amount["value"])
    except (KeyError, TypeError, ValueError):
        return None


_WIRE_PRICE_GRID = Decimal("0.0001")   # every Poly price field ships as a 4dp decimal string


def _wire_quantity(size: "float | int | Decimal") -> int | str:
    """The order-create quantity, exactly. Integral → int (the maker's proven wire shape,
    byte-identical to every real order ever sent); fractional → the fixed-point STRING of
    the Decimal. Probed against the venue's own preview: it echoes fractional quantities back
    unrounded in either form — ⚠️ proven under the BUY_LONG intent only, and the string form is
    the shape the probe actually proved. Replaces `int(round(size))`, which silently reshaped
    fractional closes upward (12.80 → 13) and so could overshoot straight THROUGH flat.

    `format(q, "f")` never emits exponent notation — `str(Decimal("1E-7"))` would ship
    "1E-7" on the wire.

    First-use protocol: one real BUY-reducing-a-SHORT close (that ships BUY_LONG, the
    proven intent) with held ≥ qty + 1, before size-N or sell-side fractional trust."""
    q = size if isinstance(size, Decimal) else Decimal(str(size))
    if not q.is_finite() or q <= 0:
        # The retired int(round()) RAISED on NaN/Infinity; format() would ship the
        # literal string. Programmatic callers bypass parse_close's regex, so guard here.
        raise PreSendRefusal(f"_wire_quantity: {size!r} is not a positive finite quantity")
    return int(q) if q == q.to_integral_value() else format(q, "f")


class PreSendRefusal(ValueError):
    """A pre-send caller-bug refusal from an order method: raised BEFORE any venue call, so
    the caller KNOWS nothing was placed (unlike a generic exception, where the order may
    rest). `bot/poly_us/maker._place` clears its durable intent on this type only — a
    phantom intent otherwise reads as `maybe_live_orders` and blocks a sibling lane's start
    from starting. FOUR raise sites, all in the GTC path: unknown side, `::short`
    token, non-finite price (`_wire_price`), non-positive/non-finite quantity
    (`_wire_quantity`). Do not raise it after the venue call is issued."""


def _wire_price(price: Decimal | float) -> Decimal:
    """A caller's price snapped to the 4dp WIRE format — the ONE rounding, not a tick check.

    **A `Decimal` caller price quantizes DIRECTLY — no float round-trip.** Routing a
    Decimal through `from_float` re-parses it via `repr(float(x))`, collapsing the exact value onto
    the nearest shortest-repr double before the quantize — precisely the laundering the repo's
    Decimal rule forbids. The float path below is unchanged byte-for-byte for every FINITE
    float; non-finite inputs now raise the TYPED refusal where they previously died opaquely
    (Infinity: InvalidOperation at the quantize; NaN: InvalidOperation at the crossing compare).

    ⚠️ **4dp is the transport, NOT the grid this venue trades on.** The tradeable tick is
    per-market — 0.01 on ~88% of Poly US, 0.005 and 0.001 elsewhere, and it varies *within* a series
    (see `get_market_tick`). This function does not know the tick and does not snap to it, so a price
    it returns can be perfectly 4dp and still OFF-tick; Poly then FLOORS the limit rather than
    rejecting it (verified against `/v1/order/preview`), which is a silently different price than
    the one the crossing guard cleared. **On-tick-ness is the CALLER's job** — a caller that can
    place must refuse `price % tick != 0` before it ever gets here.

    What this rounding IS for: the wire carries a 4dp decimal string, so 0.44996 is 0.4500 to the
    venue no matter what the caller meant. Rounding here, once, before anything reads the number, is
    what lets the crossing guard compare the value that will really be placed — guarding on the raw
    float and formatting separately is two roundings of one price, and they disagree exactly at the
    touch: a buy at 0.44996 cleared a 0.45 ask as "behind it" and then shipped as 0.4500, AT it.

    `from_float` (shortest round-tripping repr) recovers the decimal the caller intended before the
    quantize, so a price computed in float lands on the grid point it reads as.

    **Versus the old `f"{price:.4f}"`, precisely** (measured exhaustively): byte-identical for every
    price expressible on any tick ≥ 0.0001 (0 mismatches over all 9,999 k/10000), which is every
    price this method can legally place. It differs by ONE CENTICENT on exact half-centicent inputs
    — 4,988 of the 10,000 values 0.00005 … 0.99995 — and the reason is NOT that half-even "matches
    format's tie rule". `format` rounds the exact BINARY double, which is essentially never a decimal
    tie, so its own tie rule almost never fires and the result is decided by which side of the tie
    the binary value happens to sit. It is `from_float`'s shortest-repr recovery that moves the value
    ONTO the tie (0.12345 → Decimal("0.12345"), exactly), where ROUND_HALF_EVEN then decides — and
    half-even's answer disagrees with the binary coin-flip about half the time. Both are off-tick
    inputs a caller should never send; the point is that the rule is now a stated one."""
    d = price if isinstance(price, Decimal) else from_float(price)
    if not d.is_finite():
        # No literal ever shipped — and note that the obvious justifications for this guard are
        # both wrong: Infinity raises InvalidOperation at the quantize on the next line, and NaN
        # raises InvalidOperation at the crossing compare. What this guard buys is the TYPE:
        # an opaque InvalidOperation falls into `_place`'s generic except and KEEPS the
        # durable intent (phantom `maybe_live_orders`); the typed refusal proves nothing was
        # sent, so the intent is cleared. Mirrors `_wire_quantity`.
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

    THE touch parser: `place_limit_gtc`'s crossing guard and the probe tooling both read a book
    through this, so the number the guard refuses on and the number a report quotes are produced by
    one piece of code. (Promoted out of a probe, the way `get_market_tick` was — a second copy of a
    touch read is a second thing to get wrong, on the one path where being wrong places an order.)

    Each side is independently None when it is empty or unparseable, so a caller requires only the
    side it actually guards on. **None means "we could not tell", NEVER "nothing to cross"** — every
    placement guard must read it as a refusal.

    max over bid prices and min over ask prices, never `bids[0]`/`offers[0]`: the SDK's book is a
    bare list with no documented ordering, and every sibling reader (get_book_depth, sell_back,
    quote_from_md) already refuses to trust the order. The bid TOB qty is the SUM at the best price,
    not level-0's slice — it is the queue ahead of us.

    Decimal throughout, per the repo's money rule: this feeds a comparison against a wire price, and a
    float hop would launder binary error into exactly the tie cases the guard exists to catch. Never
    raises — it rides a money path, so a shape change fails closed to all-None."""
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
    fire-path read, so a malformed stats block can never break the quote). `*SetTime` ages reuse
    `transact_age_s` (same ns-ISO format as transactTime). `openInterest`/`sharesTraded` are bare
    numeric strings; `lastTradePx`/`notionalTraded` are {value,currency} Amounts.

    ⚠️ **UNITS — MEASURED, and do NOT infer them from the `currency: "USD"` tag.**
    · `sharesTraded` and a trade frame's `quantity` are SHARE counts (verified by isolating single
      trades between two market_data frames: Δ`sharesTraded`/`quantity` came out at exactly 1).
    · `notionalTraded` is in **CENTS, not dollars** — Δ`notionalTraded`/Δ`sharesTraded` came out at
      exactly 100×price, so the raw field reads 100× too large if you call it dollars.
    The `currency: "USD"` field is present on BOTH and is therefore not evidence of a dollar unit; a
    trade's `quantity` carries it while being a share count. This value is passed through UNCONVERTED
    (it is logging-only context — nothing computes with it), so any ANALYSIS of `poly_notional_traded`
    must divide by 100 before calling it dollars."""
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


# Public alias — the maker tapes these per quote cycle: the underscore would
# otherwise lie about external consumers, and a future "unused private" cleanup would
# break the tape with no test in this module to catch it.
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

    Split out of it so a caller ALREADY HOLDING a book can read the same quote without fetching
    again. That matters on Poly specifically: the limit is ~1 req/s sustained and over-limit is
    THROTTLED rather than rejected — a late, stale 200. A second fetch for data already in hand
    therefore doesn't just waste a request, it degrades the freshness of the reads around it, and
    the fire path shares that budget. A caller wanting both directions of one book reads the ask
    here and the bid via kalshi_arb._poly_exit_from_book, off a single fetch — which also makes
    the two describe one instant instead of two books a round-trip apart.

    No I/O, and no state — but NOT pure: _parse_book_stats stamps the stats ages off time.time(),
    so two calls on one book return different `oi_age_s`/`last_trade_age_s`. Compare two results
    only with the clock pinned. get_fill_quote owns the fetch and its fail-closed error path."""
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

    Per the SDK types, CreateOrderResponse is {"id", "executions": [Execution]}
    and Execution.order is an Order carrying both `state` (an OrderState enum)
    and fill quantities (`cumQuantity` / `quantity`). A fully-filled FOK reports
    state == "ORDER_STATE_FILLED" OR cumQuantity >= quantity. A killed/rejected/
    expired FOK reports neither — a non-None response alone does NOT mean filled.

    CAVEAT: this only works if the create response actually carries the terminal
    execution state. If orders are placed without `synchronousExecution`, the
    response can return before the FOK resolves (empty executions / NEW state)
    even though it fills async — in which case no parsing here can detect it.
    See place_limit_fok.
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

    Poly US coerces our FOK to IOC, so an order can PARTIAL-fill: the response
    carries the terminal execution with cumQuantity < quantity. Return the max
    cumQuantity seen across executions (it's cumulative/monotonic). 0 = no fill.
    Used to reconcile the hedge to the real fill instead of assuming all-or-none.

    ⚠️ The max() is LOAD-BEARING — `executions[0]` LIES. ✅ VERIFIED on a real partial fill:
    the FIRST execution's order carried `cumQuantity: 0` with the full quantity still shown as
    leaves, while the order had genuinely filled most of the way; only a LATER execution carried
    the true cum. Reading executions[0] (or trusting the first order object
    you find) reports "no fill" on a leg that filled — under poly_first that is a naked,
    UNRECORDED Poly position: no hedge booked, invisible to the exposure caps and the
    strand alert. Never "simplify" this to the first/last execution.

    The FOK coercion itself is also VERIFIED on a real order (see place_limit_fok): we ask
    for FILL_OR_KILL, the exchange echoes IMMEDIATE_OR_CANCEL and partial-fills. The venue
    docs claim FOK is honored — they are wrong.
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
# ends up on. Those differ, because Poly's names are {verb}_{POSITION} with the verb acting ON the
# position: `SELL_SHORT` DISPOSES of a short, so it ends up long while still concerning the SHORT
# position. The caller's `is_short` means "this is the ::short token", i.e. the position, so the
# position reading is the one the guard needs. (`endswith("_SHORT")` happened to give exactly this;
# the whitelist preserves it while making an UNRECOGNISED value read as no-information rather than
# silently as long.) Source: the SDK's OrderIntent Literal (polymarket_us/types/orders.py).
_INTENT_IS_SHORT: dict[str, bool] = {
    "ORDER_INTENT_BUY_LONG": False,
    "ORDER_INTENT_SELL_LONG": False,
    "ORDER_INTENT_BUY_SHORT": True,
    "ORDER_INTENT_SELL_SHORT": True,
}
# `poly_avg_fill_cost` prices OPENING legs. A disposal has no defined value under its contract
# ("cost per share of the side we bought"), so it is refused rather than silently returning a
# proceeds quantity with an inverted fee.
_OPENING_INTENTS = frozenset({"ORDER_INTENT_BUY_LONG", "ORDER_INTENT_BUY_SHORT"})


def poly_avg_fill_cost(resp: Optional[dict], *, is_short: bool) -> Optional[float]:
    """ACTUAL fee-inclusive cost per share from a Poly create, or None if unreadable.

    ⛔ `is_short` COMES FROM THE CALLER'S TOKEN, NOT FROM THE VENUE'S `intent` FIELD.

    `avgPx` is in YES space for both short intents, so a short's cost is the cost of the NO side,
    `(1 − avgPx) + fee`. The first version of this read `order["intent"]` and keyed off
    `endswith("_SHORT")`. That reopened the very bug this function was changed to fix: a MISSING
    `intent` resolves to the long formula, and below yes 0.5 that flips the booked flatten loss
    NEGATIVE — and the loss caps clamp a negative loss to zero, so a REAL loss becomes invisible to
    both of them. Above yes 0.5 the same mistake over-reports the loss by a large multiple. Either
    way the cap is reading a number with no relation to what happened.

    And `intent` CAN be absent: the only captured real create-response carries `intent` on the
    first execution and NOT on the max-cum execution this function actually reads. The SDK's
    `Order` type is `total=False`, so every field is optional.

    Both call sites hold `opp.poly_token` and `parse_token` is already imported there, so the
    answer is free and certain. When the venue DOES send a RECOGNISED `intent` that contradicts the
    caller, return None (CANNOT READ) rather than guess — the caller's fallback is the loud, correct
    short-space `opp.poly_ask`.

    ⚠️ `is_short` is REQUIRED, deliberately — it has no default. A default of False would make an
    omission at a short call site silent, which is the failure this parameter exists to prevent:
    the caller gets a mirrored price, no exception and no log. Requiring it makes the omission a
    TypeError at every call site, and the existing suite then catches it many ways over. The first
    version defaulted to False and pinned the wiring with a source-string count instead; hardcoding
    `is_short=False` at all four call sites — literally reinstating the bug on the majority of
    directions — tripped only that pin and no behavioural test.

    Poly reports both halves, so the Θ·p·(1−p) model isn't needed here:
        avgPx                            — the real VWAP (IOC can sweep levels)
        commissionNotionalTotalCollected — the real commission, for the ORDER
    ✅ VERIFIED against a real fill: dividing the order-level commission by cumQuantity reproduces
    the per-share cost the balance actually moved by.

    Reads the MAX-cumQuantity execution, not executions[0] — that one LIES (it reported
    cumQuantity 0 on an order that filled 255). Same rule as order_filled_qty.

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
        # ⛔ `avgPx` IS IN YES SPACE EVEN FOR A SHORT INTENT — so a short's cost is NOT `avgPx + fee`.
        # ✅ PROVEN on real fills: send a short intent at a wire price and the venue echoes an
        # `avgPx` in YES space, matching the yes-side touch it traded against — never the
        # complement of the price you sent.
        #
        # Opening a short buys the NO side, which costs `(1 − avgPx) + fee` per share. Returning
        # `avgPx + fee` overstates it by roughly `2·avgPx − 1`, which high on the yes side is a
        # several-fold overstatement and drives `guaranteed_profit` negative. Confirmed the honest
        # way: the settled cash movement per share matched `(1 − avgPx) + fee`, not `avgPx + fee`.
        #
        # The value returned is the effective per-share cost OF THE SIDE WE BOUGHT, in that side's
        # own space: yes-space for a long, short-space for a short. That is what `_record_hedge`
        # wants directly, and what `_unwind_poly_excess` complements once to reach yes space.
        # The venue's own `intent`, when present, must AGREE with the caller. A contradiction means
        # one of us is wrong about which side this fill is, and guessing either way books a real
        # position at a mirrored price — so refuse and let the caller take its explicit fallback.
        # WHITELIST, not a suffix test. An unrecognised intent is NO INFORMATION — the same state
        # as an absent one — and must not be read as a contradiction. A suffix test calls anything
        # unrecognised "long", so a proto3 zero value (`ORDER_INTENT_UNSPECIFIED`) or a renamed enum
        # would make EVERY short fill refuse and fall back to the detection price: optimistic, on
        # the majority of directions, with only a log line. Refuse only on a RECOGNISED
        # disagreement.
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
        # intent IS a contradiction. An earlier version instead flipped the fee sign and returned a
        # proceeds quantity under a cost-shaped name, with no signal to the caller — and pinned it
        # with a ZERO-commission fixture, the one value at which the sign is invisible. The fee
        # direction there was the author's model, never a measurement.
        # RECOGNISED and non-opening. An unrecognised intent is no-information (handled above) and
        # must not be read as a disposal — the same conflation the contradiction guard avoids.
        if intent in _INTENT_IS_SHORT and intent not in _OPENING_INTENTS:
            log.error(
                f"poly_avg_fill_cost: {intent!r} is a DISPOSAL, but this prices opening legs only. "
                f"Refusing rather than returning proceeds under a cost-shaped name.")
            return None
        fee = comm / cum
        best_cum, best = cum, ((1.0 - px) + fee) if is_short else (px + fee)
    return best


class PolyUSClient:
    """Polymarket US CLOB client with dry-run support. Async-native."""

    def __init__(self) -> None:
        self._dry_run = config.DRY_RUN
        if not self._dry_run and (
            not config.POLYMARKET_US_KEY_ID or not config.POLYMARKET_US_SECRET_KEY
        ):
            raise ValueError(
                "Polymarket US requires POLYMARKET_US_KEY_ID and "
                "POLYMARKET_US_SECRET_KEY (set them in .env, or enable DRY_RUN)"
            )
        self._sdk = AsyncPolymarketUS(
            key_id=config.POLYMARKET_US_KEY_ID or None,
            secret_key=config.POLYMARKET_US_SECRET_KEY or None,
        )
        log.info("Polymarket US client initialized")
        if self._dry_run:
            log.info("DRY RUN mode enabled — no real orders will be placed")

    async def close(self) -> None:
        await self._sdk.close()

    async def get_usdc_balance(self, force_real: bool = False) -> float:
        """Return free-to-trade USD balance (buyingPower). -1.0 on error.

        In DRY_RUN this returns a 9999 sentinel so the executor's pre-trade check
        always "affords" simulated trades. Pass force_real=True to bypass that and
        fetch the actual balance (e.g. for FBAR/tax logging) — it's a read-only
        query, safe in any mode.
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
        with nothing in the response saying so. It once fed `place_limit_gtc`'s crossing guard,
        which made that guard fail-OPEN on a stale-HIGH ask; the guard now reads
        `_fetch_book(fresh=True)` + `touch_from_md`, and every placement-side probe refuses this
        method for the same reason. Fine for reporting and diagnostics where half a minute does
        not matter. Anything a placement depends on must cache-bust."""
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
        and carrying the same ⛔ CDN-CACHED warning — see there. It was added for
        `place_limit_gtc`'s SELL-side crossing guard; that guard stopped using it once the
        30s-stale touch was understood, which leaves this method with **no caller**. Kept as the
        symmetric half of a two-method pair rather than deleted, but it is not a placement read:
        route anything that decides an order through `_fetch_book(fresh=True)` + `touch_from_md`.
        Same fail-to-None contract: an unreadable book must never resolve to a number a crossing
        check would then pass."""
        try:
            resp = await self._sdk.markets.bbo(market_slug)
            md = resp.get("marketData", {}) if isinstance(resp, dict) else {}
            return _amount_to_float(md.get("bestBid"))
        except Exception as exc:
            log.debug(f"PolyUSClient.get_best_bid({market_slug}): {exc}")
            return None

    async def get_market_tick(self, slug: str) -> Optional[Decimal]:
        """The market's minimum price increment (`orderPriceMinTickSize`), or None.

        **None means REFUSE, never "use a default."** The tick varies per market on this venue —
        most books are on 0.01, but a substantial minority trade on 0.005 or 0.001 — and it varies
        WITHIN a series, so two markets in one league can sit on different grids. A defaulted tick
        is therefore right often enough to
        look correct while silently mispricing exactly the markets that differ — the same argument
        `scanner._event_poly_tick` makes at length for failing to None rather than to a default.

        Returns a `Decimal` parsed from the venue's value via `str()`: this number is the grid every
        quote price is built on, and `Decimal(0.001)` would launder binary float error straight into
        it.

        The venue sends a BARE number here, not the {value,currency} Amount its neighbouring price
        fields use, and wraps the payload as {"market": {...}} — both ✅ VERIFIED against the live
        endpoint. Only parseability is judged — a caller that needs a USABLE tick must still reject
        a non-positive one.

        Promoted out of a probe so every maker-side tool reads the tick one way."""
        try:
            resp = await self._sdk.markets.retrieve_by_slug(slug)
        except Exception as exc:
            # debug, not warning, to match get_best_ask/get_best_bid: WARNING is wired to a Discord
            # handler, and a multi-slug tick sweep with a flaky venue would post one webhook per
            # slug. Callers surface the refusal themselves — None is already loud where it matters.
            log.debug(f"PolyUSClient.get_market_tick({slug}): {exc}")
            return None
        md = resp.get("market", resp) if isinstance(resp, dict) else {}
        raw = md.get("orderPriceMinTickSize") if isinstance(md, dict) else None
        if raw is None:
            return None
        try:
            return Decimal(str(raw))
        except (InvalidOperation, ValueError):
            return None


    async def _fetch_book(self, slug: str, *, fresh: bool):
        """Single choke-point for every order-book GET, so the cached-vs-live decision lives in
        ONE place (no caller can silently read stale). fresh=True appends a nonce query param so
        the read bypasses Polymarket's 30s Cloudflare /book cache (cf=MISS → origin) — REQUIRED
        for anything feeding a live decision, sizing, or the strand-unwind. fresh=False stays
        cacheable for bulk/discovery. Same SDK _request → response.json() either way; only the
        cache key differs (locked by the structural-equivalence test)."""
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

        ask_levels is the tradeable side normalized to ASK space — [(ask_price, qty), ...]
        (long=offers as-is; short=(1−bid_px, qty)) — so the caller can sum FILLABLE-AT-LIMIT
        depth (qty across every level at-or-better than the price it will pay), matching how
        the Kalshi leg is sized (_rest_fillable). Best-level-only depth would overstate
        fillable size in thin books — exactly where it matters.

        transact_time is the book's server-side mutation timestamp (marketData.transactTime,
        ISO-8601), or None when absent — a freshness signal callers log (see transact_age_s).

        fresh=True appends a nonce query param so the read bypasses the 30s Cloudflare cache
        (cf=MISS) and reaches origin; default reads stay cacheable for bulk/discovery callers.
        Both paths go through the same SDK _request → response.json(), so they return the same
        book shape — only the cache key differs (locked by a structural-equivalence test).

        market_state matters: after a goal Poly SUSPENDS the market, freezing a stale
        price that is NOT tradeable — firing into it would reject or fill post-unhalt at
        a bad price. The caller fires only when state == MARKET_STATE_OPEN. Returns
        (None, state, [], transact_time, stats) when there's no quote; (None, '?', [], None,
        empty-stats) on error → fails closed.

        stats = liquidity/activity from marketData.stats (OI / last-trade / shares / notional +
        age stamps) — LOGGING-ONLY, parsed from the SAME fetch (free); all-None when absent and
        never raises into the fire path (see _parse_book_stats). The first four values are the
        fire-path authority and are computed exactly as before — stats is purely additive."""
        slug, is_short = parse_token(token)
        try:
            book = await self._fetch_book(slug, fresh=fresh)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_fill_quote({token}): {exc}")
            return None, "?", [], None, dict(_EMPTY_BOOK_STATS)
        md = book.get("marketData", {}) if isinstance(book, dict) else {}
        return quote_from_md(md, is_short)

    async def get_book(self, market_slug: str) -> Optional[dict]:
        """Return raw order book dict (bids/offers) for a market-side slug."""
        try:
            return await self._sdk.markets.book(market_slug)
        except Exception as exc:
            log.debug(f"PolyUSClient.get_book({market_slug}): {exc}")
            return None

    async def get_book_depth(self, token: str) -> Optional[float]:
        """Authoritative REST depth (shares) at the tradeable side's best level — for the
        would-fire SAMPLER to log alongside the freeze-prone WS depth, so phantom depth can
        be caught by ground truth instead of inference. long=offers@min-px, short=bids@max-px.
        None on error/empty. Deliberately NOT used in the fire path (that fix is deferred)."""
        slug, is_short = parse_token(token)
        try:
            book = await self._sdk.markets.book(slug)
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

        For a moneyline game ONE slug carries both teams: a bare slug buys the
        long side (team A); a "<slug>::short" token buys the short side (team B)
        via BUY_SHORT. `price` is that side's price (long ask, or short = 1 − bid).
        DRY_RUN only logs.

        ⚠️ MISNOMER — this does NOT place a fill-or-kill order. **Polymarket US does not
        honor FOK: it silently REWRITES tif FILL_OR_KILL → IMMEDIATE_OR_CANCEL.** The
        venue DOCS claim otherwise ("must fill entirely or cancel") — the docs are wrong;
        don't "restore" FOK on their say-so. ✅ VERIFIED on a real order: we sent FOK into a
        book shallower than the order, the echoed tif came back IOC, and it PARTIAL-FILLED —
        the exact outcome FOK forbids.

        So the leg is IOC and **CAN PARTIAL-FILL**. There is no "fills completely or is
        killed" guarantee to lean on (a previous version of this docstring claimed one —
        it was never true). The stranded-leg invariant is preserved NOT by the tif but by
        callers sizing the opposite leg off the ACTUAL fill this returns — see
        order_filled_qty and kalshi_arb._place_poly. Any new caller MUST reconcile to the
        real fill; assuming all-or-none books a phantom hedge and leaves a naked leg.
        """
        market_slug, is_short = parse_token(token)
        intent = "ORDER_INTENT_BUY_SHORT" if is_short else "ORDER_INTENT_BUY_LONG"
        # ⚠️ CALLER CONVENTION vs WIRE CONVENTION — they differ on a short, and this is where the
        # two meet. Callers pass `price` in the TOKEN's own space (short = `1 − yes_bid`, per the
        # docstring), because that is what `opp.poly_ask_raw` already carries for a `::short` token.
        # The venue, however, reads a BUY_SHORT price in YES space as a SELL limit — "fill me here
        # or better" — ✅ PROVEN on real orders. So the
        # short price is complemented back to yes space HERE, once, rather than changing three
        # callers on a path that is currently stopped.
        #
        # This was previously sent unconverted, which did not visibly break because an aggressive
        # sell whose limit is far too LOW still fills at the touch. What it lost was the price
        # BOUND: a leg the caller believed capped at `1 − yes_bid` would in fact have accepted any
        # price down to that number had the book moved under it. The fill was right; the protection
        # was not there.
        # `complement`, not `1.0 - price`: in float that is 0.44999999999999996 for 0.55, and this
        # value goes straight to a 4dp wire format. The helper subtracts exactly and is an
        # involution on the wire grid.
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
                # IOC, not FOK. Poly does not honor FOK — it silently rewrites it to IOC
                # (✅ verified on a real partial fill). Asking for a guarantee
                # we never receive is how a false safety claim survived in this docstring. Naming
                # what we actually get also makes us independent of Poly's roadmap: if they ever
                # SHIP real FOK, an order asking for FOK would silently become all-or-nothing —
                # changing fill rates and breaking the data regime with no code change.
                # And IOC is what we'd choose anyway: a partial fill is a smaller arb, which beats
                # the nothing that FOK returns — and measured against real books, the depth at the
                # touch is routinely BELOW the size we want, so all-or-nothing would kill most legs.
                "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
                "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
                # Block until the FOK is terminal so the response carries the real
                # executions/state. Without this, create() can return before the
                # order resolves and a real fill reads as a miss → stranded leg.
                "synchronousExecution": True,
            })
        except Exception as exc:
            # AN ERROR IS NOT AN OUTCOME — it used to `return None`, and that is how an order
            # whose fate we do not know became "filled nothing". `_place_poly` fed the None to
            # `order_filled_qty` → 0.0 → `_exec_poly_first` took its `P <= 0` branch, alerted
            # "MISSED (no fill)", and returned, under a comment reading "no exposure". A genuine
            # IOC kill is a 200 with cumQuantity 0; this path is ONLY timeout / 502 / reset —
            # exactly the case where the engine may already have filled. `synchronousExecution`
            # blocks the order ~61ms server-side, so a timeout lands squarely inside the fill
            # window. The Kalshi client states this rule for itself in _post ("a FOK kill is an
            # OUTCOME, NOT AN ERROR") and raises here; Poly, which fires FIRST in the deployed
            # config, made the opposite choice one layer lower and silently.
            #
            # Callers must distinguish "the venue said zero" from "we do not know". Raising is the
            # only way to say the second. Both exec helpers now catch it, and the verification
            # tooling is safer for it too — it used to print "buy did not fill" on an ERROR and
            # skip its sell_back, having possibly just bought.
            log.error(f"PolyUSClient order failed for {market_slug}: {exc}")
            raise

    async def preview_order(
        self, token: str, price: float, size: float,
        tif: str = "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    ) -> Optional[dict]:
        """Preview a BUY via POST /v1/order/preview — server-side validation that PLACES NOTHING
        (no order, no capital, no position). Returns the venue's echoed Order, or None on error.

        Mirrors place_limit_fok's order shape so the preview validates EXACTLY what the fire path
        would send (same intent, tick-space price, IOC tif). Use it as a pre-flight sanity check
        that an order is ACCEPTED and how its price/tif are handled.

        ⚠️ Preview validates HANDLING ONLY — it does NOT simulate matching. `cumQuantity`/`avgPx`/
        `commission*` come back 0, so it can never tell you whether the order would FILL or what it
        would COST. Never read a fill/cost estimate off it.

        Deliberately NOT DRY-gated: it places nothing, so the point is to hit the REAL venue even in
        DRY. Zero-capital, pre-flight only — not wired into the fire path."""
        market_slug, is_short = parse_token(token)
        intent = "ORDER_INTENT_BUY_SHORT" if is_short else "ORDER_INTENT_BUY_LONG"
        # Mirror place_limit_fok's SHORT-space→yes-space conversion too, or the preview validates
        # the MIRROR of the real order for a `::short` token and the "EXACTLY" above is a lie.
        # This drifts the moment the conversion moves — keep the two in one place mentally.
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

    async def place_limit_gtc(
        self, token: str, price: Decimal | float, size: float, label: str = "",
        *, post_only: bool = False, side: str = "buy",
    ) -> Optional[dict]:
        """Place a RESTING (maker) limit order — `TIME_IN_FORCE_GOOD_TILL_CANCEL`.

        ⚠️ **THIS VENUE DOES HAVE POST-ONLY**, despite a long-lived belief in this repo that it did
        not. Poly exposes `participateDontInitiate` — first-party-documented as "order must rest on
        the book prior to matching (maker only); order will be rejected if it would immediately
        match" — and it is typed in the official SDK. `post_only=True` passes it as a venue-side
        backstop BEHIND the crossing guard below: it closes the read-vs-place RACE the guard cannot
        (we read the book, then place; the ask can move in between, and only the venue can act
        atomically). Default False keeps every existing caller's wire payload byte-identical.

        ✅ **PROVEN ENFORCED** by a two-arm real-money A/B on the same market, book and price minutes
        apart: WITH the flag, a buy priced straight through a deep ask RESTED and never filled;
        WITHOUT it, the same order filled instantly at the ask. So the flag genuinely prevents the
        take. **⚠️ The behaviour is NOT the documented one** — the docs say a would-match order
        "will be rejected"; it is actually ACCEPTED and RESTS without initiating. Do not write code
        that expects a rejection, and do not read "accepted" as "post-only did not apply".

        So: this method reads the live book and REFUSES to place through the touch — at or above the
        best ask on a buy, at or below the best bid on a sell. That refusal is the whole safety
        contract of this function; do not remove it or add a bypass flag — `post_only` is a backstop
        behind it, never a substitute.

        ⚠️ **"LIVE BOOK" MEANS CACHE-BUSTED.** The guard originally read `get_best_ask`/
        `get_best_bid`, which route through `markets.bbo` — NOT cache-busted, and every Poly public
        GET is Cloudflare `max-age=30`. So "live" was false for up to half a minute at a time, and
        the failure is fail-OPEN in the direction that costs money: a stale-HIGH ask clears a buy
        that now crosses, while a stale-LOW one merely false-refuses. The touch now comes from
        `_fetch_book(slug, fresh=True)` (the single cache-busting choke-point, cf=MISS → origin),
        parsed by `touch_from_md` — the same parser every probe reads its touch through, so guard
        and report cannot drift. ONE fetch carries BOTH sides.
        **Rate cost:** the same one GET per placement as before, but it can no longer be served from
        the CDN edge, so it always spends an origin request from Poly's ~1 req/s sustained budget
        (over-limit is THROTTLED, not rejected — a late, stale 200), and `/book` returns a full
        ladder rather than a touch. Do not add a second read to this path; if a caller needs depth
        too, hand it this book rather than fetching again.

        ⚠️ **BOTH sides FAIL CLOSED on an unreadable touch.** The
        buy guard used to read `if ask is not None and price >= ask`, so a 502 or a timeout inside
        `get_best_ask` (which swallows every error to None) SKIPPED the check and let the order go
        out unguarded — at the one moment the book is least knowable. A guard that cannot run
        refuses, and that covers a failed book read and an EMPTY side alike: a missing side means
        "we could not tell", never "nothing to cross". The cost is that a venue blip returns None
        where it would previously have placed. ⚠️ Callers do NOT transparently retry — a probe
        caller records the refusal and RETURNS into its `finally`, i.e. it ABORTS the run and
        sweeps; a human re-runs it. So a flaky book read costs a probe run, which is the right
        trade against an unguarded real order but is not a free retry.

        ⚠️ **ONE CANONICAL ROUNDING (`_wire_price`).** The price is snapped to the
        venue's 4dp grid BEFORE either guard runs, and that same value is what ships — the payload
        is built from it rather than from a second `f"{price:.4f}"`. Two roundings of one number are
        two chances to disagree, and they disagreed exactly at the touch: a buy at 0.44996 passed
        the raw-float compare against a 0.45 ask (0.44996 < 0.45) and then went on the wire as
        "0.4500", the very price the guard had just cleared as behind the touch. The sell side had
        the same gap through the complement (a yes-space 0.40004 over a 0.40 bid shipped as short
        "0.6000" = a yes ask AT the bid). Keep the guard and the wire reading one value.

        ✅ VERIFIED zero-capital via `preview_order`: the venue ACCEPTS
        TIME_IN_FORCE_GOOD_TILL_CANCEL and echoes it back unrewritten, leaving quantity resting —
        unlike FILL_OR_KILL, which Poly silently rewrites to IOC. So a GTC order genuinely rests.

        ── `side="sell"` — the maker's ASK, as a SYNTHETIC short. PLUMBING ONLY, NO CALLER. ──────
        **Polymarket has no native resting ask.** An opening ask — selling yes exposure we do not
        hold — is expressed as a resting `ORDER_INTENT_BUY_SHORT`. ⚠️ **The price stays in YES SPACE
        on the wire — there is NO complement conversion.** The venue reads a BUY_SHORT `price` as a
        yes-space SELL limit ("fill me here or better"), so an ask at 0.853 ships as 0.853.
        ✅ PROVEN on real orders. An earlier version of this docstring described a complement
        conversion, which shipped every ask at its MIRROR — an order that can never fill.

        ⚠️ **`SELL_SHORT` IS NOT THE ASK — it is a BUY.** Poly's intents are {verb}_{POSITION} with
        the verb acting ON the position, so `BUY_LONG` and `SELL_SHORT` both END UP LONG yes (acquire
        a long / dispose of a short), and `BUY_SHORT` and `SELL_LONG` both end up SHORT. The name
        reads like an ask and is the opposite of one. MEASURED, not derived, over a large trade
        tape carrying the venue's own `taker_side` raw: SELL_SHORT is `ORDER_SIDE_BUY` and prints
        ABOVE the yes mid almost always — it lifts the offer, exactly like BUY_LONG; BUY_SHORT is
        `ORDER_SIDE_SELL` and hits the yes bid. The mapping is 1:1 with no exceptions. The first
        version of this parameter sent SELL_SHORT on the
        reasoning that "an opening ask is a short"; it would have rested a SECOND BID, so the arm
        would never have sold, would have ratcheted long, and every ask-side fill number off it would
        have been fiction. Do not re-derive this from the names.

        ⚠️ **PRICE SPACE.** The venue reads BOTH `BUY_SHORT` and `SELL_SHORT` prices in **YES
        space**, and this is the single easiest thing to get backwards here: an earlier belief that
        short intents are priced in SHORT space had two real-money precedents behind it, and both
        precedents were themselves wrong. `place_limit_fok`'s CALLERS still pass short-space (so it
        complements at the wire); `sell_back` prices a short disposal at the yes ask. ✅ PROVEN on
        real orders — get this wrong and the order ships mirrored and can never fill.

        Crossing guard: refuse a yes-space ask at or BELOW the best yes bid (the `bids` side of the
        fresh book). The guard was always correct — it compares in yes space, which is what the wire
        now actually carries; previously it cleared a price that was never sent. Both sides
        FAIL CLOSED on an unreadable touch (see above). `post_only` composes with either side.

        **UNUSED, and it stays unused until a live Poly maker run is approved.** It exists so that a
        confirmed maker rebate has somewhere to go without a money-path edit under time pressure;
        nothing in the bot, the probes, or the shadow collector calls it. Treat its first caller as a
        money-path change and review it as one — and note the intent above is measured from the TRADE
        tape, and is still UNVERIFIED BY AN ACTUAL FILL for a resting order: the zero-capital
        confirmation would be an
        `orders.preview` echo showing `side: ORDER_SIDE_SELL` for a BUY_SHORT, which no one has run.
        `side="buy"` is the default and its wire payload is unchanged (pinned by a whole-dict test).

        ── `::short` tokens are REFUSED ON BOTH SIDES — yes-space quoting only. ─────────────────
        One slug carries ONE book, in yes space; the short side is its mirror (short ask = `1 −
        best_yes_bid`). The guard reads that book's yes touches, so a `::short` buy compared a
        short-space price against the LONG side's ask — the wrong touch, and a guard reading the
        wrong touch is worse than none. (The suffix was also once forwarded verbatim to `bbo`,
        which knows nothing of it; the touch read now strips it, but comparing against the
        un-mirrored side would still be wrong.) On the sell side the suffix has no coherent reading
        at all (a short of a short).

        **This is not missing functionality, so do not "restore" it out of symmetry with
        `place_limit_fok`.** Short-space quoting is already expressible here: an opening ask on the
        yes side IS a resting bid on the no side, which is exactly what `side="sell"` on the LONG
        token sends — `ORDER_INTENT_BUY_SHORT`, priced in yes space. Adding short-space guard
        arithmetic would buy a second spelling of an order this method can already place, in
        exchange for a second set of price-space conversions on a money path. (Decided while the
        method had zero production callers — the cheapest moment such a call is ever made.)

        Refusal shapes differ ON PURPOSE, so a caller can tell them apart: a would-cross or
        unreadable touch returns None (a market condition — retry), while an unknown `side`, a
        `::short` token, or a non-finite price/quantity raises `PreSendRefusal` (a ValueError)
        BEFORE any venue call (a caller bug — nothing is placed, and silently defaulting a typo'd
        side to `buy` would send a wrong-DIRECTION order). The TYPE is load-bearing: `_place`
        clears its durable intent on `PreSendRefusal` only — a new pre-send refusal raised as
        plain ValueError leaves a phantom intent.

        Returns the create response, or None if the price would cross (nothing placed). DRY logs only.
        """
        if side not in ("buy", "sell"):
            raise PreSendRefusal(f"place_limit_gtc: side must be 'buy' or 'sell', got {side!r}")
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
        # guard below, so a dry run exercises every refusal a real one would.
        wire_qty = _wire_quantity(size)
        # Crossing check FIRST — before any DRY short-circuit, so a dry run exercises the same guard.
        wire_px = yes_px
        # THE touch read, and it must reach ORIGIN. NOT get_best_ask/get_best_bid: those route
        # through `markets.bbo`, which does not cache-bust, and every Poly public GET is Cloudflare
        # max-age=30 — so the guard could clear a placement against a touch half a minute old, and
        # the bias is fail-OPEN (a stale-HIGH ask lets a crossing buy through). `_fetch_book` is the
        # repo's single cache-busting choke-point; one fetch carries BOTH touches.
        try:
            book = await self._fetch_book(market_slug, fresh=True)
        except Exception as exc:
            log.warning(
                f"REFUSING GTC {side} {market_slug} @ {yes_px}: fresh book read FAILED "
                f"({safe_exc(exc)}), so the crossing check cannot run. A guard that cannot run "
                f"refuses.")
            return None
        bid, ask, _tob = touch_from_md(book.get("marketData") if isinstance(book, dict) else None)
        if side == "sell":
            if bid is None:
                log.warning(
                    f"REFUSING GTC sell {market_slug} @ {yes_px}: best bid UNREADABLE, so the "
                    f"crossing check cannot run. A guard that cannot run refuses.")
                return None
            if yes_px <= bid:
                log.warning(
                    f"REFUSING GTC sell {market_slug} @ {yes_px}: best bid is {bid}, so this "
                    f"would CROSS and execute as a taker. Price behind the touch.")
                return None
            # ⛔ THE PRICE STAYS IN YES SPACE. Do NOT complement it. ✅ PROVEN on real orders:
            # complement it and the ask ships at its mirror, where it can never fill.
            #
            # This line used to send `Decimal(1) - yes_px`, on the reasoning that a BUY_SHORT is a
            # bid for the NO side and therefore priced at the complement. The venue does not read it
            # that way: for BUY_SHORT it treats `price` as a YES-space SELL limit — "fill me here or
            # BETTER". So an ask intended to rest at 0.853 went out as "sell YES at anything down to
            # 0.147", a market sell wearing a limit order's clothes.
            #
            # The proof, because this is not the kind of line to change on a hunch. A post-only ask
            # intended at 0.900 — which cannot cross a 0.854 ask under ANY reading — was still
            # REJECTED. The same order with post_only off FILLED at the best bid (0.851) and paid
            # the taker fee, which is what a sell limit of 0.100 does and is NOT what a NO bid at
            # 0.100 does, since a buyer is never filled worse than their limit. Sending the yes-space
            # price instead rested exactly as intended (ORDER_STATE_NEW, leaves 5).
            #
            # It hid for so long because every other BUY_SHORT in this repo is AGGRESSIVE (FOK/IOC,
            # the arb short leg): a sell limit that is far too LOW still fills at the touch, which
            # looks like success. Only a RESTING ask can show it, and none had been placed until the
            # rung-2 gate. The guard above is unaffected — it was always correct, comparing in yes
            # space against the yes bid; only the wire conversion was wrong.
            intent = "ORDER_INTENT_BUY_SHORT"
            wire_px = yes_px
        else:
            if ask is None:
                log.warning(
                    f"REFUSING GTC buy {market_slug} @ {yes_px}: best ask UNREADABLE, so the "
                    f"crossing check cannot run. A guard that cannot run refuses.")
                return None
            if yes_px >= ask:
                log.warning(
                    f"REFUSING GTC buy {market_slug} @ {yes_px}: best ask is {ask}, so this would "
                    f"CROSS and execute as a taker. Price behind the touch.")
                return None
            intent = "ORDER_INTENT_BUY_LONG"
        tag = "[DRY RUN] " if self._dry_run else ""
        log.info(f"{tag}GTC(US)  slug={market_slug}  {side}  "
                 f"leg={'short' if side == 'sell' else 'long'}  "
                 f"price={yes_px}  wire={wire_px}  size={size}  "
                 f"post_only={post_only}  {label}")
        if self._dry_run:
            return {"status": "dry_run", "market_slug": market_slug, "price": price, "size": size}
        body: dict = {
            "marketSlug": market_slug,
            "intent": intent,
            "type": "ORDER_TYPE_LIMIT",
            # `str` of a value already quantized to the 4dp grid — NOT a second `f"{x:.4f}"`, which
            # would be a rounding the guard never saw.
            "price": {"value": str(wire_px), "currency": "USD"},
            # ⛔ The `int(round(size))` cast is RETIRED — probed against the venue's own
            # preview, which echoes fractional quantities EXACTLY, with an integer control
            # clean. The cast silently reshaped a 12.80 close into 13, which can overshoot
            # straight THROUGH flat. Integral sizes still ship as int (the maker's
            # proven payload, byte-identical); a fractional size ships as the exact STRING
            # of its Decimal — the string shape the venue accepted, never a float relaunder.
            "quantity": wire_qty,
            "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            # NOT synchronousExecution: a resting order has no terminal state to wait for.
        }
        if post_only:
            # Omitted (not False) when unset: every real order ever sent omitted it, and the venue
            # SILENTLY ACCEPTS unknown/false-y fields (poly_postonly_probe header) — omission is the
            # only value with a proven track record. ⚠️ Enforcement semantics are the PROVEN ones,
            # not the documented ones: a would-cross post-only order RESTS (it is not rejected).
            body["participateDontInitiate"] = True
        try:
            return await self._sdk.orders.create(body)
        except Exception as exc:
            # Same rule as place_limit_fok: an error is NOT an outcome. We do not know whether the
            # order rested, so the caller must treat this as "unknown" and sweep open orders.
            log.error(f"PolyUSClient GTC order failed for {market_slug}: {exc}")
            raise

    @staticmethod
    def _order_verdict(exc: Exception) -> str:
        """`not_found` vs `error` for order reads/cancels.

        ⛔ The SDK raises NotFoundError for ANY 404 — a renamed route or base-URL drift
        included — and its message degrades to the bare reason-phrase when the response
        body is not JSON. So the venue's ORDER-SCOPED answer is distinguished by a
        STRUCTURED body: NotFoundError + dict body ⇒ `not_found`; anything else ⇒
        `error`, never terminal. A substring never makes this decision."""
        global _NOT_FOUND_BODY_LOGGED
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
                        f"the probe chain cannot arm.")
        if isinstance(exc, NotFoundError) and isinstance(getattr(exc, "body", None), dict):
            if not _NOT_FOUND_BODY_LOGGED:
                # A standing pre-merge obligation: the one structured-404-body specimen we
                # went looking for recovered before it could be read, so the first real one
                # the new code meets goes on record. Until a real dict body
                # is captured, the not_found verdict is treated as possibly-unreachable and
                # retirement rides the cancel-probe chain. Once per process, WARNING so it
                # survives log rotation triage.
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
            # ⛔ LOG LEVEL is decided by the ROUTINE-NESS of the message, not by the verdict:
            # the structured dict body has never been observed, so gating INFO on
            # verdict=="not_found" alone made every routine order-scoped 404 a WARNING —
            # which the logger mirrors to the alerts channel per poll, per distinct order
            # id. That is the alarm-fatigue trap `get_order` already hit. The substring
            # decides the level ONLY; the verdict stays structured-body-only.
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

    async def get_open_orders(self, slugs: Optional[Sequence[str]] = None) -> list[dict]:
        """Resting Poly orders — account-wide, or SERVER-SIDE scoped to `slugs` when given.

        Used to sweep strays whose create-response was lost — the same hole that stranded three
        Kalshi orders on an early run. Scoping at the source: the
        endpoint's envelope carries no pagination token, so a silent server-side cap cannot be
        ruled out on an account-wide read — a slate-bounded listing shrinks that truncation
        surface for the callers whose decisions ride on listing-ABSENCE (the poll's retirement
        hint, the teardown sweep's swept_clean). Callers that hunt strays ANYWHERE (the
        preflights) deliberately stay account-wide.

        RAISES on an unreadable shape rather than returning [] — "no open orders" and "we could not
        tell" must never be the same answer (the get_positions lesson)."""
        params = {"slugs": [str(s) for s in slugs]} if slugs else None
        resp = await self._sdk.orders.list(params) if params else await self._sdk.orders.list()
        orders = resp.get("orders") if isinstance(resp, dict) else None
        if orders is None and isinstance(resp, dict) and not resp:
            return []
        if not isinstance(orders, list):
            raise RuntimeError(
                f"Poly open orders: unrecognised shape (keys={sorted(resp) if isinstance(resp, dict) else type(resp)}) "
                f"— refusing to report 'no open orders'")
        return [o for o in orders if isinstance(o, dict)]

    async def get_activities_page(self, cursor: str = "") -> tuple[list[dict], str]:
        """ONE page of the trade-activities ledger — the recovery walk's read unit.
        Returns (activities, nextCursor); "" cursor = page 1
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

        This is how the MAKER REBATE gets verified: Poly's docs claim an automatic maker rebate of
        0.0125*C*p*(1-p) per fill, but that is [UNVERIFIED, docs-sourced] — the same provenance that
        produced the Kalshi cent-vs-centicent bug. `commissionNotionalTotalCollected` on this endpoint
        is the authoritative number — it is what pinned the Poly TAKER fee against real fills. A
        rebate should surface as a NEGATIVE commission; if it does not appear at all, the docs are
        wrong and Poly maker is merely fee-free, not paid.

        Via the SDK's `orders.retrieve` — the hand-rolled `self._sdk.get(f"/v1/order/{id}")` this
        used before was unauthenticated and returned "not authorized" on every call; it had zero
        callers, so nothing ever noticed until a probe added a response-shape check.
        Response shape, ✅ VERIFIED live on real orders:
        {"order": {..., "cumQuantity": int, "state": "ORDER_STATE_*",
                   "commissionNotionalTotalCollected": {"value": "0.0000", "currency": "USD"},
                   "avgPx": {...}}}."""
        try:
            return await self._sdk.orders.retrieve(order_id)
        except Exception as exc:
            # "Order not found" is a ROUTINE venue answer (post-only reject / purge of a
            # cancelled order — observed many times in a single session, every one
            # recovered by the delayed verify with belief == venue after) and every
            # caller already escalates loudly itself if the miss matters. WARNING here
            # mirrored each poll to the Discord alerts channel via the logger handler —
            # alarm fatigue on the channel that must stay quiet enough to matter.
            # Match "order not found" NARROWLY: a bare "Not Found" (reason-phrase of
            # ANY 404 — e.g. a renamed route or SDK base-URL drift breaking every read)
            # must stay WARNING; only the venue's order-scoped answer is routine.
            level = log.info if "order not found" in str(exc).lower() else log.warning
            level(f"PolyUSClient get_order failed for {order_id}: {exc}")
            return None

    async def sell_back(
        self, token: str, size: float, label: str = ""
    ) -> tuple[Optional[float], float]:
        """
        Emergency sell of a stranded position at the best available price.

        Returns **(vwap_price, sold_qty)** — `(None, 0.0)` if nothing sold.

        Long leg → SELL_LONG at the best bid. Short leg ("<slug>::short") →
        SELL_SHORT at the best yes ASK, **in yes space** (it buys the yes side back, crossing the
        offers). ⚠️ NOT `1 − best ask`: that was the shipped behaviour for a long time and made a
        short flatten impossible, not merely mispriced — see the comment at the price selection.
        Tries the best price, then retries 2¢ through it for WHATEVER IS STILL UNSOLD — **through**
        being −2¢ on a long (accept less) and +2¢ on a short (pay more). Preserves the stranded-leg
        unwind invariant for Poly US.

        ⚠️ This is NOT a fill-or-kill sell, despite the tif we send: Poly rewrites
        FOK → IOC (✅ verified on a real order — see place_limit_fok), so a
        sell **CAN PARTIAL-FILL**. That is why this returns the sold QUANTITY and not
        just a price: a caller given only a price cannot tell a full sale from a
        partial one, and must strand only the UNSOLD remainder.

        History — the bug this shape exists to prevent: the old
        version returned Optional[float] and its retry closed over the ORIGINAL `size`.
        A partial first attempt (60/100) therefore (a) read as a total failure, because
        order_is_filled requires a FULL fill, (b) re-sent quantity=100 on the retry while
        only 40 were still held — an OVERSELL, and (c) ended up reporting None, so the
        caller stranded 100 phantom shares and the proceeds of the 60 that really sold
        never reached P&L. Every one of those followed from assuming FOK semantics the
        venue does not provide.

        The reported price is the qty-weighted mean of the LIMITS we transacted at, and it is
        conservative in BOTH directions — but they are different directions, so callers must not
        share one formula. On a LONG disposal we sell: a real fill is at-or-better than the limit,
        so we RECEIVE at-least this. On a SHORT disposal we buy the yes side back: a real fill is
        at-or-better than the limit, so we PAY at-most this. Either way the flatten cost a caller
        books off it is an upper bound and never flattering — provided the caller subtracts the
        right way round. See `_unwind_poly_excess`, where getting that backwards booked a realized
        loss as a NEGATIVE number, which both loss caps then clamped to zero — a real loss, made
        invisible to the two rails whose whole job is to see it.
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
                # Disposing of a short BUYS the yes side back, so it crosses the LONG offers and
                # the limit is the best (lowest) yes ask — IN YES SPACE, NOT the complement.
                # ✅ PROVEN on a real order: SELL_SHORT sent at the yes-space ask filled at that
                # ask and closed the position.
                #
                # ⛔ This line was `1.0 - min(ask_prices)`, which made the short flatten IMPOSSIBLE
                # rather than merely mispriced: it bid the COMPLEMENT of the ask for something
                # offered at the ask — far below the market, so it could never fill.
                # Observed live — both attempts missed and the leg was reported stranded. This is
                # the strand-unwind path, so it has never been able to do its job on a short.
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
                # A response can report terminal FILLED without echoing cumQuantity (the
                # order_is_filled path this replaced relied on exactly that). FILLED means the
                # WHOLE requested qty filled by definition, so trust it rather than read 0.
                # Direction matters: UNDER-reporting a sale is the dangerous way to be wrong —
                # it strands shares we already sold and re-offers them on the retry. Over-
                # reporting can't happen here (state==FILLED is only ever a complete fill).
                if sold <= 0 and order_is_filled(resp):
                    sold = float(int(round(qty)))
                return sold
            except Exception as exc:
                # ⛔ AN ERROR IS NOT AN OUTCOME — the same rule `place_limit_fok` states above for the
                # BUY leg, and the same asymmetry the comment directly above already names. This
                # handler used to `return 0.0`, which told the retry loop those shares were still
                # HELD, so it re-offered them 2¢ lower. `synchronousExecution` blocks the order
                # server-side for tens of milliseconds, so a timeout/502/reset lands INSIDE the
                # fill window and the engine may have sold the whole qty. Reproduced end to end:
                # first reply lost, retry sells the same quantity again → NET SHORT the position we
                # were flattening, and because the remainder reached zero the caller booked a clean
                # flatten — no strand record, no global pause, no alert,
                # while the log read "Sell-back(US) succeeded".
                #
                # Raising is the only way to say "we do not know". `_unwind_poly_excess` already maps
                # an exception to a full strand → pause → alert → reconciler, which is the correct
                # fail direction. Accepted cost, stated so nobody "simplifies" it back: a transient
                # 502 during an unwind now needs an operator `touch logs/resume`.
                #
                # A genuine IOC kill is NOT this path — it is a 200 with cumQuantity 0, which returns
                # 0.0 above and lets the discount retry proceed.
                log.error(f"Sell-back attempt at {price:.4f} FAILED — outcome UNKNOWN: {safe_exc(exc)}")
                raise

        # Best price first, then 2¢ THROUGH it — each attempt sized to what is STILL HELD.
        #
        # ⛔ "Through" is a different DIRECTION on each side, and getting it wrong makes the retry a
        # no-op exactly when it is needed. Disposing of a LONG sells, so conceding means accepting
        # LESS (−2¢). Disposing of a SHORT buys the yes side back, so conceding means paying MORE
        # (+2¢). The single `best_price - 0.02` ladder applied the long direction to both, which on
        # a short moved the limit further from marketable — a "more aggressive" retry that was
        # strictly less likely to fill than the attempt it was rescuing.
        # ⚠️ The clamp must never land BEHIND the first attempt. A flat `min(0.99, best+0.02)` makes
        # the retry LESS marketable than attempt 1 whenever the yes ask is already above 0.99 —
        # reintroducing, in the deepest-adverse regime, the exact no-op this ladder was fixed to
        # remove. That regime is reachable: the extreme-band gate bounds the DETECTION price, while
        # `sell_back` re-reads a fresh book at unwind time, i.e. after the move that caused the
        # unwind. Same on the long side for a bid under 0.01. So clamp to the wire bound and then
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
