"""
bot/kalshi/client.py
────────────────────
Kalshi REST + WebSocket auth client.

Auth: every request requires three headers derived from a fresh timestamp:
  KALSHI-ACCESS-KEY        — API key ID (from Kalshi dashboard)
  KALSHI-ACCESS-SIGNATURE  — base64(sign(ts_ms + METHOD + path))
  KALSHI-ACCESS-TIMESTAMP  — Unix milliseconds as string

Signing is RSA-PSS (SHA-256) for RSA keys — Kalshi's default scheme — or Ed25519
if the key is an Ed25519 key. _sign auto-detects from the loaded key type.
"""
from __future__ import annotations

import base64
import time
from typing import Any

import aiohttp

from bot.core import config
from bot.core.money import complement
from bot.core.logger import get_logger
from bot.core.redact import safe_exc

log = get_logger(__name__)

_PROD_REST = "https://external-api.kalshi.com/trade-api/v2"
_DEMO_REST = "https://external-api.demo.kalshi.co/trade-api/v2"
_PROD_WS   = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
_DEMO_WS   = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

_WS_PATH   = "/trade-api/ws/v2"
_REST_PATH = "/trade-api/v2"

# Kalshi V2 signals a killed FOK with a 409 carrying this code (V1 returned a body with
# fill_count=0). It is an OUTCOME, not an error — see _post. Same string the RTT probe keys on.
_FOK_KILLED = "fill_or_kill_insufficient_resting_volume"

# Hard bound on an ORDER round-trip. aiohttp's DEFAULT is total=300s (5 MINUTES) — an unexamined
# default from before anyone had measured an RTT. A probe against the live order endpoint puts the
# real round-trip in the low tens of milliseconds, so 300s is four orders of magnitude of slack.
# Why it matters: _execution_lock is ONE GLOBAL lock (runner.py) shared by kalshi_arb, poly_arb,
# ladder and macro, and poly_first fires POLY FIRST. So a hung Kalshi POST means we sit on a NAKED,
# unhedged Poly leg while EVERY trading path in the bot is frozen — for up to five minutes.
# 5s = ~200x the max observed order RTT: generous for a real slow path, bounded for a hung one.
# Scoped to _post (the order path / the lock-holder); bulk discovery GETs keep the session default.
_ORDER_TIMEOUT = aiohttp.ClientTimeout(total=5, sock_connect=2)

# /portfolio/positions is cursor-paginated. 200 is the venue's max page size; the page stop mirrors
# the Poly side of the reconciler. Hitting the stop with a cursor still set RAISES — a truncated
# read of what we hold must not be reported as a complete one (see get_positions).
_POSITIONS_PAGE_LIMIT = 200
_POSITIONS_MAX_PAGES = 20



# (v1_side, v1_action) → V2 book side. V2 is YES-ONLY: `bid` buys YES, `ask` sells YES; there is
# no `action` field and no "buy NO". Buying NO is expressed as selling YES at the COMPLEMENT.
# The NO side flips BOTH the book side AND the price — flipping only one buys the wrong side.
# CONFIRMED BY THE EXCHANGE: we sent {side:"ask", price:"0.3000"} holding zero YES and
# Kalshi's fill record came back {"side":"no","no_price_dollars":"0.7000"} with position_fp=-1.00.
_V2_BOOK_SIDE = {
    ("yes", "buy"): "bid",    # buy YES  @ p        → bid @ p
    ("yes", "sell"): "ask",   # sell YES @ p        → ask @ p
    ("no", "buy"): "ask",     # buy NO   @ n        → ask @ (1-n)
    ("no", "sell"): "bid",    # sell NO  @ n        → bid @ (1-n)
}


def _v2_order_params(side: str, action: str, price_dollars: str | float) -> tuple[str, float]:
    """Map V1 (side, action, side-space price) → V2 (book_side, YES price).

    V2 quotes everything from the YES side, so a NO order must be complemented: `price` is ALWAYS
    the YES price. This is the 1-x footgun that once stranded every NO leg under V1 — pinned by
    tests/test_kalshi_v2_orders.py against the exchange's own fill record.

    TICK SAFETY: callers tick-floor in the SIDE'S OWN space (kalshi_tick_floor) BEFORE calling, and
    1 − (multiple of 0.01) is still a multiple of 0.01, so complementing AFTER that floor is exact
    and preserves the economic bound (pay ≤ limit). Do NOT reorder: complementing before the floor
    inverts the rounding direction (flooring 1−n makes us pay MORE for NO, not less).
    """
    key = (str(side).lower(), str(action).lower())
    if key not in _V2_BOOK_SIDE:
        raise ValueError(f"unknown Kalshi side/action combination: {side!r}/{action!r}")
    p = float(price_dollars)
    # complement() not round(1.0-p, 6): this is the price SENT to Kalshi (the wire is always
    # YES-space, so a NO leg is 1-p). Exact — 1-0.55 is 0.45, never 0.44999999999999996.
    yes_price = p if key[0] == "yes" else complement(p)
    return _V2_BOOK_SIDE[key], yes_price


def _order_counts(resp) -> tuple[float | None, float | None]:
    """(fill, remaining) from EITHER order response shape, else (None, None).

    V2 CREATE (POST /portfolio/events/orders) is FLAT: {fill_count, remaining_count, ...} with NO
    `order` envelope and NO `status`. GET /portfolio/orders/{id} is NESTED: {order:{fill_count_fp,
    remaining_count_fp, status}}. The V1 parsers only read the nested shape, so against a V2 create
    they silently returned 0 — which reads as "the Kalshi leg missed" on a leg that actually FILLED,
    flattening Poly while holding Kalshi (a naked position the bot doesn't know it has).
    """
    if not isinstance(resp, dict):
        return (None, None)
    if "fill_count" in resp or "remaining_count" in resp:      # V2 create (flat)
        f, r = resp.get("fill_count"), resp.get("remaining_count")
    else:                                                       # GET / DRY / legacy (nested)
        o = resp.get("order")
        if not isinstance(o, dict):
            return (None, None)
        if "fill_count_fp" not in o and "remaining_count_fp" not in o:
            return (None, None)
        f, r = o.get("fill_count_fp"), o.get("remaining_count_fp")
    try:
        return (float(f or 0), float(r or 0))
    except (TypeError, ValueError):
        return (None, None)


def kalshi_order_filled(resp) -> bool:
    """True if a Kalshi FOK FULLY filled. Handles both the V2 flat and nested shapes.

    Fill counts are authoritative and checked STRICTLY — a fully-filled FOK has fill > 0 and
    remaining == 0, so a partial never reads as a full fill even if a status says 'executed'.
    V2 create responses carry no `status` at all; the status fallback exists only for the DRY
    stub and legacy/GET fixtures.
    """
    fill, remaining = _order_counts(resp)
    if fill is not None:
        return fill > 0 and remaining == 0
    o = resp.get("order") if isinstance(resp, dict) else None
    return isinstance(o, dict) and o.get("status") in ("executed", "filled")


def kalshi_avg_fill_cost(resp, side: str) -> float | None:
    """ACTUAL fee-inclusive cost per contract from a V2 create, or None if unreadable. `side`
    ('yes'/'no') is REQUIRED — the wire price is YES-space, so a NO leg is complemented (see below).

    The exchange reports both halves, so no fee model prices the trade:
        average_fill_price  — the real VWAP (an IOC can sweep several levels), PER CONTRACT
        average_fee_paid    — the real fee. **Per contract or per order? WE DO NOT KNOW.**
    [The price VERIFIED on a real prod fill: 1 YES @ 0.4270 → 0.4270/contract.]

    ✅ RESOLVED — MEASURED PER-CONTRACT. A demo multi-contract fill settled it: at fill_count=2 the
    reported `average_fee_paid` matched the per-contract prediction and sat a clean 2× below the
    order total, agreeing with Kalshi's docs. The resolver below picked per-contract on that real
    response. It keeps auto-detecting anyway — robust if the basis ever changes, or if demo ≠ prod.
    The reasoning that made NOT guessing the right call is preserved:

    ⚠️ `average_fee_paid`'s basis WAS unmeasurable read-only — the field
    exists on the create response and on NO other endpoint (/portfolio/fills
    reports `fee_cost`, /portfolio/orders reports `taker_fees_dollars`, neither is this field).
    Our only real fixture is `fill_count=1`, where **per-contract and per-order are arithmetically
    identical**, so the measurement that "verified" this could never have discriminated. The
    predecessor of this function asserted "for the ORDER (pairs with fill_count)" and divided; its
    test pinned that with a FABRICATED fixture (4 @ 0.50 → fee 0.0400, which matches neither the
    real order total 0.0700 nor the real per-contract 0.0175). Kalshi's docs say "per contract",
    and its sibling `average_fill_price` certainly is — but this repo has been burned once already
    by reading a venue doc instead of a fill, and that is exactly how the cent-vs-centicent bug got
    in.

    So we DON'T guess: the response identifies itself. The fee FORMULA is verified to the
    centicent against real fills (tests/test_kalshi_fee_model.py) and the two candidates differ
    by a factor of `fill_count`, so we compute both and take whichever the venue's own number
    matches. At fill_count=1 they coincide and the answer is the same either way. Guessing is not
    an option worth taking, and it is wrong in BOTH directions: reading a per-contract fee as an
    order total UNDERSTATES cost by a factor of `fill_count` — eating a large fraction of the whole
    minimum edge and flattering `realized_settled`, the designated sizing authority — while the
    mirror mistake OVERSTATES it by the same factor, which would make every hedge book as
    catastrophically unprofitable and trip the cumulative loss cap on healthy trades.

    Matching NEITHER candidate returns None (the caller falls back) and logs loudly: it means the
    fee schedule moved or the field changed, and either way we must not price a real position off
    a number we no longer understand.

    Why it matters: _record_hedge books this into trades.log's `cost`, and settlement_scorer reads
    that same `cost` for realized_settled — the sizing authority.

    None = CANNOT READ; the caller falls back explicitly. NEVER return 0.0 — that books a FREE
    fill, the most flattering possible lie about a leg we actually paid for. A price with no fee is
    also None: booking it fee-free under-costs the leg (the failure _coerce_fee_rate exists to
    prevent).
    """
    if not isinstance(resp, dict):
        return None
    try:
        fill = float(resp.get("fill_count") or 0)
        px = resp.get("average_fill_price")
        fee = resp.get("average_fee_paid")
        if fill <= 0 or px is None or fee is None:
            return None
        px, fee = float(px), float(fee)
    except (TypeError, ValueError):
        return None
    got = _kalshi_px_and_fee(px, fee, fill, "kalshi_avg_fill_cost")
    if got is None:
        return None
    px, fee_pc = got
    # V2's average_fill_price is ALWAYS YES-space; a NO leg's real per-contract cost is the
    # COMPLEMENT (1 − px) + fee. The SEND side complements (_v2_order_params); the READ side must
    # too, or every NO leg books the wrong cost into trades.log → realized_settled → the loss cap
    # (demo-caught: a NO buy at a no-ask of 0.99 came back average_fill_price 0.01). The fee is
    # symmetric in p(1−p), so only the price flips.
    px_side = complement(px) if side == "no" else px   # exact YES-space wire → side space
    return px_side + fee_pc


def _kalshi_px_and_fee(px: float, fee: float, fill: float,
                       who: str) -> tuple[float, float] | None:
    """(avg_fill_price, fee PER CONTRACT) — resolving `average_fee_paid`'s unknown basis against
    the verified fee formula. See kalshi_avg_fill_cost for the full why. None = matches neither
    candidate, i.e. we no longer understand the number and must not price a position off it.

    Shared by the BUY reader (cost = px + fee) and the SELL reader (proceeds = px − fee), because
    the basis question is identical for both and answering it twice would let them drift.
    """
    # Pure fee math from bot.kalshi.fees (deliberately decoupled from the arb engine) —
    # kept a local import to preserve this function's original lazy-load shape.
    from bot.kalshi.fees import _kalshi_taker_fee
    per_contract = _kalshi_taker_fee(px, int(fill) or 1)
    order_total = per_contract * fill
    tol = 5e-4                                  # a few centicents of slack around the ceiling
    if abs(fee - per_contract) <= tol:
        return px, fee                          # the field is PER CONTRACT
    if abs(fee - order_total) <= tol:
        return px, fee / fill                   # the field is the ORDER TOTAL
    log.error(
        f"{who}: average_fee_paid={fee:.6f} matches NEITHER the per-contract fee "
        f"({per_contract:.6f}) nor the order total ({order_total:.6f}) for {fill:.0f} @ {px:.4f} — "
        f"the fee schedule or the field changed. Refusing to price the leg; caller falls back."
    )
    return None


def kalshi_avg_sell_proceeds(resp, side: str) -> float | None:
    """NET proceeds per contract from a V2 SELL — the mirror of kalshi_avg_fill_cost. `side`
    ('yes'/'no') REQUIRED: the wire price is YES-space, so a NO leg is complemented (see cost reader).

    A buy PAYS the fee (cost = px + fee); a sell NETS it (proceeds = px − fee). Reading a sell with
    the buy helper would report proceeds ~2 fees too high and make every unwind look cheaper than
    it was — on a number the loss cap reads.

    Exists because the unwind used to book its exit at the LIMIT it sent (`sell_price`) and discard
    `sell_resp` entirely, so an IOC that swept several levels booked as if it got the top of the
    book. None = unreadable → the caller falls back to the limit and says so.
    """
    if not isinstance(resp, dict):
        return None
    try:
        fill = float(resp.get("fill_count") or 0)
        px = resp.get("average_fill_price")
        fee = resp.get("average_fee_paid")
        if fill <= 0 or px is None or fee is None:
            return None
        px, fee = float(px), float(fee)
    except (TypeError, ValueError):
        return None
    got = _kalshi_px_and_fee(px, fee, fill, "kalshi_avg_sell_proceeds")
    if got is None:
        return None
    px, fee_pc = got
    px_side = complement(px) if side == "no" else px      # exact; YES-space wire → side space
    return px_side - fee_pc


def kalshi_fee_paid(resp) -> float | None:
    """TOTAL taker fee ($) actually paid on a V2 create — the discrete-fee companion to
    kalshi_avg_fill_cost (which folds the fee into a per-contract COST). This returns the whole
    dollar fee for the executions ledger / tax reporting: `average_fee_paid` resolved to per
    contract via _kalshi_px_and_fee (the same unknown-basis resolution the cost reader uses), times
    fill_count.

    None = unreadable (matches neither fee candidate, or a field is missing) → the caller records a
    blank, never 0.0: a 0 fee on a real fill is the same free-fill lie kalshi_avg_fill_cost refuses.
    """
    if not isinstance(resp, dict):
        return None
    try:
        fill = float(resp.get("fill_count") or 0)
        px = resp.get("average_fill_price")
        fee = resp.get("average_fee_paid")
        if fill <= 0 or px is None or fee is None:
            return None
        px, fee = float(px), float(fee)
    except (TypeError, ValueError):
        return None
    got = _kalshi_px_and_fee(px, fee, fill, "kalshi_fee_paid")
    if got is None:
        return None
    _px, fee_pc = got
    return fee_pc * fill


def kalshi_filled_qty(resp) -> float:
    """Contracts actually filled on a Kalshi order, else 0. Handles both response shapes.

    With IOC a thin book partial-fills (fill < count, remainder canceled). Read the REAL fill so
    the paired Poly leg is sized to it instead of assuming all-or-nothing (a stated safety
    invariant: size the second leg from the ACTUAL fill of the first).
    """
    fill, _ = _order_counts(resp)
    return fill if fill is not None else 0.0


class KalshiClient:
    def __init__(self) -> None:
        is_prod = config.KALSHI_ENV == "prod"
        self._api_key  = config.KALSHI_API_KEY
        self._base_url = _PROD_REST if is_prod else _DEMO_REST
        self.ws_url    = _PROD_WS   if is_prod else _DEMO_WS

        self._private_key = None
        if config.KALSHI_PRIVATE_KEY_PATH:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            with open(config.KALSHI_PRIVATE_KEY_PATH, "rb") as f:
                self._private_key = load_pem_private_key(f.read(), password=None)

        # One pooled session reused across requests — avoids a fresh TCP+TLS
        # handshake (~2-3 RTT) on every order. Created lazily in the event loop.
        self._session: aiohttp.ClientSession | None = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, ts_ms: int, method: str, path: str) -> str:
        """Sign `ts_ms + METHOD + path` with the configured private key."""
        if self._private_key is None:
            raise RuntimeError("Kalshi private key not configured (KALSHI_PRIVATE_KEY_PATH)")
        msg = f"{ts_ms}{method}{path}".encode()

        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        if isinstance(self._private_key, Ed25519PrivateKey):
            sig = self._private_key.sign(msg)
        else:
            sig = self._private_key.sign(
                msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256(),
            )
        return base64.b64encode(sig).decode()

    def _headers(self, method: str, path: str) -> dict[str, str]:
        """Return auth headers with a fresh timestamp + signature."""
        ts_ms = int(time.time() * 1000)
        return {
            "KALSHI-ACCESS-KEY":       self._api_key,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts_ms, method, path),
            "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
            "Content-Type":            "application/json",
        }

    def ws_headers(self) -> dict[str, str]:
        """Auth headers for WebSocket connect (signs the WS path)."""
        return self._headers("GET", _WS_PATH)

    # ── REST helpers ──────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create + reuse one pooled session (keep-alive connections)."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the pooled session (call on shutdown)."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self._base_url}{path}"
        session = await self._get_session()
        async with session.get(
            url,
            headers=self._headers("GET", f"{_REST_PATH}{path}"),
            params=params or {},
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _post(self, path: str, body: dict) -> Any:
        url = f"{self._base_url}{path}"
        session = await self._get_session()
        async with session.post(
            url,
            headers=self._headers("POST", f"{_REST_PATH}{path}"),
            json=body,
            timeout=_ORDER_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                # Surface Kalshi's rejection reason — raise_for_status() alone
                # discards the body, which hid order rejections (e.g. invalid
                # price tick) and stranded the paired Poly leg silently.
                detail = await resp.text()
                # A FOK kill is an OUTCOME, NOT AN ERROR: the order reached the matching engine
                # and didn't fill. Return it as a ZERO-FILL response so callers reconcile it
                # through their normal kfill==0 path.
                #
                # ⚠️ This used to `raise`, and that was a naked-leg bug on the DEFAULT exec order.
                # V1 signalled a kill with a BODY (fill_count=0); V2 signals it with a 409. The V2
                # migration propagated that to scripts/order_rtt_probe.py but NOT here, and
                # _exec_poly_first has no try/except:
                #     P = await self._place_poly(...)        # the Poly leg FILLS
                #     kalshi_resp = await create_order(...)  # raised 409 on a kill
                #     await self._unwind_poly_excess(...)    # ← NEVER RAN
                # so a killed Kalshi leg left a naked, UNRECORDED Poly position: no hedge, no
                # strand, no global pause, no alert — on poly_first (the default) and on the MODAL
                # outcome (64.0% kalshi_moved). The old comment here reasoned "Kalshi-first means
                # it strands nothing (caller skips)" — true for kalshi_first, where the raise lands
                # BEFORE any Poly order; false for the default, where it lands AFTER Poly filled.
                if _FOK_KILLED in detail:
                    log.info(f"Kalshi POST {path} → FOK killed (thin book): {detail}")
                    return {"fill_count": 0, "remaining_count": float(body.get("count") or 0),
                            "status": "canceled", "_fok_killed": True}
                # A post_only order that WOULD CROSS is a benign REJECTION, not a break — the venue
                # correctly refused to let a maker quote take. Expected/routine for resting-maker
                # strategies (never on the taker fire path). WARNING not ERROR; still raises so the
                # caller knows the quote wasn't placed.
                if "post only cross" in detail.lower() or "post_only" in detail.lower():
                    log.warning(f"Kalshi POST {path} → post_only would cross — rejected, not placed")
                    raise RuntimeError(f"Kalshi {resp.status} on {path}: {detail}")
                # EVERY other 4xx/5xx is a REAL error (bad tick, insufficient funds, auth) and
                # stays loud — raise_for_status() alone discarded the body, which once hid order
                # rejections and stranded the paired Poly leg silently.
                log.error(f"Kalshi POST {path} → {resp.status}: {detail}")
                raise RuntimeError(f"Kalshi {resp.status} on {path}: {detail}")
            return await resp.json()

    async def _delete(self, path: str) -> Any:
        url = f"{self._base_url}{path}"
        session = await self._get_session()
        async with session.delete(
            url,
            headers=self._headers("DELETE", f"{_REST_PATH}{path}"),
            timeout=_ORDER_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                detail = await resp.text()
                # 404 = the order is already GONE (filled / already cancelled) — benign for a cancel,
                # not a break (cancel_order treats it as success). DEBUG, not ERROR. Real failures
                # (auth / 5xx) stay loud.
                if resp.status == 404:
                    log.debug(f"Kalshi DELETE {path} → 404 already-gone: {detail}")
                else:
                    log.error(f"Kalshi DELETE {path} → {resp.status}: {detail}")
                raise RuntimeError(f"Kalshi {resp.status} on {path}: {detail}")
            return await resp.json()

    # ── Public REST methods ───────────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Return available balance in dollars. API returns cents, so divide by 100."""
        data = await self._get("/portfolio/balance")
        return float(data["balance"]) / 100.0

    async def get_market(self, ticker: str) -> dict:
        """Return market details including yes_bid, no_bid, volume."""
        return await self._get(f"/markets/{ticker}")

    async def get_orderbook(self, ticker: str, depth: int = 10) -> dict:
        """Return bids/asks for a market."""
        return await self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    async def get_events(self, series_ticker: str) -> list[dict]:
        """Return open events for a series, with nested markets."""
        data = await self._get(
            "/events",
            params={
                "series_ticker":       series_ticker,
                "status":              "open",
                "with_nested_markets": "true",
            },
        )
        return data.get("events", [])

    async def fetch_markets_raw(self, series_tickers: list[str]) -> list[dict]:
        """Return raw Kalshi market dicts (incl. floor_strike/cap_strike) across series.

        Reuses get_events but returns the unparsed market dicts so callers can
        read strike fields for scope-boundary checks (e.g. validate_macro_pairs).
        """
        out: list[dict] = []
        for series in series_tickers:
            try:
                events = await self.get_events(series)
            except Exception as e:
                log.error(f"fetch_markets_raw: error fetching {series}: {e}")
                continue
            for ev in events:
                out.extend(ev.get("markets", []))
        return out

    async def create_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price_dollars: str,
        time_in_force: str = "fill_or_kill",
        post_only: bool = False,
    ) -> dict:
        """
        Place an order on Kalshi via the **V2** endpoint (`POST /portfolio/events/orders`).

        The V1 endpoint (`/portfolio/orders`) is DEAD — 410 Gone `deprecated_v1_order_endpoint`
        — and it was found by an RTT probe, not by the bot, because ALL live order placement was
        broken and INVISIBLE: DRY short-circuits before the network, so nothing ever hit the dead
        endpoint. A code path only exercised in production is a path with no test coverage at all.

        The CALLER-FACING signature is deliberately UNCHANGED from V1 — side/action with the price
        in THAT side's own space — so every call site (fire, unwind, ladder) keeps its existing
        limit and tick logic. The V2 translation happens here, at the last moment:

            V1 (side, action, price)      →  V2 (side, price)      [price is ALWAYS the YES price]
            yes / buy   @ p               →  bid @ p
            yes / sell  @ p               →  ask @ p
            no  / buy   @ n               →  ask @ (1-n)     ← the 1-x complement
            no  / sell  @ n               →  bid @ (1-n)     ← the 1-x complement

        V2 is YES-ONLY: `bid` buys YES, `ask` sells YES; there is no `action` and no "buy NO".
        Buying NO is selling YES at the complement — CONFIRMED by the exchange's own fill record
        (we sent {side:"ask", price:"0.3000"} holding zero YES; Kalshi booked side:"no" at
        no_price 0.7000, position_fp=-1.00). See _v2_order_params + tests/test_kalshi_v2_orders.py.

        ⚠️ The complement is the footgun that once "stranded/blocked every NO leg" under V1. It is
        exact ONLY because callers tick-floor in the side's own space first (see _v2_order_params).

        Args:
            ticker:        Market ticker, e.g. "KXNBAGAME-LAKCEL-JUN14"
            side:          "yes" or "no"  (caller-space; translated here)
            action:        "buy" or "sell" (caller-space; translated here)
            count:         Number of contracts (int; serialised as the V2 fixed-point STRING)
            price_dollars: Price of THIS side as a 4-decimal string (yes price for yes, no price
                           for no) — already tick-floored by the caller.
            time_in_force: "fill_or_kill" (default) | "immediate_or_cancel" | "good_till_canceled"

        Returns the raw V2 response — FLAT: {order_id, fill_count, remaining_count,
        average_fill_price, average_fee_paid, ts_ms}. NOTE there is **no `status`** and no `order`
        envelope; use kalshi_filled_qty / kalshi_order_filled, never resp["order"].
        """
        if config.DRY_RUN:
            log.info(
                f"[DRY RUN] Kalshi order: {action} {count}× {ticker} {side} "
                f"@ {side}_price={price_dollars} tif={time_in_force}"
            )
            return {"order": {"status": "dry_run"}}

        v2_side, yes_price = _v2_order_params(side, action, price_dollars)
        body = {
            "ticker":                     ticker,
            "side":                       v2_side,
            "price":                      f"{yes_price:.4f}",   # ALWAYS the YES price
            "count":                      f"{float(count):.2f}",  # V2 wants a fixed-point STRING
            "time_in_force":              time_in_force,
            # REQUIRED in V2. taker_at_cross cancels OUR taker order if it would cross our own
            # resting order — fail-closed. We are taker-only, so this should never trigger.
            "self_trade_prevention_type": "taker_at_cross",
        }
        # Maker-only flag (resting-quote strategies). Added CONDITIONALLY so the FOK/IOC fire path's
        # body is byte-unchanged (post_only defaults False → key absent). A post_only order that would
        # cross the book is rejected by the venue rather than taking — the safe maker primitive.
        if post_only:
            body["post_only"] = True
        return await self._post("/portfolio/events/orders", body)

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel a RESTING order — V2: `DELETE /portfolio/events/orders/{order_id}` (same V2 base as
        create; the V1 `/portfolio/orders/{id}` is DEAD → 410 deprecated_v1_order_endpoint, confirmed
        live via a demo round-trip, which is what caught the V1 path). Only resting-maker
        strategies need this — the FOK/IOC fire path never rests, so it never cancels. DRY
        short-circuits. A wrong path still fails loud (4xx RuntimeError), never silent."""
        if config.DRY_RUN:
            log.info(f"[DRY RUN] Kalshi cancel order {order_id}")
            return {"order_id": order_id, "status": "dry_run_canceled"}
        try:
            return await self._delete(f"/portfolio/events/orders/{order_id}")
        except RuntimeError as e:
            # 404 = the order is already gone (filled / already cancelled). Cancel is idempotent:
            # gone == cancelled → benign success, not an error. Any other failure re-raises loud.
            if "not_found" in str(e) or "Kalshi 404" in str(e):
                return {"order_id": order_id, "status": "already_gone"}
            raise

    async def series_charges_maker_fee(self, series: str) -> bool | None:
        """True if `series` charges a MAKER fee, False if maker-free, None if unreadable.

        THE AUTHORITATIVE SOURCE, and it is NOT the fee-schedule PDF. `/series/{s}` carries a
        `fee_type` field: **`quadratic_with_maker_fees` = maker charged, `quadratic` = maker free**.
        Both halves are confirmed against the live endpoint AND against real fills.

        ⚠️ The discriminator is `fee_type`, NOT `fee_multiplier` — the multiplier is `1` on BOTH
        kinds, so reading it instead silently marks everything as charged.

        ⚠️ WHY THIS EXISTS: the published fee-schedule PDF's maker table is a strict SUBSET of what
        the API reports — the API lists substantially more charged series than the document does. So
        inferring "maker-free" from ABSENCE in the PDF is WRONG, and it did mislabel real series
        (`KXNBAGAME`, `KXNHLGAME` — both `quadratic_with_maker_fees`) as free. A maker strategy built
        on that would pay a fee on every fill it believed was free, which at a tight quoted width is
        the whole edge. Ask the venue; never infer a fee from a document's silence."""
        try:
            r = await self._get(f"/series/{series}")
        except Exception as exc:
            log.warning(f"series_charges_maker_fee({series}) unreadable: {safe_exc(exc)}")
            return None
        d = r.get("series", r) if isinstance(r, dict) else {}
        fee_type = d.get("fee_type") if isinstance(d, dict) else None
        if not isinstance(fee_type, str):
            log.warning(f"series_charges_maker_fee({series}): no fee_type in response — CANNOT VERIFY")
            return None
        return fee_type == "quadratic_with_maker_fees"

    async def get_positions(self) -> list[dict]:
        """Every open MARKET position, following the cursor to the end.

        ⚠️ IT RETURNED `[]` UNCONDITIONALLY, FOREVER. The body was
        `data.get("positions", [])`, but the response is
        `{cursor, event_positions, market_positions}` — there is no top-level `positions` key
        (confirmed against the live endpoint). So the reconciler's Kalshi half reported
        "confirmed flat" on every poll regardless of what we actually held, which is the exact
        fail-open its docstring forbids ("never [] on failure — an empty list means 'confirmed
        flat' and would mask exactly what we're hunting"). Every guard downstream — the isinstance
        check, `_first_qty` returning None, the never-[]-on-failure rule — sat behind this one
        line and never got a chance to run. It had zero callers until the reconciler was built,
        and its tests mock at this boundary, so the parse had never once executed.

        RAISES rather than returning `[]` on any shape it does not recognise, and that asymmetry
        is the whole design: `reconcile._fetch_kalshi_positions` maps an exception to CANNOT-VERIFY
        (alerts, does not pause) and an empty list to "Kalshi confirmed flat" (all clear). A shape
        we cannot read is not evidence of no position, so it must never take the second path. Same
        reason a truncated page raises instead of returning what it managed to read.

        `market_positions` is the right array, not `event_positions`: its items carry `ticker` and
        `position_fp`, which is exactly what the caller parses. Sign is the caller's problem and it
        handles it — Kalshi books a NO position as NEGATIVE `position_fp` (short YES == long NO)
        and `_first_qty` takes the absolute value — which matters, because the NO side is the
        MAJORITY of the directions this bot takes, not a rare case.

        Paginates because the endpoint is cursor-based and page 1 alone would under-read — the
        same trap the Poly side of the reconciler paginates to avoid. `cursor` is `''` when there
        are no more pages.
        """
        out: list[dict] = []
        cursor, pages = "", 0
        while True:
            params: dict = {"limit": _POSITIONS_PAGE_LIMIT}
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/portfolio/positions", params=params)
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"Kalshi positions: expected an object, got {type(data).__name__} — "
                    f"refusing to report flat")
            markets = data.get("market_positions")
            if not isinstance(markets, list):
                raise RuntimeError(
                    f"Kalshi positions: `market_positions` missing or not a list "
                    f"(keys={sorted(data)!r}) — refusing to report flat")
            out.extend(m for m in markets if isinstance(m, dict))
            cursor, pages = data.get("cursor") or "", pages + 1
            if not cursor:
                return out
            if pages >= _POSITIONS_MAX_PAGES:
                raise RuntimeError(
                    f"Kalshi positions: cursor still set after {pages} pages — refusing to "
                    f"report a truncated list as complete")

    async def get_resting_orders(self, tickers: list[str] | None = None) -> list[dict]:
        """Every RESTING (open, unfilled/partially-filled) order, following the cursor to the end.
        Optional `tickers` filters client-side to those markets.

        The safety point of this method: cancel STRAYS. The resting-maker exit cancels the order_ids
        it captured, but a create whose response was LOST (order booked on the venue, reply dropped)
        leaves a resting order with NO captured id — invisible to a tracked-id cancel, and real
        exposure. Asking the venue "what is actually resting?" is the only way to reach it.

        Mirrors get_positions' fail-closed contract EXACTLY, and the envelope is confirmed against
        the live endpoint: `GET /portfolio/orders` → `{cursor, orders}`; items carry `order_id`,
        `ticker`, `status`, `*_count_fp`. `status=resting` filters SERVER-side — verified to
        actually narrow the result, so it is a real filter and not a no-op parameter the API
        ignores. RAISES rather than returning `[]` on any shape it does not recognise —
        a shape we cannot read is NOT evidence of "nothing resting", and treating it as such would
        silently skip the very strays the sweep exists to catch (the same fail-open that `[]`-forever
        made of the reconciler). Cursor is `''` when there are no more pages."""
        out: list[dict] = []
        cursor, pages = "", 0
        while True:
            params: dict = {"status": "resting", "limit": _POSITIONS_PAGE_LIMIT}
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/portfolio/orders", params=params)
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"Kalshi resting orders: expected an object, got {type(data).__name__} — "
                    f"refusing to report nothing-resting")
            orders = data.get("orders")
            if not isinstance(orders, list):
                raise RuntimeError(
                    f"Kalshi resting orders: `orders` missing or not a list "
                    f"(keys={sorted(data)!r}) — refusing to report nothing-resting")
            out.extend(o for o in orders if isinstance(o, dict))
            cursor, pages = data.get("cursor") or "", pages + 1
            if not cursor:
                break
            if pages >= _POSITIONS_MAX_PAGES:
                raise RuntimeError(
                    f"Kalshi resting orders: cursor still set after {pages} pages — refusing to "
                    f"report a truncated list as complete")
        if tickers is not None:
            keep = set(tickers)
            out = [o for o in out if o.get("ticker") in keep]
        return out

    async def get_fills(self, *, min_ts: int | None = None,
                        tickers: list[str] | None = None) -> list[dict]:
        """Real executed FILLS (the authoritative economics record), following the cursor to the end.
        `min_ts` (epoch seconds) scopes to a run; `tickers` filters client-side.

        Unlike position deltas — which only reveal that inventory MOVED, valued at the mid — a fill
        record carries the TRUTH the maker thesis turns on — confirmed field-by-field against the
        live endpoint: the actual `yes_price_dollars`/`no_price_dollars` we filled at, the real
        `fee_cost`, `is_taker`
        (a `true` on a post_only quote would mean a maker leaked into a take), and `ts`. Envelope is
        `{cursor, fills}`.

        Empty is LEGITIMATE here (no fills yet) — distinct from the positions/orders reads, where
        `[]` was a fail-open. But an UNRECOGNISED shape still RAISES: a fills read we cannot parse must
        not silently read as 'no fills' and understate realized cost. `min_ts` is sent to the API to
        cut volume; the caller ALSO dedupes by `fill_id`, so a server that ignores the param stays
        correct (just noisier)."""
        out: list[dict] = []
        cursor, pages = "", 0
        while True:
            params: dict = {"limit": _POSITIONS_PAGE_LIMIT}
            if min_ts is not None:
                params["min_ts"] = int(min_ts)
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/portfolio/fills", params=params)
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"Kalshi fills: expected an object, got {type(data).__name__} — "
                    f"refusing to understate realized fills")
            fills = data.get("fills")
            if not isinstance(fills, list):
                raise RuntimeError(
                    f"Kalshi fills: `fills` missing or not a list (keys={sorted(data)!r}) — "
                    f"refusing to understate realized fills")
            out.extend(f for f in fills if isinstance(f, dict))
            cursor, pages = data.get("cursor") or "", pages + 1
            if not cursor:
                break
            if pages >= _POSITIONS_MAX_PAGES:
                raise RuntimeError(
                    f"Kalshi fills: cursor still set after {pages} pages — refusing to report a "
                    f"truncated list as complete")
        if tickers is not None:
            keep = set(tickers)
            out = [f for f in out if f.get("ticker") in keep or f.get("market_ticker") in keep]
        return out
