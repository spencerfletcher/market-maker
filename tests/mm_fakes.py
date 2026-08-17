"""Fakes for driving `bot/kalshi/maker.py` in a test.

WHY THIS EXISTS. The maker tool's order lifecycle — quote, cancel, fill-attribution, markout — LIVED
in closures inside a 720-line `main()` (since extracted to `MakerSession`), so the only way
anyone had managed to "test" it was by string-matching its own source. Three such tests were written and every one passed against an
inverted branch. Four consecutive attempts at a single P&L defect each shipped broken. That is a
property of the file's structure, and these fakes are the first half of fixing it: they make a whole
quote→fill→cancel cycle drivable without touching a venue.

⚠️ DRY MODE IS NOT A REHEARSAL, which is exactly why these fakes must support REAL mode.
`KalshiClient.create_order` in DRY returns `{"order": {"status": "dry_run"}}` with **no order_id**,
so `resting`, `order_meta`, `_cancel_all`, `_queue_ahead` and `_log_unfilled` never execute, and the
fills/markout machinery is `if real`-gated. A DRY preview always prints `quotes=0 rejects=0 fills=0`
— byte-identical to two real runs that placed nothing because of an unrelated feed bug. So these
fakes drive the tool with `real=True` against a venue that is entirely in-memory.

Everything here is scriptable rather than clever: a test says what the venue returns, including the
failure modes that have actually bitten (a 429 on cancel, the `already_gone` 404, a fills read that
raises). Nothing here infers or simulates market behaviour — that would be a model, and a model in a
fixture is how you confirm your own beliefs rather than test them.
"""
from __future__ import annotations

import csv
import itertools
import json


class FakeClock:
    """A clock the test advances explicitly.

    The tool calls `time.time()` 23 times and its loop runs `while time.time() - t0 < seconds`, so a
    real clock makes cycle counts depend on machine speed. Timestamps also land in every CSV row, so
    a fake clock is what makes an output comparison reproducible at all."""

    def __init__(self, start: float = 1_700_000_000.0, step: float = 0.0) -> None:
        self.now = start
        self.step = step

    def time(self) -> float:
        self.now += self.step
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeBook:
    """Stands in for `KalshiOrderBookCache`.

    ⚠️ `get_depth(side)` is the OPPOSITE ladder — it means "size available to BUY that side", so
    `get_depth(t, "no")` is the YES-BID depth and `get_depth(t, "yes")` the YES-ASK depth — the
    real cache does the same. Mapping them the obvious way inverts OBI, which has already
    produced one false null in this repo. The fake preserves the venue's confusing convention on
    purpose: a fake that fixes the naming would let a caller's inversion bug pass."""

    def __init__(self, books: dict[str, tuple[float | None, float | None, float, float]]) -> None:
        # ticker -> (yes_bid, yes_ask, yes_bid_depth, yes_ask_depth)
        self.books = dict(books)
        self.set_tickers_calls: list = []
        self.ran = False
        self.level_observer = None

    def set(self, ticker, bid, ask, bid_depth=100.0, ask_depth=100.0) -> None:
        self.books[ticker] = (bid, ask, bid_depth, ask_depth)

    def one_sided(self, ticker, *, keep: str = "bid") -> None:
        """Make a book unpriceable the way the venue really does it.

        `feed._derive` FABRICATES the missing side: an empty NO ladder yields `yes_ask = 1.0` and an
        empty YES ladder `yes_bid = 0.0`. Both are positive and finite, which is why a `bid > 0 and
        ask > 0` check waves them through — the defect behind four mis-valuations."""
        b, a, bd, ad = self.books[ticker]
        self.books[ticker] = (b, 1.0, bd, 0.0) if keep == "bid" else (0.0, a, 0.0, ad)

    def get_best_bid(self, t):
        return self.books.get(t, (None, None, 0.0, 0.0))[0]

    def get_best_ask(self, t):
        return self.books.get(t, (None, None, 0.0, 0.0))[1]

    def get_depth(self, t, side):
        b = self.books.get(t)
        if b is None:
            return None
        return b[2] if side == "no" else b[3]      # see the class docstring: opposite ladder

    def set_book_tickers(self, tickers):
        self.set_tickers_calls.append(list(tickers))

    def set_level_observer(self, cb):
        """Record the per-level observer the maker registers.

        Kept as state rather than ignored so a test can drive real level events through whatever
        the maker attached, and so the teardown's detach (`set_level_observer(None)`) is
        observable — an observer left attached to a live feed after its consumer is gone is the
        leak this ordering exists to prevent."""
        self.level_observer = cb

    @property
    def level_observer_errors(self) -> dict:
        return {}

    async def run_forever(self):
        """Park forever WITHOUT sleeping.

        ⚠️ Deliberately not `await asyncio.sleep(3600)`. Under a virtual clock a sleeping background
        task participates in time, so a 3600s park dragged the fake clock an hour forward on the
        first tick and `main()`'s `while time.time() - t0 < seconds` loop exited before it had
        quoted once — a fixture silently reducing every end-to-end run to zero cycles. A fake book
        is static; it has no reason to consume time at all."""
        self.ran = True
        import asyncio
        await asyncio.Event().wait()


class FakeTradeFeed:
    """Stands in for `KalshiTradeFeed`.

    ⚠️ IT MUST NOT CONSUME VIRTUAL TIME — the same trap `FakeBook.run_forever` documents, reached by
    a different route. The real feed reconnects on a 3s sleep, and against a fake client whose first
    connect fails that is an endless reconnect loop; under the virtual clock each sleep DRAGS THE
    CLOCK, which inserted a 3s gap between the fill poll and the cancel and went red on the
    fill-poll-ordering invariant. The production ordering was untouched — a background task sleeping
    does not move wall time — so it was purely the fixture, and a fixture that can manufacture a
    failure in an unrelated invariant can also mask a real one.

    `subscribed` defaults True so the queue-attribution path is exercised in its working state; flip
    it to False in a test that wants the fail-loud UNKNOWN_TAPE_DOWN behaviour."""

    def __init__(self, client=None, tickers=None, on_trade=None) -> None:
        self.client = client
        self.tickers = list(tickers or [])
        self.on_trade = on_trade
        self.subscribed = True
        self.n_trades = 0
        self.n_dropped = 0
        self.n_bad_trades = 0
        self.n_callback_errors = 0
        self.n_reconnects = 0
        self.last_error = None
        self.errors: dict[str, int] = {}
        self.unknown_frames: list[str] = []
        self.ran = False

    def feed(self, ticker, yes_price, qty, ts=None) -> None:
        """Push one print through the callback the maker registered, as the real socket would."""
        from decimal import Decimal
        self.n_trades += 1
        self.on_trade({"ticker": ticker, "yes_price": Decimal(str(yes_price)),
                       "qty": Decimal(str(qty)), "ts": ts, "raw": {}})

    async def run_forever(self):
        self.ran = True
        import asyncio
        await asyncio.Event().wait()      # park without participating in time — see the docstring


class FakeKalshiClient:
    """In-memory Kalshi. Records every call so a test can assert on ORDERING, which is where the
    real defects have been (fills polled after the cancel that depended on them)."""

    def __init__(self, *, balance: float = 500.0, positions=None,
                 maker_free: bool = True, orderbook=None, clock=None) -> None:
        self._base_url = "https://external-api.kalshi.com/trade-api/v2"
        # Optional clock: when set, every logged call is timestamped in `call_times`, so a test can
        # assert WHEN a call happened relative to another rather than merely that it came earlier.
        # Ordering alone cannot express "no unpolled fill window precedes a cancel" — see
        # `at_call` and the fill/cancel ordering test.
        self.clock = clock
        self.call_times: list[float] = []
        self.balance = balance
        self.positions = list(positions or [])
        self.maker_free = maker_free
        # ⚠️ THE VENUE WRAPS THE BOOK. Prod returns `{"orderbook_fp": {...}}` and `_two_sided` reads
        # `ob.get("orderbook_fp") or ob.get("orderbook")`. An earlier version of this fake returned
        # the INNER dict, so `_two_sided` saw `{}`, every market failed the contested-mid gate, and
        # `main()` selected nothing — while the unit test "proving" the fake parsed correctly passed
        # the inner dict straight to `_book_mid` and so only confirmed the belief.
        # ⚠️ SHAPED LIKE THE REAL VENUE, not like the code that reads it — see the captured response
        # at tests/test_detect_probe.py:_REAL_BOOK, which is the evidence for every property here:
        #   · prices are 4-dp STRINGS and quantities 2-dp STRINGS — never floats. A float fixture
        #     never exercises the string→Decimal parse the repo's Decimal rule requires.
        #   · quantities are FRACTIONAL ("326.89"), which the venue independently confirmed by
        #     partial-filling a 1-lot at 0.11 contracts.
        #   · levels arrive ASCENDING, so the BEST bid is the LAST element. The earlier single-level
        #     fixture made `lvl[0]` and `max(...)` indistinguishable, so a reader that took the
        #     first level instead of the best would have passed.
        # Best yes bid 0.40, best no bid 0.55 ⇒ yes ask 0.45 ⇒ mid 0.425, as before.
        self.orderbook = {"orderbook_fp": orderbook or {
            "yes_dollars": [["0.3600", "5.00"], ["0.3800", "120.00"], ["0.4000", "500.00"]],
            "no_dollars": [["0.5100", "8.00"], ["0.5300", "210.00"], ["0.5500", "326.89"]]}}
        self.calls: list[str] = []                 # ordered call log — the point of the fake
        self.orders: dict[str, dict] = {}          # order_id -> {ticker, action, price, resting}
        self.fills: list[dict] = []
        self._ids = itertools.count(1)
        # Scripted failures, because the money bugs live on these paths and not the happy one.
        self.fail_create: Exception | None = None
        self.fail_cancel: Exception | None = None
        self.fail_fills: Exception | None = None
        self.fail_positions: Exception | None = None
        self.cancel_returns_already_gone = False

    def _log(self, call: str) -> None:
        """Record a call and, when a clock is wired, WHEN it happened."""
        self.calls.append(call)
        self.call_times.append(self.clock.time() if self.clock is not None else 0.0)

    def at_call(self, index: int) -> float:
        """Virtual time of the call at `index`. Every call inside one quote cycle shares a
        timestamp (the tool sleeps only at the end of a cycle), so comparing two timestamps asks
        'same cycle or not?' — which is the question ordering alone cannot answer."""
        return self.call_times[index]

    # ── the surface main() actually touches ──────────────────────────────────────────────────
    async def get_balance(self):
        self._log("get_balance")
        return self.balance

    async def get_positions(self):
        self._log("get_positions")
        if self.fail_positions:
            raise self.fail_positions
        return list(self.positions)

    async def get_resting_orders(self, tickers=None):
        """⚠️ HONOURS `tickers`, because that argument is the stray sweep's ONLY scoping.

        An earlier version ignored it, which made the scoping untestable: mutating the teardown's
        `get_resting_orders(targets)` to an unscoped call survived the whole suite, even though an
        unscoped sweep would cancel resting orders in markets this run never touched. The real
        client filters client-side on `ticker` [client.py:get_resting_orders]."""
        self._log("get_resting_orders" if tickers is None else "get_resting_orders:scoped")
        want = None if tickers is None else set(tickers)
        return [{"order_id": oid, "ticker": o["ticker"]}
                for oid, o in self.orders.items()
                if o["resting"] and (want is None or o["ticker"] in want)]

    async def get_fills(self, min_ts=None, tickers=None):
        self._log("get_fills")
        if self.fail_fills:
            raise self.fail_fills
        return list(self.fills)

    async def get_orderbook(self, ticker):
        self._log("get_orderbook")
        return self.orderbook

    async def series_charges_maker_fee(self, series):
        self._log("series_charges_maker_fee")
        return not self.maker_free

    async def create_order(self, ticker, side, action, count, price, **kw):
        """⚠️ The call log records COUNT, POST_ONLY and the ORDER ID, not just side and price.

        An earlier version logged only `create:{ticker}:{action}@{price}`, and the omission was not
        cosmetic: mutating `args.size` to `args.size * 10` (10× the order size) and dropping
        `post_only=True` (the module header's rail #1 — the maker silently becomes a taker) BOTH
        survived the entire test suite, because no assertion could see either value. The order id is
        here so a test can tie a `cancel:` back to the `create:` that placed it, which is what makes
        the fill/cancel ordering invariant checkable per-order rather than per-run."""
        if self.fail_create:
            self._log(f"create:{ticker}:{action}@{price} FAILED")
            raise self.fail_create
        oid = f"oid-{next(self._ids):04d}"
        self.orders[oid] = {"ticker": ticker, "action": action, "price": float(price),
                            "count": count, "resting": True}
        self._log(f"create:{ticker}:{action}@{price} x{count} "
                          f"po={kw.get('post_only')} tif={kw.get('time_in_force')} oid={oid}")
        # CAPTURED FROM A REAL VENUE RESPONSE (a post_only order, placed and cancelled):
        #   {"fill_count": "0.00", "order_id": "...", "remaining_count": "1.00", "ts_ms": 1784526831142}
        # ⚠️ Two corrections to what this repo believed. The counts are STRINGS, not ints — and
        # `average_fill_price`/`average_fee_paid` are ABSENT when nothing filled, though
        # client.py:create_order's docstring lists them unconditionally. Flat, with no `status` and
        # no `order` envelope. Returning only `order_id` (the earlier fake) made an
        # immediately-filled create unrepresentable.
        return {"order_id": oid, "fill_count": "0.00",
                "remaining_count": f"{float(count):.2f}", "ts_ms": 0}

    async def cancel_order(self, oid):
        self._log(f"cancel:{oid}")
        if self.fail_cancel:
            raise self.fail_cancel
        count = self.orders[oid]["count"] if oid in self.orders else 0
        if oid in self.orders:
            self.orders[oid]["resting"] = False
        # The real client maps a 404 to this, which is the venue saying the order was no longer
        # resting — i.e. it filled, or was already gone. It is a CLIENT SYNTHESIS, not a venue field.
        if self.cancel_returns_already_gone:
            return {"status": "already_gone"}
        # CAPTURED FROM A REAL VENUE RESPONSE (DELETE /portfolio/events/orders/{id} on a resting
        # order): {"order_id": "...", "reduced_by": "1.00", "ts_ms": 1784526831172}.
        # ⚠️ THERE IS NO `status` FIELD ON A SUCCESSFUL CANCEL. `MakerSession.cancel_all` branches on
        # `r.get("status")` under a comment reading "READ THE VENUE, don't infer" — on the success
        # path the venue sends no such field, so that branch is reachable only via the client's own
        # 404→already_gone synthesis. `reduced_by` is the signal the venue actually gives: how much
        # was still resting when the cancel landed, so `reduced_by < initial` means it partially
        # filled first. That is strictly more information than the boolean now in use.
        return {"order_id": oid, "reduced_by": f"{float(count):.2f}", "ts_ms": 0}

    async def close(self):
        self._log("close")

    # ── test helpers ─────────────────────────────────────────────────────────────────────────
    def fill(self, order_id: str, *, price: float | None = None, count: float = 1.0,
             ts: float = 1_700_000_000.0, fee: float = 0.0, taker: bool = False,
             side: str | None = None) -> dict:
        """Fill a resting order: append a venue fill record AND move the position, the way a real
        fill does both. A fake that moved only one of them would let a reconciliation bug pass."""
        o = self.orders[order_id]
        px = o["price"] if price is None else price
        o["resting"] = False
        # ⚠️ A yes-space SELL books as side="no" on the real venue — short YES IS long NO. Gating on
        # `side == "yes"` therefore recorded NOTHING for sells; direction must come from `action`.
        # Default reproduces that convention rather than the convenient one.
        rec = {"fill_id": f"fill-{len(self.fills) + 1}", "order_id": order_id,
               "ticker": o["ticker"], "market_ticker": o["ticker"],
               "side": side or ("no" if o["action"] == "sell" else "yes"), "action": o["action"], "count_fp": f"{count:.2f}",
               "yes_price_dollars": f"{px:.4f}", "no_price_dollars": f"{1 - px:.4f}",
               "fee_cost": f"{fee:.6f}", "is_taker": taker, "ts": ts}
        self.fills.append(rec)
        signed = count if o["action"] == "buy" else -count
        # ⚠️ COST BASIS. Kalshi fully collateralizes: a yes-BUY costs the yes price, a yes-SELL is a
        # long NO and costs the NO price (1 − yes). Verified against a real venue record — a 0.11
        # sell at yes 0.85 returned `maker_fill_cost_dollars 0.016500` = 0.11 × 0.15 exactly.
        # Adding to the basis when the position grows and reducing it PRO RATA when it shrinks is a
        # simple model, not the venue's own bookkeeping — the venue field stays authoritative for
        # any real number. It exists so the fixture cannot report a zero basis for a live position.
        old = self.position_of(o["ticker"])
        new = old + signed
        basis = self.exposure_of(o["ticker"])
        if old == 0 or (old > 0) == (signed > 0):          # opening or adding
            basis += count * (px if o["action"] == "buy" else 1.0 - px)
        elif old != 0:                                      # reducing: release basis pro rata
            basis = max(0.0, basis * (1.0 - min(count / abs(old), 1.0)))
        self.set_position(o["ticker"], new, exposure=basis)
        return rec

    def position_of(self, ticker: str) -> float:
        for p in self.positions:
            if p.get("ticker") == ticker:
                return float(p.get("position_fp", 0.0))
        return 0.0

    def exposure_of(self, ticker: str) -> float:
        for p in self.positions:
            if p.get("ticker") == ticker:
                return float(p.get("market_exposure_dollars", 0.0))
        return 0.0

    def set_position(self, ticker: str, qty: float, exposure: float | None = None) -> None:
        """⚠️ `market_exposure_dollars` IS THE COST BASIS AND THE MAX LOSS — never leave it at zero.

        An earlier version defaulted it to 0.0 and, on the UPDATE path, never touched it at all — so
        a fake position of 3 contracts bought at 0.41 reported a cost basis of zero, and the
        harness's own `positions_raw` row said exactly that. This field is the max loss and is to
        be READ from the venue, not computed; a fixture hardcoding it to zero pre-installs the
        mis-valuation class every position-value review exists to catch. Pass it explicitly, or let
        `fill()` accumulate it."""
        for p in self.positions:
            if p.get("ticker") == ticker:
                p["position_fp"] = f"{qty:.2f}"
                if exposure is not None:
                    p["market_exposure_dollars"] = f"{exposure:.6f}"
                return
        self.positions.append({"ticker": ticker, "position_fp": f"{qty:.2f}",
                               "market_exposure_dollars": f"{(exposure or 0.0):.6f}",
                               "realized_pnl_dollars": "0.000000",
                               "fees_paid_dollars": "0.000000",
                               "total_traded_dollars": "0.000000",
                               "last_updated_ts": 0})

    def resting_ids(self) -> list[str]:
        return [oid for oid, o in self.orders.items() if o["resting"]]

    def calls_of(self, kind: str) -> list[str]:
        return [c for c in self.calls if c.startswith(kind)]


class FakeMarket:
    """Minimal `KalshiMarket` — the tool reads only `.ticker` and `.volume_24h`, plus
    `expected_expiration_time` via `_expiry_ts` for phase classification."""

    def __init__(self, ticker: str, *, volume_24h: float = 1000.0,
                 expected_expiration_time: str = "") -> None:
        self.ticker = ticker
        self.volume_24h = volume_24h
        self.expected_expiration_time = expected_expiration_time
        self.price_tick = 0.01


class FakeScanner:
    def __init__(self, markets):
        self._markets = list(markets)

    def __call__(self, _client):
        return self

    async def fetch_markets(self, series):
        return list(self._markets)


class RecordingWriter:
    """Captures CSV rows in memory. `rows_of("end")` beats grepping a file on disk."""

    def __init__(self):
        self.rows: list[list] = []

    def writerow(self, row):
        self.rows.append(list(row))

    def rows_of(self, event: str) -> list[list]:
        return [r for r in self.rows if len(r) > 1 and r[1] == event]

    def events(self) -> list[str]:
        return [r[1] for r in self.rows if len(r) > 1]
