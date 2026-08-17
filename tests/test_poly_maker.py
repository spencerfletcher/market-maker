"""Tests for the Polymarket US maker (`bot/poly_us/maker.py`) and its CLI shim.

Every test here pins a rule that traces to a MEASUREMENT, not a preference. Five of them are
mutation-tested — the fix was reverted and the test required to go RED, because a green suite
is not the same as a pinned fix:

  · the size floor          — a size-1 fill's rebate rounds to zero at EVERY price
  · the cap-in-fills maths  — a contract cap binds after 4 fills at size 10 but 55+ at size 1
  · reduce-only-on-cap      — at the cap the ADDING side stops and the REDUCING side keeps quoting
  · requote-only-on-change  — an unchanged quote keeps its queue position; a reprice forfeits it
  · the arming gate         — DRY_RUN=false with no flag must be a HARD STOP, never a downgrade

A test that only asserts "the guard exists" is not a pin. Where the rule is a boundary (`>=` vs
`>`), the boundary case itself is asserted, because a guard written with a strict inequality is one
contract away from not firing.
"""
from __future__ import annotations

import asyncio
import time
import types
from decimal import Decimal

import pytest

from bot.poly_us import maker


@pytest.fixture(autouse=True)
def _tapes_to_tmp(tmp_path, monkeypatch):
    """⛔ Redirect every default tape into tmp for the whole module.

    Found by running the suite alongside a live shadow run: the tests were appending
    fabricated one-market rows into `logs/poly_live_mm_cycles.csv` — the same file the real run was
    writing — interleaved by timestamp and indistinguishable afterwards. A test that silently
    contaminates production data is worse than a missing test.
    """
    monkeypatch.setattr(maker, "DEFAULT_QUOTE_CSV", str(tmp_path / "quotes.csv"))
    monkeypatch.setattr(maker, "DEFAULT_CYCLE_CSV", str(tmp_path / "cycles.csv"))
    monkeypatch.setattr(maker, "DEFAULT_FILL_CSV", str(tmp_path / "fills.csv"))


def test_the_default_tape_paths_are_resolved_absolutely_not_relative_to_the_cwd():
    """A relative operational default is a known, expensive defect in this repo: the loss caps
    read `logs/execution_pnl.csv` relative to the cwd, so a launch from any other directory gives
    FileNotFoundError → 0.0 loss → both caps permanently and silently inert.

    Asserted against the SOURCE rather than the attribute, because the autouse fixture above
    deliberately replaces the attribute."""
    import pathlib
    import re

    src = (pathlib.Path(__file__).resolve().parents[1] / "bot" / "poly_us" / "maker.py").read_text()
    for name in ("DEFAULT_QUOTE_CSV", "DEFAULT_CYCLE_CSV", "DEFAULT_FILL_CSV"):
        assert re.search(rf"^{name} = durable\.repo_path\(", src, re.M), name


# ── the size floor ───────────────────────────────────────────────────────────────────────────

def test_size_one_is_refused_because_its_rebate_rounds_to_zero_everywhere():
    reason = maker.size_refusal(1)
    assert reason is not None
    assert "5" in reason


def test_size_four_is_refused_and_size_five_is_accepted():
    assert maker.size_refusal(4) is not None
    assert maker.size_refusal(5) is None
    assert maker.size_refusal(25) is None


def test_size_refusal_names_the_rounding_boundary_not_just_the_number():
    """The reason has to carry the arithmetic. An operator who is told only 'minimum 5' will raise
    it back to 1 the first time capital feels tight."""
    reason = maker.size_refusal(1)
    assert "0.005" in reason
    assert "0.003125" in reason        # the size-1 maximum, at p=0.50


def test_size_zero_and_negative_are_refused():
    assert maker.size_refusal(0) is not None
    assert maker.size_refusal(-5) is not None


def test_predicted_rebate_is_the_documented_formula():
    # 0.0125 * p * (1-p) * C, exact in Decimal.
    assert maker.predicted_rebate(Decimal("0.50"), 1) == Decimal("0.003125")
    assert maker.predicted_rebate(Decimal("0.50"), 5) == Decimal("0.015625")


def test_size_one_rebate_is_below_the_cent_boundary_at_the_most_favourable_price():
    """This is the measurement the floor rests on — assert it rather than trusting the comment."""
    best_case = maker.predicted_rebate(Decimal("0.50"), 1)
    assert best_case < maker.NEAREST_CENT_BOUNDARY


def test_size_five_clears_the_boundary_across_the_liquid_range_but_not_at_the_extremes():
    """⚠️ The design doc says size 5 'clears everywhere'. It does not — it clears p in about
    [0.10, 0.90]. Pinned so the overstatement cannot be re-derived from the doc."""
    for p in ("0.10", "0.25", "0.50", "0.75", "0.90"):
        assert not maker.rebate_rounds_to_zero(Decimal(p), 5)
    for p in ("0.02", "0.05", "0.96"):
        assert maker.rebate_rounds_to_zero(Decimal(p), 5)


def test_min_rebate_size_grows_toward_the_price_extremes():
    assert maker.min_rebate_size(Decimal("0.50")) == 2
    assert maker.min_rebate_size(Decimal("0.05")) > 5


# ── the inventory cap, expressed in FILLS ────────────────────────────────────────────────────

def test_cap_contracts_is_size_times_fills():
    assert maker.cap_contracts(size=5, cap_fills=3) == 15
    assert maker.cap_contracts(size=25, cap_fills=3) == 75


def test_cap_in_fills_is_size_invariant_which_is_the_entire_point():
    """A CONTRACT cap binds after 4 fills at size 10 and 55+ at size 1 — that asymmetry was the
    fake 23x size ramp. In FILLS the number of fills to cap is the same at every size."""
    for size in (5, 10, 25):
        assert maker.cap_contracts(size=size, cap_fills=3) // size == 3


def test_cap_fills_must_be_positive():
    with pytest.raises(ValueError):
        maker.cap_contracts(size=5, cap_fills=0)


# ── reduce-only at the cap ───────────────────────────────────────────────────────────────────

def test_below_the_cap_both_sides_quote():
    assert maker.sides_allowed(Decimal("0"), 15) == (True, True)
    assert maker.sides_allowed(Decimal("10"), 15) == (True, True)
    assert maker.sides_allowed(Decimal("-10"), 15) == (True, True)


def test_at_the_long_cap_the_bid_stops_and_the_ask_keeps_quoting():
    """The ask REDUCES a long, so it must keep quoting — stopping both sides at the cap leaves the
    inventory parked with nothing working to take it off."""
    allow_bid, allow_ask = maker.sides_allowed(Decimal("15"), 15)
    assert allow_bid is False
    assert allow_ask is True


def test_at_the_short_cap_the_ask_stops_and_the_bid_keeps_quoting():
    allow_bid, allow_ask = maker.sides_allowed(Decimal("-15"), 15)
    assert allow_bid is True
    assert allow_ask is False


def test_the_cap_is_inclusive_at_exactly_the_cap():
    """`>=`, not `>`. A guard one contract away from firing is a guard that does not fire."""
    assert maker.sides_allowed(Decimal("15"), 15)[0] is False
    assert maker.sides_allowed(Decimal("14"), 15)[0] is True


def test_beyond_the_cap_still_reduces_rather_than_stopping_everything():
    """Inventory can overshoot the cap on a partial fill race. Overshoot must not stop the side
    that unwinds it."""
    assert maker.sides_allowed(Decimal("40"), 15) == (False, True)
    assert maker.sides_allowed(Decimal("-40"), 15) == (True, False)


def test_a_zero_cap_blocks_both_adding_sides():
    assert maker.sides_allowed(Decimal("0"), 0) == (False, False)


# ── requote only when OUR price moves ────────────────────────────────────────────────────────

def test_an_unchanged_quote_is_held_and_keeps_its_queue_position():
    """Queue position is the dominant variable: the one fill measured came at queue-depth 2 while
    queue 376 and 1681 both went unfilled. Amending to reprice forfeits it."""
    assert maker.quote_action(Decimal("0.44"), Decimal("0.44")) == maker.HOLD


def test_a_moved_price_is_a_cancel_replace():
    assert maker.quote_action(Decimal("0.44"), Decimal("0.45")) == maker.REPLACE
    assert maker.quote_action(Decimal("0.45"), Decimal("0.44")) == maker.REPLACE


def test_no_resting_order_and_a_desired_price_is_a_place():
    assert maker.quote_action(None, Decimal("0.44")) == maker.PLACE


def test_a_resting_order_with_no_desired_price_is_a_cancel():
    """The side has gone un-quotable (capped, unreadable touch, market closed). Leaving the order
    resting would keep taking fills on a side we have decided to stop quoting."""
    assert maker.quote_action(Decimal("0.44"), None) == maker.CANCEL


def test_nothing_resting_and_nothing_desired_is_a_hold():
    assert maker.quote_action(None, None) == maker.HOLD


def test_equal_prices_written_differently_still_hold():
    """`Decimal('0.4400') == Decimal('0.44')` is True; a string compare would not be, and would
    forfeit the queue on every cycle."""
    assert maker.quote_action(Decimal("0.4400"), Decimal("0.44")) == maker.HOLD


# ── the quote rule (join, or improve one tick with >=2 ticks of room) ─────────────────────────

def test_pick_quotes_joins_a_one_tick_book():
    my_bid, my_ask, imp_b, imp_a = maker.pick_quotes(
        Decimal("0.44"), Decimal("0.45"), Decimal("0.01"))
    assert (my_bid, my_ask) == (Decimal("0.44"), Decimal("0.45"))
    assert imp_b is False and imp_a is False


def test_pick_quotes_joins_an_exactly_two_tick_book_rather_than_locking_itself():
    my_bid, my_ask, imp_b, imp_a = maker.pick_quotes(
        Decimal("0.44"), Decimal("0.46"), Decimal("0.01"))
    assert (my_bid, my_ask) == (Decimal("0.44"), Decimal("0.46"))
    assert imp_b is False and imp_a is False


def test_pick_quotes_improves_both_sides_with_three_ticks_of_room():
    my_bid, my_ask, imp_b, imp_a = maker.pick_quotes(
        Decimal("0.44"), Decimal("0.47"), Decimal("0.01"))
    assert (my_bid, my_ask) == (Decimal("0.45"), Decimal("0.46"))
    assert imp_b is True and imp_a is True


def test_pick_quotes_never_improves_only_one_side():
    """One-sided improvement buys with sole queue priority while selling from the back — it
    accumulates a systematic long and stops measuring anything (shadow_mm_probe v4's correction)."""
    for spread_ticks in range(1, 12):
        bid = Decimal("0.40")
        ask = bid + spread_ticks * Decimal("0.01")
        _, _, imp_b, imp_a = maker.pick_quotes(bid, ask, Decimal("0.01"))
        assert imp_b == imp_a


def test_pick_quotes_refuses_an_unusable_touch_or_tick():
    with pytest.raises(ValueError):
        maker.pick_quotes(Decimal("0.45"), Decimal("0.44"), Decimal("0.01"))     # crossed
    with pytest.raises(ValueError):
        maker.pick_quotes(Decimal("0.44"), Decimal("0.44"), Decimal("0.01"))     # locked
    with pytest.raises(ValueError):
        maker.pick_quotes(Decimal("0.44"), Decimal("0.45"), Decimal("0"))        # no tick


# ── queue-ahead, the column the whole lane's economics is read from ──────────────────────────

_BOOK = {"marketData": {
    "bids": [{"px": "0.44", "qty": "300"}, {"px": "0.44", "qty": "76"},
             {"px": "0.43", "qty": "1000"}],
    "offers": [{"px": "0.46", "qty": "500"}, {"px": "0.47", "qty": "20"}],
}}


def test_queue_ahead_sums_every_level_at_our_price_not_just_the_first():
    md = _BOOK["marketData"]
    assert maker.qty_at_price(md, "bid", Decimal("0.44")) == Decimal("376")


def test_queue_ahead_is_zero_when_we_improve_inside_the_touch():
    """An improved quote rests alone at the front — queue-ahead 0, which is the whole reason to
    record the improved/joined bit alongside it."""
    md = _BOOK["marketData"]
    assert maker.qty_at_price(md, "bid", Decimal("0.45")) == Decimal("0")


def test_queue_ahead_reads_the_ask_side_for_an_ask():
    md = _BOOK["marketData"]
    assert maker.qty_at_price(md, "ask", Decimal("0.46")) == Decimal("500")
    assert maker.qty_at_price(md, "ask", Decimal("0.44")) == Decimal("0")


def test_queue_ahead_is_none_when_the_book_is_unreadable():
    """None means 'we could not tell', never 0 — a fabricated 0 would read as front-of-queue."""
    assert maker.qty_at_price(None, "bid", Decimal("0.44")) is None
    assert maker.qty_at_price({"bids": "nonsense"}, "bid", Decimal("0.44")) is None


# ── teardown order ───────────────────────────────────────────────────────────────────────────

def test_teardown_order_is_cancel_reconcile_flatten_sweep():
    """Sweeping before flattening re-reads a book the flatten is about to move (the Kalshi maker
    had that order backwards in four places). reconcile_pending sits BETWEEN cancel-all and the
    flatten: cancel-all can park blind-cancelled orders, and the flatten sizes off the inventory
    those parked fills belong to — flattening first would size the disposal against a belief the
    next 90 seconds could prove wrong, which is how a live flatten once tried to sell contracts
    the account did not hold short."""
    assert maker.TEARDOWN_PHASES == ("cancel_all", "reconcile_pending", "flatten", "sweep")


def test_the_sweep_is_last():
    assert maker.TEARDOWN_PHASES[-1] == "sweep"


# ── the arming gate ──────────────────────────────────────────────────────────────────────────

def test_dry_run_false_without_the_flag_is_a_hard_stop():
    """The dangerous combination: orders are gated by DRY_RUN alone, so a .env carrying
    DRY_RUN=false with no flag would place REAL quotes while reporting a shadow/preview run."""
    reason = maker.arming_refusal(dry_run=False, flagged=False)
    assert reason is not None
    assert "--i-understand-real-money" in reason


def test_the_flag_with_dry_run_false_is_coherent_and_arms():
    assert maker.arming_refusal(dry_run=False, flagged=True) is None


def test_the_flag_without_dry_run_false_is_not_refused():
    """The 'I forgot to configure it' case cannot move money, and main() prints a DRY banner."""
    assert maker.arming_refusal(dry_run=True, flagged=True) is None


def test_plain_dry_run_is_coherent():
    assert maker.arming_refusal(dry_run=True, flagged=False) is None


def test_is_real_money_requires_both_conditions():
    assert maker.is_real_money(dry_run=False, flagged=True) is True
    assert maker.is_real_money(dry_run=True, flagged=True) is False
    assert maker.is_real_money(dry_run=True, flagged=False) is False


# ── the engine, driven against a fake venue ──────────────────────────────────────────────────

class FakeClient:
    """The venue surface the maker is allowed to touch, and nothing else.

    ⛔ EVERY RESPONSE SHAPE HERE IS THE RECORDED REAL ONE, and that is the whole point of the
    class. The first version of this fixture invented `{"order": {"orderId": ...}}` for a create
    and `{"orderId": ...}` for an open order. Neither exists on this venue: `orders.create` returns
    a FLAT `{"id": "BG4G3YHJM9ST", "executions": []}` [recorded live in
    logs/poly_rebate_probe.json, stage "create"; SDK CreateOrderResponse = {id, executions}] and
    `Order` carries `id`, not `orderId` [SDK types/orders.py:70-90; recorded live at stage
    "commission_t0"]. Because the fixture and the code were written from the same wrong belief,
    all 88 tests passed against a maker that would have read every order id as None — no fills, no
    cancels, no cap, and a teardown reporting success over four live orders.

    A fixture written from the same belief as the code confirms only the belief. Do not "simplify"
    these shapes; change them only against a recorded venue response.
    """

    def __init__(self, book: dict | None = None, tick: str = "0.01") -> None:
        self.book = book if book is not None else {
            "marketData": {"bids": [{"px": "0.44", "qty": "376"}],
                           "offers": [{"px": "0.47", "qty": "500"}]}}
        self.tick = Decimal(tick)
        self.fetches: list[tuple[str, bool]] = []
        self.placed: list[dict] = []
        self.cancelled: list[tuple[str, str]] = []
        self.open_orders: list[dict] = []
        self.open_orders_calls: list = []
        self.orders: dict[str, dict] = {}
        self._next_id = 0
        self.book_read_fails = False
        self.cancel_fails = False
        # ── belief-recovery surfaces ─────────────────────────────────────────────────────
        # Forced verdicts for the typed reads; absent order + no forced verdict answers
        # `not_found` because that IS the venue's answer for an id it has purged (the real
        # client's structured-404 discrimination happens below this fake's level).
        self.order_verdicts: dict[str, str] = {}
        self.cancel_verdicts: dict[str, str] = {}
        self.cancel_ex_calls: list[tuple[str, str]] = []
        # cursor -> (activities, nextCursor); "" is page 1. Recorded envelope shapes only.
        self.activities: dict[str, tuple[list[dict], str]] = {"": ([], "")}
        self.activities_calls: list[str] = []

    async def get_market_tick(self, slug: str):
        return self.tick

    async def _fetch_book(self, slug: str, *, fresh: bool):
        self.fetches.append((slug, fresh))
        if self.book_read_fails:
            raise RuntimeError("book read failed")
        # ⛔ PER-SLUG when `books` is set. This returned `self.book` for EVERY slug, so no test in
        # the suite could distinguish "this slug's touch" from "some slug's touch" — a mutation
        # swapping `last_touch.get(order.slug)` for `next(iter(last_touch.values()))` passed the
        # ENTIRE suite. A real run quotes several concurrent books at very different prices
        # (say p≈0.54 and p≈0.84), so that mutation is a large error on every mid and spread
        # of the mis-stamped book.
        return self.books.get(slug, self.book) if getattr(self, "books", None) else self.book

    async def place_limit_gtc(self, token, price, size, label="", *, post_only=False, side="buy"):
        self._next_id += 1
        oid = f"BG4G3YHJM9S{self._next_id}"
        self.placed.append({"token": token, "price": price, "size": size, "side": side,
                            "post_only": post_only, "order_id": oid})
        # The RECORDED create response: flat, `id`, plus an executions list. No wrapper.
        # ⛔ The resting state is ORDER_STATE_NEW — 579 of 1,166 recorded WS bodies; the venue
        # has NO state containing "OPEN" (0/1,166). This fixture said ORDER_STATE_OPEN for a
        # month, which is why the maker's `"OPEN" not in state` pop-condition — vacuously true
        # against every REAL state including live ones — survived the whole suite: fixture and
        # code written from the same wrong belief [the header's own trap, second occurrence].
        self.orders[oid] = {"id": oid, "marketSlug": token, "cumQuantity": 0,
                            "leavesQuantity": int(size), "state": "ORDER_STATE_NEW"}
        self.open_orders.append({"id": oid, "marketSlug": token,
                                 "state": "ORDER_STATE_NEW"})
        return {"id": oid, "executions": []}

    async def cancel_order(self, order_id: str, market_slug: str) -> bool:
        self.cancelled.append((order_id, market_slug))
        if self.cancel_fails:
            return False           # the client returns False; it does NOT raise
        self.open_orders = [o for o in self.open_orders if o.get("id") != order_id]
        return True

    async def get_open_orders(self, slugs=None) -> list[dict]:
        # Records EVERY call's scoping; serves everything regardless, mirroring a venue whose
        # slugs filter is best-effort — the client-side filters must still hold. Per-call
        # history matters: the sweep polls AFTER its own listing, and a last-value-only record
        # let the sweep-scoping mutant hide behind the poll's scoped call.
        self.open_orders_slugs = list(slugs) if slugs else None
        self.open_orders_calls.append(list(slugs) if slugs else None)
        return list(self.open_orders)

    async def get_order(self, order_id: str):
        return self.orders.get(order_id)

    async def get_order_ex(self, order_id: str):
        # Routes through `get_order` DELIBERATELY: the real client's two methods hit the same
        # endpoint, and the lag/stale subclasses override get_order alone — a get_order_ex
        # that read self.orders directly would silently bypass every venue-lie fixture.
        forced = self.order_verdicts.get(order_id)
        if forced in ("not_found", "error"):
            return None, forced
        body = await self.get_order(order_id)
        if body is None:
            return None, "not_found"
        return body, "ok"

    async def cancel_order_ex(self, order_id: str, market_slug: str):
        self.cancel_ex_calls.append((order_id, market_slug))
        forced = self.cancel_verdicts.get(order_id)
        if forced in ("not_found", "error"):
            return False, forced
        ok = await self.cancel_order(order_id, market_slug)
        return ok, ("ok" if ok else "error")

    async def get_activities_page(self, cursor: str = ""):
        self.activities_calls.append(cursor)
        return self.activities.get(cursor, ([], ""))

    async def close(self) -> None:
        pass


def _order_read(oid, qty, commission="-0.0138", *, state="ORDER_STATE_FILLED", slug="mkt-a"):
    """A `get_order` read-back in the RECORDED venue shape — flat, keyed `id`, with the commission
    as an {value,currency} Amount. [logs/poly_rebate_probe.json stage "commission_t0"]"""
    return {"id": oid, "marketSlug": slug, "cumQuantity": qty, "state": state,
            "commissionNotionalTotalCollected": {"value": commission, "currency": "USD"}}


def _maker(client, **kw):
    kw.setdefault("slugs", ["mkt-a"])
    kw.setdefault("size", 5)
    kw.setdefault("shadow", True)
    # Effectively unpaced, so the suite is not spending real seconds in the rate limiter. The
    # pacing itself is tested explicitly below with a realistic ceiling.
    kw.setdefault("max_req_per_s", 10_000.0)
    return maker.PolyMaker(client=client, **kw)


def test_the_cycle_paces_on_REQUESTS_SPENT_not_on_markets_visited():
    """⛔ Found by running a DRY cycle: pacing a fixed gap BETWEEN markets ignores that a quoting
    market costs 5 requests (1 book read + 2 placements x 2), not 1. A 2-market DRY cycle spent 10
    requests in 0.28s — a 35 req/s BURST against a documented 20 req/s ceiling. Over-limit on Poly
    is THROTTLED, not rejected, so the symptom would be a late, stale book rather than an error.

    The pace must therefore be driven by requests actually spent."""
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], shadow=False, max_req_per_s=20.0)
    asyncio.run(m.prepare())
    started = time.monotonic()
    stats = asyncio.run(m.run_cycle())
    elapsed = time.monotonic() - started
    assert stats.requests == 10                      # 2 x (1 book read + 2 placements x 2)
    # 10 requests at a 20/s ceiling cannot legitimately complete in under ~0.45s.
    assert elapsed >= 0.45, f"10 requests took {elapsed:.3f}s — that is a burst, not paced"
    assert stats.requests / elapsed <= 20.0 * 1.15


def test_an_unpaced_shadow_cycle_still_respects_the_ceiling():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b", "mkt-c"], shadow=True, max_req_per_s=20.0)
    asyncio.run(m.prepare())
    started = time.monotonic()
    stats = asyncio.run(m.run_cycle())
    elapsed = time.monotonic() - started
    assert stats.requests == 3
    assert stats.requests / elapsed <= 20.0 * 1.15


def test_shadow_cycle_reads_the_book_cache_busted_and_places_nothing():
    client = FakeClient()
    m = _maker(client)
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    assert client.fetches == [("mkt-a", True)]        # fresh=True — never the 30s-cached /bbo
    assert client.placed == []
    assert stats.markets_quoted == 1


def test_shadow_mode_refuses_to_place_even_if_the_placement_path_is_reached():
    """Structural, not incidental: a shadow run must not be one bad branch away from an order."""
    client = FakeClient()
    m = _maker(client, shadow=True)
    with pytest.raises(maker.ShadowViolation):
        asyncio.run(m._place("mkt-a", "bid", Decimal("0.44"), improved=False,
                             queue_ahead=Decimal("0")))


def test_a_live_cycle_places_both_sides_post_only():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert len(client.placed) == 2
    assert all(o["post_only"] is True for o in client.placed)
    assert {o["side"] for o in client.placed} == {"buy", "sell"}


def test_a_second_cycle_on_an_unchanged_book_cancels_nothing_and_replaces_nothing():
    """The queue-position rule, end to end: the same touch two cycles running must leave both
    orders exactly where they are."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    placed_after_first = len(client.placed)
    asyncio.run(m.run_cycle())
    assert client.cancelled == []
    assert len(client.placed) == placed_after_first


def test_a_moved_touch_cancels_before_replacing():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.book = {"marketData": {"bids": [{"px": "0.40", "qty": "10"}],
                                  "offers": [{"px": "0.43", "qty": "10"}]}}
    asyncio.run(m.run_cycle())
    assert len(client.cancelled) == 2
    assert len(client.placed) == 4


def test_at_the_cap_the_adding_side_is_cancelled_and_the_reducing_side_stays():
    client = FakeClient()
    m = _maker(client, shadow=False, cap_fills=1)     # cap = 5 contracts
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m.inventory["mkt-a"] = Decimal("5")               # a full fill on the bid
    client.placed.clear()
    asyncio.run(m.run_cycle())
    # the bid (adds) is pulled; the ask (reduces) is untouched, so it keeps its queue position
    assert len(client.cancelled) == 1
    assert client.placed == []


def test_the_global_exposure_cap_stops_adding_across_markets():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], shadow=False, cap_fills=3,
               max_total_contracts=10)
    asyncio.run(m.prepare())
    m.inventory["mkt-b"] = Decimal("10")
    asyncio.run(m.run_cycle())
    # mkt-a is under its OWN cap but the book-wide exposure is used up, so no bid goes out on it
    a_buys = [o for o in client.placed if o["token"] == "mkt-a" and o["side"] == "buy"]
    assert a_buys == []


def test_a_paused_kill_switch_skips_the_cycle_before_any_venue_call(monkeypatch):
    """`is_paused()` is the FIRST statement of the cycle — a pause that is checked after the book
    read has already spent the request and already decided a quote."""
    client = FakeClient()
    m = _maker(client)
    asyncio.run(m.prepare())
    client.fetches.clear()
    monkeypatch.setattr(maker, "is_paused", lambda: True)
    stats = asyncio.run(m.run_cycle())
    assert stats.paused is True
    assert client.fetches == []


def test_a_failed_book_read_is_recorded_and_does_not_kill_the_cycle():
    client = FakeClient()
    client.book_read_fails = True
    m = _maker(client, slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    assert stats.markets_skipped == 2
    assert stats.markets_quoted == 0


def test_a_transient_unreadable_book_keeps_the_quote_resting_and_its_queue_position():
    """A single failed read must NOT pull the quote. Book reads fail routinely; cancelling on one
    would forfeit queue position on every hiccup, which is the one thing this lane cannot afford."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.book_read_fails = True
    asyncio.run(m.run_cycle())
    assert client.cancelled == []
    assert len(m.resting) == 2


def test_a_persistently_unreadable_market_has_its_quotes_pulled():
    """⛔ The other half. An order resting on a market we have stopped being able to read is an
    unmanaged real-money exposure: it can still fill, at a price nothing is checking. Tolerate the
    blip, refuse the outage."""
    client = FakeClient()
    m = _maker(client, shadow=False, stale_cancel_cycles=3)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.book_read_fails = True
    for _ in range(3):
        asyncio.run(m.run_cycle())
    assert len(client.cancelled) == 2
    assert m.resting == {}


def test_a_recovered_read_resets_the_failure_streak():
    """Three failures spread over an hour are not an outage. Only CONSECUTIVE ones are."""
    client = FakeClient()
    m = _maker(client, shadow=False, stale_cancel_cycles=3)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    for _ in range(2):
        client.book_read_fails = True
        asyncio.run(m.run_cycle())
        client.book_read_fails = False
        asyncio.run(m.run_cycle())
    assert client.cancelled == []


def test_a_market_with_an_unreadable_tick_is_dropped_at_prepare_not_defaulted():
    """A defaulted tick is right often enough to look correct while mispricing exactly the markets
    that differ — and the tick varies WITHIN a series on this venue."""
    class NoTick(FakeClient):
        async def get_market_tick(self, slug):
            return None if slug == "mkt-b" else Decimal("0.01")

    client = NoTick()
    m = _maker(client, slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    assert list(m.ticks) == ["mkt-a"]


def test_a_nonpositive_tick_is_also_dropped():
    class ZeroTick(FakeClient):
        async def get_market_tick(self, slug):
            return Decimal("0")

    m = _maker(ZeroTick(), slugs=["mkt-a"])
    asyncio.run(m.prepare())
    assert list(m.ticks) == []


# ── throughput instrumentation ────────────────────────────────────────────────────────────────

def test_cycle_stats_count_requests_and_wall_time():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    assert stats.requests == 2                    # one cache-busted book read per market
    assert stats.wall_s >= 0.0
    assert stats.markets_total == 2


def test_request_count_includes_the_hidden_book_read_inside_every_placement():
    """⛔ `place_limit_gtc` fetches the book itself for its crossing guard, so a live placement
    costs TWO requests, not one. Counting only our own book reads understates the live rate by
    several-fold at full quoting — and the whole point of this instrumentation is to decide how
    many markets fit inside the venue's rate budget. An instrument that flatters the thing it is
    measuring is worse than none."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    # 1 book read + 2 placements x (1 guard book read + 1 create)
    assert stats.requests == 5


def test_fills_are_polled_before_the_cap_decision_so_inventory_is_current():
    """⛔ The cap and the reduce-only rule are functions of inventory. Polling fills AFTER the
    quote loop sizes every cycle off the previous cycle's position, so the side that should have
    stopped adding keeps adding for one more requote — at exactly the moment inventory is running."""
    client = FakeClient()
    m = _maker(client, shadow=False, cap_fills=1)          # cap = 5 contracts
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = [o for o in client.placed if o["side"] == "buy"][0]["order_id"]
    client.orders[buy_oid] = _order_read(buy_oid, 5)
    client.open_orders = []
    client.placed.clear()
    asyncio.run(m.run_cycle())
    # The bid filled to the cap during cycle 1; cycle 2 must NOT put a new bid out.
    assert [o for o in client.placed if o["side"] == "buy"] == []
    assert m.inventory["mkt-a"] == Decimal("5")


def test_a_shadow_cycle_costs_exactly_one_request_per_market():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], shadow=True)
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    assert stats.requests == 2


def test_a_cancel_replace_is_counted_as_its_own_request():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.book = {"marketData": {"bids": [{"px": "0.40", "qty": "10"}],
                                  "offers": [{"px": "0.43", "qty": "10"}]}}
    stats = asyncio.run(m.run_cycle())
    # fill poll (1 open-orders + 2 get_order) + 1 book read
    # + 2 cancels x (1 final-fill reconcile + 1 cancel) + 2 placements x 2
    assert stats.requests == 12


def test_missed_cycles_counts_whole_intervals_the_cycle_overran_by():
    """The question is 'can one process hold N books inside the rate budget WITHOUT missing
    cycles', so an overrun has to be counted, not absorbed by a shrinking sleep."""
    assert maker.missed_cycles(wall_s=3.0, requote_s=10.0) == 0
    assert maker.missed_cycles(wall_s=10.0, requote_s=10.0) == 0
    assert maker.missed_cycles(wall_s=25.0, requote_s=10.0) == 2


def test_staleness_is_tracked_per_market():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert set(m.last_read_ts) == {"mkt-a", "mkt-b"}
    stats = asyncio.run(m.run_cycle())
    assert stats.max_staleness_s is not None
    assert stats.max_staleness_s < 5.0


def test_staleness_is_an_age_and_can_never_be_negative():
    """⛔ Found by RUNNING it: the first version measured each market's last read against the
    cycle's OWN start time, so a market read during the cycle came out at `-0.0s` and the live
    log reported `stale_max=-0.0s` for every cycle. A staleness metric that goes negative is
    not a slightly-wrong number, it is a metric that cannot detect the thing it exists for — a
    market whose reads have stopped."""
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    for _ in range(3):
        stats = asyncio.run(m.run_cycle())
        assert stats.max_staleness_s is not None
        assert stats.max_staleness_s >= 0.0


def test_the_quote_tape_records_each_markets_real_sampling_interval(tmp_path):
    """The per-market half of the same question. A cycle-level average can look healthy while
    one market is quietly being sampled at half the rate."""
    import csv as _csv

    quote_csv = tmp_path / "q.csv"
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a"], quote_csv=str(quote_csv))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    asyncio.run(m.run_cycle())
    m._close_writers()
    rows = list(_csv.DictReader(quote_csv.open()))
    assert rows[0]["sample_gap_s"] == ""          # first cycle: no previous read to measure from
    assert float(rows[1]["sample_gap_s"]) >= 0.0


def test_a_market_that_stops_reading_shows_growing_staleness():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a"])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m.last_read_ts["mkt-a"] -= 120.0
    client.book_read_fails = True
    stats = asyncio.run(m.run_cycle())
    assert stats.max_staleness_s > 100.0


def test_projected_request_rate_flags_a_slate_that_will_not_fit_the_budget():
    """Poly is 20 req/s per key. 33 markets at 10s is 3.3 req/s of book reads — fine. The same
    slate quoting live also spends a hidden book read INSIDE place_limit_gtc's crossing guard on
    every placement, which is what actually approaches the ceiling."""
    assert maker.projected_req_per_s(requests=33, requote_s=10.0) == pytest.approx(3.3)
    assert maker.rate_budget_warning(33, 10.0) is None
    assert maker.rate_budget_warning(330, 10.0) is not None


# ── the rails, consumed rather than reinvented ───────────────────────────────────────────────

class RecordingHeartbeat:
    def __init__(self) -> None:
        self.beats: list[dict] = []
        self.exits: list[str] = []

    def beat(self, **fields) -> None:
        self.beats.append(dict(fields))

    def mark_exit(self, status: str = "clean", **fields) -> None:
        self.exits.append(status)


def test_every_cycle_writes_a_heartbeat_carrying_the_operators_three_questions():
    """Markets quoted, inventory on, P&L — the deadman reads these, and a dying process cannot
    report its own death."""
    hb = RecordingHeartbeat()
    m = _maker(FakeClient(), heartbeat=hb)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert len(hb.beats) == 1
    assert "markets_quoted" in hb.beats[0]
    assert "inventory" in hb.beats[0]


def test_a_paused_cycle_still_beats_so_the_deadman_does_not_trip_on_a_deliberate_pause(monkeypatch):
    hb = RecordingHeartbeat()
    m = _maker(FakeClient(), heartbeat=hb)
    asyncio.run(m.prepare())
    monkeypatch.setattr(maker, "is_paused", lambda: True)
    asyncio.run(m.run_cycle())
    assert len(hb.beats) == 1


def test_a_memory_halt_stops_the_run_through_the_makers_own_teardown(monkeypatch):
    """A clean halt with orders cancelled beats a SIGKILL with orders resting — the OOM killer
    picks the largest-RSS process, and on a small host that is the maker itself."""
    from bot.core import memguard

    m = _maker(FakeClient(), shadow=False)
    asyncio.run(m.prepare())
    monkeypatch.setattr(maker.memguard, "check",
                        lambda *a, **k: memguard.MemStatus("halt", "rss 900MB >= halt 700MB"))
    stats = asyncio.run(m.run_cycle())
    assert stats.halt_reason is not None
    assert m.should_stop is True


def test_a_memory_warning_does_not_stop_the_run(monkeypatch):
    from bot.core import memguard

    m = _maker(FakeClient(), shadow=False)
    asyncio.run(m.prepare())
    monkeypatch.setattr(maker.memguard, "check",
                        lambda *a, **k: memguard.MemStatus("warn", "rss 450MB >= warn 400MB"))
    asyncio.run(m.run_cycle())
    assert m.should_stop is False


def test_a_real_run_records_the_order_intent_before_the_order_is_sent(tmp_path):
    """The uncoverable window is 'sent, then died before the response arrived'. Recording after
    the venue answers leaves that window with no trace at all."""
    from bot.core.maker_state import MakerStateStore

    order_of_events: list[str] = []

    class SpyStore(MakerStateStore):
        def record_intent(self, intent_id, **kw):
            order_of_events.append("intent")
            super().record_intent(intent_id, **kw)

        def record_placed(self, intent_id, order_id):
            order_of_events.append("placed")
            super().record_placed(intent_id, order_id)

    class SpyClient(FakeClient):
        async def place_limit_gtc(self, *a, **kw):
            order_of_events.append("sent")
            return await super().place_limit_gtc(*a, **kw)

    store = SpyStore(str(tmp_path / "maker_state.json"))
    m = _maker(SpyClient(), shadow=False, real=True, state_store=store)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert order_of_events[:3] == ["intent", "sent", "placed"]


def test_a_shadow_run_writes_no_durable_state(tmp_path):
    """Deliberate: a preview must not erase a crashed real run's record."""
    from bot.core.maker_state import MakerStateStore

    store = MakerStateStore(str(tmp_path / "maker_state.json"))
    m = _maker(FakeClient(), shadow=True, real=False, state_store=store)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert store.snapshot().maybe_live_orders == 0


# ── fill recording ───────────────────────────────────────────────────────────────────────────

def test_a_fill_records_commission_queue_ahead_time_to_fill_and_improved():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    oid = client.placed[0]["order_id"]
    client.orders[oid] = _order_read(oid, 5)
    fills = asyncio.run(m.poll_fills())
    assert len(fills) == 1
    f = fills[0]
    assert f.commission_order_total == Decimal("-0.0138")
    assert f.queue_ahead is not None
    assert f.time_to_fill_s >= 0.0
    assert f.improved in (True, False)
    assert f.filled_qty == Decimal("5")
    assert f.cum_filled_qty == Decimal("5")


def test_the_commission_is_recorded_as_an_ORDER_total_beside_the_cumulative_quantity():
    """⛔ `commissionNotionalTotalCollected` is a property of the ORDER, not of one increment. A
    25-lot filling 10 then 15 writes -0.03 then -0.08; summing the column gives -0.11 against a
    true -0.08. The rebate gate ("commission confirmed at the venue's formula") is read off this
    column, so it has to carry the quantity it actually belongs to."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = [o for o in client.placed if o["side"] == "buy"][0]["order_id"]

    client.orders[buy_oid] = _order_read(buy_oid, 2, "-0.01",
                                         state="ORDER_STATE_PARTIALLY_FILLED")
    first = asyncio.run(m.poll_fills())[0]
    client.orders[buy_oid] = _order_read(buy_oid, 5, "-0.02")
    second = asyncio.run(m.poll_fills())[0]

    assert (first.filled_qty, first.cum_filled_qty) == (Decimal("2"), Decimal("2"))
    assert (second.filled_qty, second.cum_filled_qty) == (Decimal("3"), Decimal("5"))
    # The order's true total is the LAST row's value, never the sum of the column.
    assert second.commission_order_total == Decimal("-0.02")


def test_a_positive_commission_is_recorded_not_silently_treated_as_a_rebate():
    """The verdict must be able to come back CHARGED — the docs are the provenance that produced
    the Kalshi cent-vs-centicent bug."""
    assert maker.commission_verdict(Decimal("-0.0138")) == "REBATE"
    assert maker.commission_verdict(Decimal("0.0138")) == "CHARGED"
    assert maker.commission_verdict(Decimal("0")) == "ZERO"
    assert maker.commission_verdict(None) == "UNREADABLE"


def test_a_fill_updates_inventory_in_the_right_direction():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = [o for o in client.placed if o["side"] == "buy"][0]["order_id"]
    client.orders[buy_oid] = _order_read(buy_oid, 5)
    asyncio.run(m.poll_fills())
    assert m.inventory["mkt-a"] == Decimal("5")


def test_an_ask_fill_moves_inventory_short():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    sell_oid = [o for o in client.placed if o["side"] == "sell"][0]["order_id"]
    client.orders[sell_oid] = _order_read(sell_oid, 5)
    asyncio.run(m.poll_fills())
    assert m.inventory["mkt-a"] == Decimal("-5")


def test_a_partial_fill_is_counted_at_the_filled_quantity_only():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = [o for o in client.placed if o["side"] == "buy"][0]["order_id"]
    client.orders[buy_oid] = _order_read(buy_oid, 2, "-0.0055",
                                         state="ORDER_STATE_PARTIALLY_FILLED")
    asyncio.run(m.poll_fills())
    assert m.inventory["mkt-a"] == Decimal("2")


def test_the_same_fill_is_not_counted_twice_across_polls():
    """`cumQuantity` is CUMULATIVE. Adding it again on the next poll would double the position and
    silently mis-state every P&L built on it."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = [o for o in client.placed if o["side"] == "buy"][0]["order_id"]
    client.orders[buy_oid] = _order_read(buy_oid, 2, "-0.0055",
                                         state="ORDER_STATE_PARTIALLY_FILLED")
    asyncio.run(m.poll_fills())
    asyncio.run(m.poll_fills())
    assert m.inventory["mkt-a"] == Decimal("2")


# ── teardown, executed ───────────────────────────────────────────────────────────────────────

def test_teardown_runs_its_phases_in_order_and_sweeps_last(monkeypatch):
    client = FakeClient()
    m = _maker(client, shadow=False)
    # Universal parking means a teardown with recently-cancelled orders spends its FULL 4x20s
    # verify window (entries inside the 120s horizon can never certify early — that is the retire
    # rule working). Real shutdowns pay that 80s on purpose; this test collapses only the gaps.
    real_sleep = asyncio.sleep
    async def _fast(_s):
        await real_sleep(0)
    monkeypatch.setattr(maker.asyncio, "sleep", _fast)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.open_orders = [{"id": "STRAY", "marketSlug": "mkt-a"}]
    phases = asyncio.run(m.teardown())
    assert [p for p, _ in phases] == list(maker.TEARDOWN_PHASES)


def test_teardown_sweep_cancels_a_stray_the_maker_never_recorded():
    """The hole that stranded real orders on a live run: a create whose response was lost leaves
    an order resting on the venue that we have no record of."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    client.open_orders = [{"id": "STRAY", "marketSlug": "mkt-a"}]
    asyncio.run(m.teardown())
    assert ("STRAY", "mkt-a") in client.cancelled


def test_the_flatten_order_is_sized_to_the_RESIDUAL_not_to_the_quote_size():
    """⛔ Sizing the reducing order at `--size` overshoots whenever the residual is smaller: a
    3-contract long flattened with a 5-lot ask ends up 2 SHORT. A teardown that opens a new
    position in the opposite direction is worse than the inventory it was clearing."""
    # Venue corroboration is a PRECONDITION for the flatten to trade at all, read FRESH at
    # teardown — a flatten sized off an uncorroborated belief is how it grows the position.
    client = _VenueClient({"mkt-a": "3"})
    m = _maker(client, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal("3")
    asyncio.run(m.teardown())
    flatten_orders = [o for o in client.placed if o["side"] == "sell"]
    assert flatten_orders, "the flatten should have placed a reducing order"
    assert flatten_orders[-1]["size"] == 3


def test_a_residual_smaller_than_one_contract_places_no_flatten_order():
    client = FakeClient()
    m = _maker(client, shadow=False, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal("0.4")
    asyncio.run(m.teardown())
    assert client.placed == []


def test_teardown_reports_residual_inventory_rather_than_market_selling_it():
    """Cancelling is safe, flattening is not. A residual is REPORTED for an operator; the maker
    never crosses the spread to be rid of it."""
    client = FakeClient()
    m = _maker(client, shadow=False, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal("5")
    phases = asyncio.run(m.teardown())
    flatten = dict(phases)["flatten"]
    assert "mkt-a" in str(flatten)


def test_teardown_in_shadow_mode_touches_no_venue_write_path():
    client = FakeClient()
    m = _maker(client, shadow=True)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    asyncio.run(m.teardown())
    assert client.cancelled == []
    assert client.placed == []


# ── the venue's order-id field, and what depends on it ───────────────────────────────────────

def test_the_order_id_is_read_from_the_field_the_venue_actually_sends():
    """⛔ The field is `id`. Reading `orderId` yields None for every order, which silently
    disables fills, both caps, every cancel and the teardown sweep at once."""
    assert maker.venue_order_id({"id": "BG4G3YHJM9ST", "executions": []}) == "BG4G3YHJM9ST"
    assert maker.venue_order_id({"order": {"id": "X1"}}) == "X1"
    assert maker.venue_order_id({"orderId": "X1"}) is None      # NOT a shape this venue sends
    assert maker.venue_order_id({"status": "dry_run"}) is None
    assert maker.venue_order_id(None) is None


def test_a_placement_records_the_venues_order_id():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert all(o.order_id is not None for o in m.resting.values())
    assert m.resting[("mkt-a", "bid")].order_id == client.placed[0]["order_id"]


def test_a_create_response_with_no_id_is_loud_and_still_recorded(caplog):
    """It may be resting. It must not look like an ordinary placement, and it must stay findable
    by the teardown sweep."""
    class NoId(FakeClient):
        async def place_limit_gtc(self, token, price, size, label="", *, post_only=False,
                                  side="buy"):
            await super().place_limit_gtc(token, price, size, label,
                                          post_only=post_only, side=side)
            return {"executions": []}

    client = NoId()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    with caplog.at_level("ERROR"):
        asyncio.run(m.run_cycle())
    assert any("NO `id`" in r.message for r in caplog.records)
    assert len(m.resting) == 2


# ── cancels: confirmed, reconciled, and scoped ───────────────────────────────────────────────

def test_a_refused_cancel_leaves_the_order_in_our_book_rather_than_forgetting_it():
    """⛔ `client.cancel_order` returns False and does NOT raise. Popping regardless turns a failed
    cancel into an invisible live order, and clearing the durable record deletes the only thing
    the recovery tool reads."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.cancel_fails = True
    ok = asyncio.run(m._cancel("mkt-a", "bid"))
    assert ok is False
    assert ("mkt-a", "bid") in m.resting


def test_a_cancel_reconciles_the_orders_final_fill_before_dropping_it():
    """An order can fill in the requote window between two polls. Dropping it at cancel time
    without a last read loses those contracts — the position is real and our inventory never
    sees it, so every later cap decision is computed against a wrong number."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    buy_oid = m.resting[("mkt-a", "bid")].order_id
    client.orders[buy_oid] = _order_read(buy_oid, 5)     # filled, unseen by any poll
    asyncio.run(m._cancel("mkt-a", "bid"))
    assert m.inventory["mkt-a"] == Decimal("5")


def test_the_teardown_sweep_leaves_orders_on_other_markets_alone():
    """⛔ `get_open_orders` is ACCOUNT-WIDE. An unscoped sweep cancels every resting order on the
    key — including another process's. A teardown may only undo what this run could have done."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    client.open_orders = [{"id": "OURS", "marketSlug": "mkt-a"},
                          {"id": "SOMEONE-ELSES", "marketSlug": "example-election-beta-2026-11-03-no"}]
    outcome = dict(asyncio.run(m.teardown()))["sweep"]
    cancelled_ids = [oid for oid, _slug in client.cancelled]
    assert "OURS" in cancelled_ids
    assert "SOMEONE-ELSES" not in cancelled_ids
    assert "LEFT ALONE" in outcome


def test_the_sweep_reports_rather_than_guesses_on_an_order_with_no_readable_slug():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    client.open_orders = [{"id": "MYSTERY"}]
    outcome = dict(asyncio.run(m.teardown()))["sweep"]
    assert client.cancelled == []
    assert "no readable id/slug" in outcome


def test_the_kill_switch_pulls_resting_quotes_rather_than_just_stopping_new_ones(monkeypatch):
    """⛔ Returning early without cancelling leaves orders live and still filling, while skipping
    the cap and reduce-only checks entirely — a maker that keeps trading with NO cap in force."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert len(m.resting) == 2
    monkeypatch.setattr(maker, "is_paused", lambda: True)
    stats = asyncio.run(m.run_cycle())
    assert stats.paused is True
    assert m.resting == {}
    assert len(client.cancelled) == 2


def test_the_per_price_rebate_rule_is_actually_wired_to_something():
    """⛔ `rebate_rounds_to_zero` existing and being tested is not the same as it being CONSULTED.
    The size floor gates on size only; without this, pointing --slugs at a longshot quotes a market
    whose rebate rounds to nothing while carrying full adverse-selection and inventory risk."""
    m = _maker(FakeClient(), size=5)
    zero = m.zero_rebate_markets({
        "longshot": (Decimal("0.02"), Decimal("0.03")),
        "healthy": (Decimal("0.49"), Decimal("0.51")),
    })
    assert len(zero) == 1
    assert zero[0].startswith("longshot")
    assert "needs size" in zero[0]


def test_a_fill_carries_a_mark_so_the_tape_has_a_cost_side():
    """The rebate is the CREDIT side. A tape with no mark is a fill counter, not a market-making
    record — and the gate for scaling up is 'net positive over a full day', which needs both."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    oid = m.resting[("mkt-a", "bid")].order_id
    client.orders[oid] = _order_read(oid, 5)
    fill = asyncio.run(m.poll_fills())[0]
    assert fill.mid_at_fill == (Decimal("0.44") + Decimal("0.47")) / 2
    assert fill.mid_age_s is not None and fill.mid_age_s >= 0.0


# ── the CLI shim ─────────────────────────────────────────────────────────────────────────────

def test_the_shim_sets_dry_run_false_only_when_the_flag_is_present():
    """The argv sniff must run BEFORE `bot.core.config` is imported — config snapshots the
    environment at import time, so the decision cannot move into main()."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "poly_live_mm.py"
    text = src.read_text()
    sniff = text.index('os.environ["DRY_RUN"]')
    first_bot_import = text.index("from bot")
    assert sniff < first_bot_import
    assert '"--i-understand-real-money" in sys.argv' in text


def test_the_shim_never_reaches_the_venue_itself():
    """All venue contact belongs in the library, so the arming decision stays reviewable in one
    short file."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "poly_live_mm.py"
    text = src.read_text()
    for forbidden in ("place_limit", "orders.create", "_fetch_book", "cancel_order"):
        assert forbidden not in text


def test_the_maker_never_reads_the_cdn_cached_touch_endpoints():
    """Every Poly public GET is Cloudflare max-age=30. `bbo` is not cache-busted, so a quote built
    on it can be half a minute old — and the failure is fail-OPEN in the direction that costs
    money."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "bot" / "poly_us" / "maker.py"
    text = src.read_text()
    for forbidden in ("get_best_ask", "get_best_bid", ".bbo("):
        assert forbidden not in text
    assert "fresh=True" in text


# ── four blocking review findings, each pinned ────────────────────────────────────────────────

def test_a_fill_records_the_VENUES_price_not_ours(tmp_path):
    """The rebate gate is "commission at the venue's formula" — and the venue computes the rebate
    at ITS fill price, not our limit. On a BUY_SHORT the two can be most of a dollar apart (wire
    0.100 → avgPx 0.8510). A tape carrying only our limit certifies the gate against the wrong p."""
    client = FakeClient()
    m = _maker(client, shadow=False, fill_csv=str(tmp_path / "fills.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    oid = [o for o in client.placed if o["side"] == "sell"][0]["order_id"]
    body = _order_read(oid, 5)
    body["avgPx"] = {"value": "0.5595", "currency": "USD"}   # venue filled BETTER than our limit
    client.orders[oid] = body
    asyncio.run(m.poll_fills())
    import csv as _csv
    rows = list(_csv.DictReader(open(tmp_path / "fills.csv")))
    assert rows[0]["avg_px"] == "0.5595"
    # and the prediction is computed at the venue's price, not our intent
    assert rows[0]["predicted_rebate_on_cum"] == str(
        maker.predicted_rebate(Decimal("0.5595"), Decimal(5)))


def test_a_refused_cancel_refuses_the_replacement_too():
    """`_cancel` keeps a venue-refused order in `self.resting` (cannot-verify is not
    cancelled) — but `_apply_action` used to place over it anyway, overwriting the book entry and
    making the maybe-live old order INVISIBLE: two lots resting, cap checked against half the real
    position, flatten sized to half. A refused cancel must refuse the place."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    old = m.resting[("mkt-a", "bid")]
    placed_before = len(client.placed)
    client.cancel_fails = True
    asyncio.run(m._apply_action(maker.REPLACE, "mkt-a", "bid",
                                Decimal("0.5560"), None, False))
    assert len(client.placed) == placed_before, "placed over an unconfirmed cancel"
    assert m.resting[("mkt-a", "bid")].order_id == old.order_id, (
        "the still-live order was evicted from our book")


def test_a_blocked_replace_is_COUNTED_not_just_logged():
    """The refuse-to-place branch above is correct but was invisible
    except as a log.error line: a venue that keeps refusing cancels leaves a side quoting a
    STALE price cycle after cycle (or dark, on the no-id path), and the operator watching the
    60s status line had no number to see it in. The counter is per-run and monotone, like
    `order_feed_unattributed`."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert m.replace_blocked == 0
    client.cancel_fails = True
    asyncio.run(m._apply_action(maker.REPLACE, "mkt-a", "bid",
                                Decimal("0.5560"), None, False))
    assert m.replace_blocked == 1, "a refused-cancel REPLACE must count"
    # a plain CANCEL refusal is NOT a blocked replace. On a venue-refused cancel the order
    # stays in `resting`, so the next cycle's retry AND the sweep own it; on the no-id path
    # the order is popped and only the sweep owns it — either way it is not a reprice that
    # failed to happen, which is what this counter measures. (An earlier blanket "the retry
    # loop and the sweep both own it" claim was wrong for the no-id half.)
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    assert m.replace_blocked == 1


def test_the_kill_switch_ENDS_the_run_not_just_the_quotes(monkeypatch):
    """`touch pause.json` used to pull the quotes and then leave the process alive and idle
    for the rest of --seconds, HOLDING inventory — the operator expects the documented
    halt-into-teardown. The pause must set should_stop so the shim breaks into teardown."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    monkeypatch.setattr(maker, "is_paused", lambda: True)
    stats = asyncio.run(m.run_cycle())
    assert stats.paused is True
    assert m.should_stop is True, "pause pulled quotes but left the run alive and holding"
    assert "kill switch" in (m.halt_reason or "")


def test_flatness_is_certified_by_the_venue_not_by_our_bookkeeping():
    """The engine's final-inventory print is a local belief; a create with no id produces a
    fill it never sees, so it can print FLAT over a real position. The verdict reads the venue,
    and unreadable is NOT flat."""
    from scripts.poly_live_mm import _flatness_verdict
    # unreadable → refuse to certify (None ≠ {})
    assert "NOT certified" in _flatness_verdict(None, {"mkt-a"}, {})
    # venue says we still hold on OUR market while we believe flat → disagree, loudly, and the
    # venue's own row timestamp rides along so divergent-replica reads can be reconciled
    v = _flatness_verdict({"mkt-a": ("5", "2026-07-29T04:31:53Z")}, {"mkt-a"}, {})
    assert "VENUE DISAGREES" in v and "'5'" in v.replace('"', "'")
    assert "04:31:53" in v, "the DISAGREE line must carry the venue's updateTime"
    # a position on an UNRELATED market must not fail this run's certification
    assert "confirms FLAT" in _flatness_verdict({"other-mkt": ("1", "?")}, {"mkt-a"}, {})
    assert "confirms FLAT" in _flatness_verdict({}, {"mkt-a"}, {})


def test_venue_positions_paginates_past_page_one():
    """The continuation key is `nextCursor`; reading `cursor` made pagination silently inert —
    always one page — while the docstring claimed it paginates to the end. Fail direction was
    OPEN: an unseen position reads as absent, i.e. flat, which is the exact reconciler bug shape
    this codebase has shipped before."""
    from scripts.poly_live_mm import _venue_positions

    class _SDK:
        def __init__(self):
            self.calls = 0
        async def positions(self, params):
            self.calls += 1
            if self.calls == 1:
                assert "cursor" not in params
                return {"positions": {"mkt-a": {"netPosition": "5", "netPositionDecimal": "5"}},
                        "nextCursor": "PAGE2", "eof": False}
            assert params.get("cursor") == "PAGE2"
            return {"positions": {"mkt-b": {"netPosition": "-3", "netPositionDecimal": "-3"}}, "nextCursor": "", "eof": True}

    class _Client:
        def __init__(self):
            import types
            self._sdk = types.SimpleNamespace(portfolio=types.SimpleNamespace())
            sdk = _SDK()
            self._sdk.portfolio.positions = sdk.positions

    got = asyncio.run(_venue_positions(_Client()))
    assert got == {"mkt-a": ("5", "?"), "mkt-b": ("-3", "?")}, (
        f"page 2 was never read — a position there reads as FLAT. got {got}")


def test_preflight_recovery_refuses_over_an_inherited_position_end_to_end():
    """⛔ Widening a value to a (qty, updateTime) tuple silently disabled the startup recovery
    gate — `assess_recovery`'s `_dec` swallows the parse error on a stringified tuple and returns
    ZERO, so every inherited position read as flat and a real-money launch printed START (clean)
    over live inventory. This drives `_preflight_recovery` itself, so the dict `assess_recovery`
    receives is `_venue_positions`' LITERAL output — every prior test hand-built that dict, which
    is exactly how a fully green suite said nothing about it."""
    import types
    from scripts.poly_live_mm import _preflight_recovery

    async def _positions(params):
        return {"positions": {"mkt-a": {"netPosition": "-17", "netPositionDecimal": "-17",
                                        "updateTime": "2026-07-29T01:38:00Z"}},
                "nextCursor": "", "eof": True}

    class _Client:
        def __init__(self):
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=_positions))

        async def get_open_orders(self, slugs=None):
            return []

    ok = asyncio.run(_preflight_recovery(_Client(), None))
    assert ok is False, (
        "an inherited −17 with no durable record must REFUSE to start (unknown_position) — "
        "True here means the gate read the tuple as flat")


def test_preflight_recovery_refuses_when_positions_are_unreadable():
    """The unwrap fix must pass None through UNTOUCHED: cannot-verify collapsing to flat is the
    reconciler bug this repo already shipped once, and `{k: v[0] ...}` over a None would raise —
    or worse, a 'fix' of that crash could substitute {}. None → refuse, always."""
    import types
    from scripts.poly_live_mm import _preflight_recovery

    async def _positions_raise(params):
        raise RuntimeError("Poly 502")

    class _Client:
        def __init__(self):
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=_positions_raise))

        async def get_open_orders(self, slugs=None):
            return []

    ok = asyncio.run(_preflight_recovery(_Client(), None))
    assert ok is False, "an unreadable venue must refuse to start — None is not flat"


def test_flatness_verdict_refuses_on_junk_rather_than_raising():
    """A junk quantity raises InvalidOperation from Decimal — inside the shim's `finally`, which
    would skip both the verdict and client.close(). Unparseable is CANNOT-VERIFY, never a crash
    and never flat. This branch shipped untested once; it does not get to again."""
    from scripts.poly_live_mm import _flatness_verdict
    v = _flatness_verdict({"mkt-a": ("not-a-number", "?")}, {"mkt-a"}, {})
    assert "NOT certified" in v


# ── the fill-loss fix: a blind cancel parks, a late read books ───────────────────────────────

class _LagClient(FakeClient):
    """get_order returns an UNREADABLE body for the first `blind` reads, then each order's true
    state (per-order cums, default 0) — the venue's minutes-long create-lag, as a live run met it."""
    def __init__(self, *, blind=2, **kw):
        super().__init__(**kw)
        self._blind = blind
        self.cums: dict[str, int] = {}
        self.reads = 0

    async def get_order(self, oid):
        self.reads += 1
        if self._blind > 0:
            self._blind -= 1
            return None                                   # the lag: order not found
        return {"id": oid, "cumQuantity": self.cums.get(oid, 0),
                "state": "ORDER_STATE_FILLED" if self.cums.get(oid) else "ORDER_STATE_CANCELED",
                "avgPx": {"value": "0.5560", "currency": "USD"},
                "commissionNotionalTotalCollected": {"value": "-0.0400", "currency": "USD"}}


def test_a_fill_eaten_by_the_blind_window_is_parked_and_late_booked(tmp_path, monkeypatch):
    """⛔ THE RUNG-2 DEFECT. An order that fills inside the venue's create-lag blind window and is
    then cancel-replaced used to vanish with its fill unbooked — 4 of 73 fills eaten, inventory
    belief −5 against a venue +5, every cap pointing the wrong way. The blind cancel must PARK the
    order and a later readable read must book the true increment."""
    client = _LagClient(blind=2)
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)   # let entries RETIRE once read
    monkeypatch.setattr(maker, "VERIFY_RETRY_S", 0.0)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.cums[bid.order_id] = 12          # the bid filled inside the blind window
    # cancel while blind: reconcile read 1 returns None -> parked, popped, cancel ok
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    assert bid.order_id in m.pending_reconcile, "blind cancel must park, not vanish"
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(0), "nothing booked while blind"
    # verify 1 (ignore_due: tests don't wait the 120s horizon): still blind -> stays parked with
    # an attempt recorded. verify 2: readable -> books +12 and unparks.
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert bid.order_id in m.pending_reconcile
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert bid.order_id not in m.pending_reconcile
    assert m.inventory["mkt-a"] == Decimal(12), "the late read must book the eaten fill"


def test_a_blind_cancel_that_never_filled_unparks_booking_nothing(tmp_path, monkeypatch):
    """The symmetric case: the late read shows cum 0 — the order really was empty. Unpark without
    booking; a parked order must not manufacture inventory."""
    client = _LagClient(blind=1)
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)   # let the verify RETIRE once read
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    assert bid.order_id in m.pending_reconcile
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert bid.order_id not in m.pending_reconcile
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(0)


def test_teardown_reconciles_parked_orders_BEFORE_the_flatten(tmp_path, monkeypatch):
    """The flatten sizes off inventory; a parked fill booked after it would size the disposal
    against a stale belief — a live flatten once tried to dispose of a short the account did not
    hold. The reconcile phase must run between cancel-all and flatten."""
    client = _LagClient(blind=2)
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"),
               flatten_wait_s=0.0)     # the reconcile leaves inventory 12; don't REALLY wait
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)   # let entries RETIRE once read (else the
    monkeypatch.setattr(maker, "VERIFY_RETRY_S", 0.0)  # teardown correctly holds them 4x20s)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    client.cums[m.resting[("mkt-a", "bid")].order_id] = 12
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    results = asyncio.run(m.teardown())
    phases = [p for p, _ in results]
    assert phases.index("reconcile_pending") < phases.index("flatten")
    assert not m.pending_reconcile, "teardown must drain the parked set (reads become readable)"
    assert m.inventory["mkt-a"] == Decimal(12)


def test_the_CYCLE_repairs_the_belief_not_just_direct_calls(tmp_path, monkeypatch):
    """Pins the run_cycle wiring itself. The three tests above call _retry_pending_reconciles
    directly, so deleting the per-cycle call leaves them green while the live loop never repairs
    its belief between fills — the mutation survived exactly that way. The cap and reduce-only
    decisions read inventory INSIDE the cycle; the repair must happen there too."""
    client = _LagClient(blind=2)
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)   # tests don't wait out the real horizon
    monkeypatch.setattr(maker, "VERIFY_RETRY_S", 0.0)  # nor the between-attempts backoff
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.cums[bid.order_id] = 12
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    assert bid.order_id in m.pending_reconcile
    # two cycles, NO direct retry calls: cycle 1's read is still blind, cycle 2's read books.
    asyncio.run(m.run_cycle())
    asyncio.run(m.run_cycle())
    assert bid.order_id not in m.pending_reconcile, "the cycle itself must drain the parked set"
    assert m.inventory["mkt-a"] == Decimal(12)


class _StaleClient(FakeClient):
    """get_order ANSWERS on every read — but each order has its own PER-ORDER schedule of cums by
    read index (default: forever 0). The venue's other lie: a readable body whose cumQuantity
    hasn't caught up with the fill (2b lost a 12-lot to this with ZERO not-found warnings).

    ⛔ Per-order, deliberately: the first version shared one schedule across all orders, so the
    ASK's reads consumed the BID's schedule and booked −12 against the +12 — the identical trap
    already hit and fixed in _LagClient, reintroduced within the hour. A shared fixture schedule
    is a fixture bug factory."""
    def __init__(self, schedules=None, **kw):
        super().__init__(**kw)
        self.schedules: dict = schedules or {}
        self.counts: dict = {}

    async def get_order(self, oid):
        seq = self.schedules.get(oid, [0])
        idx = self.counts.get(oid, 0)
        self.counts[oid] = idx + 1
        cum = seq[min(idx, len(seq) - 1)]
        # State is settable PER ORDER via self.states — deriving it from cum meant a stale
        # cum-0 read on a FILLED order presented as CANCELED, and the poll-park pin passed
        # while parking only cancel-shaped bodies.
        state = self.states.get(oid) if hasattr(self, "states") and oid in getattr(
            self, "states", {}) else ("ORDER_STATE_CANCELED" if cum == 0
                                      else "ORDER_STATE_FILLED")
        return {"id": oid, "cumQuantity": cum, "state": state,
                "avgPx": {"value": "0.5560", "currency": "USD"},
                "commissionNotionalTotalCollected": {"value": "-0.0400", "currency": "USD"}}


def test_a_READABLE_but_STALE_read_is_still_verified_and_the_fill_recovered(tmp_path, monkeypatch):
    """⛔ THE ACTUAL LOSS PATH, on a real run. The pre-cancel read ANSWERS — cum 0,
    readable, passes any presence test — but the fill lands moments later inside the venue's
    propagation gap. The old design trusted a readable read and dropped the order: the fill was
    lost with no warning anywhere. The redesign parks EVERY confirmed cancel and verifies after
    the lag horizon, so the stale read is caught by the schedule catching up."""
    client = _StaleClient()
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)   # let the verify RETIRE once read
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.schedules[bid.order_id] = [0, 12]  # pre-cancel read: cum 0 (stale). verify: cum 12.
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    assert bid.order_id in m.pending_reconcile, (
        "a READABLE cancel must still park — readable-but-stale is the loss path a presence test "
        "cannot see")
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(0)
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert m.inventory["mkt-a"] == Decimal(12), "the delayed verify must recover the stale-read fill"
    # and the recovered row is honest about its lateness
    import csv as _csv
    rows = list(_csv.DictReader(open(tmp_path / "f.csv")))
    late = [r for r in rows if r["late_booked"] == "Y"]
    assert len(late) == 1 and late[0]["mid_at_fill"] == "", (
        "a late-booked fill must be marked and must NOT carry a fresh-looking mid")
    assert late[0]["booked_via"] == "verify", (
        "the delayed verify must name itself as the booking path")


def test_the_POLL_path_parks_a_popped_terminal_order_like_the_cancel_path(tmp_path, monkeypatch):
    """⛔ THE LOST-FILL INCIDENT: a filled order vanished from orders.list, its get_order
    read-back was served STALE (cum 0), and poll_fills POPPED it without parking — the fill was
    lost forever, driving local belief to a SHORT while the venue held the opposite LONG, i.e.
    wrong by twice the position. The cancel path parks; the poll path is where fills actually
    go, so it must park too."""
    client = _StaleClient()
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    # The order fills at the venue but the poll's read-back is STALE: absent from the open set,
    # state FILLED, cum still 0. The pop must PARK, and the delayed verify recovers the fill.
    client.schedules[bid.order_id] = [0, 12]
    client.open_orders = [o for o in client.open_orders
                          if o.get("id") != bid.order_id]   # vanished from orders.list
    # ⛔ The body must present as FILLED with a stale cum-0 — the incident's exact shape. The
    # first version let the fake derive CANCELED from cum==0, so a "park only cancels" mutant
    # survived.
    client.states = {bid.order_id: "ORDER_STATE_FILLED"}
    asyncio.run(m.poll_fills())
    assert ("mkt-a", "bid") not in m.resting, "the terminal order is popped"
    assert bid.order_id in m.pending_reconcile, (
        "…but it must be PARKED — a pop without a parked verify loses a stale-read fill forever")
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(0)
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(12), (
        "the delayed verify recovers the dropped fill — the class that never booked at all")


def test_the_durable_record_retires_at_the_VERIFY_in_both_directions(tmp_path, monkeypatch):
    """⛔ The first park-fix removed clear_order from the pop and retired
    it NOWHERE — records accumulated until a crash-restart refused via PriorRunUnresolved over
    verified-dead orders (measured: 4 durable records after 3 cycles, still 4 after retirement).
    Both directions must be pinned: the record survives the pop (clearing there erased it before
    the verify could book a stale-read fill) AND is gone after the verify retires."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "retire.json"))
    client = _StaleClient()
    m = _maker(client, size=12, shadow=False, real=True, state_store=store,
               fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.schedules[bid.order_id] = [0, 12]
    client.open_orders = [o for o in client.open_orders if o.get("id") != bid.order_id]
    client.states = {bid.order_id: "ORDER_STATE_FILLED"}
    asyncio.run(m.poll_fills())
    def _ids():          # records are keyed by INTENT id; the venue id lives in the value
        return {o.get("order_id") for o in (store.snapshot().orders or {}).values()}
    assert bid.order_id in _ids(), (
        "the durable record must SURVIVE the pop — the verify has not run yet")
    asyncio.run(m._retry_pending_reconciles(ignore_due=True))
    assert bid.order_id not in _ids(), (
        "…and must be GONE once the verify retires it, or records accumulate until a "
        "crash-restart refuses over verified-dead orders")


def test_the_parked_set_is_bounded_scales_and_drops_the_NEWEST(tmp_path):
    """Without a bound, a venue outage parks every cancel and then spends a request per parked
    order per cycle forever. The bound must SCALE with the run — a fixed bound covers only a
    couple of repricing markets — and eviction must drop the NEWEST entry. Dropping the
    oldest removes the entry CLOSEST to its verify, i.e. the highest-information one, which for
    that order is the pre-fix fill-loss behaviour returning through the back door."""
    from bot.poly_us.maker import RestingOrder, MAX_PARKED
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    for i in range(m.max_parked + 3):
        m._park(RestingOrder(slug="mkt-a", side="bid", price=Decimal("0.5"), size=5,
                             order_id=f"OID-{i}", intent_id=f"i{i}", placed_ts=0.0,
                             queue_ahead=None, improved=False))
    assert len(m.pending_reconcile) == m.max_parked
    assert "OID-0" in m.pending_reconcile, (
        "the OLDEST must SURVIVE — it is closest to its verify and keeps its place")
    assert f"OID-{m.max_parked + 2}" in m.pending_reconcile, "the incoming entry always lands"
    assert f"OID-{m.max_parked}" not in m.pending_reconcile, "the churned NEWEST entries drop"


def test_the_parked_bound_scales_with_the_slate():
    """~2 x slugs x horizon/requote, floored at MAX_PARKED: 33 markets at requote 10 must clear
    the ~792-entry honest steady state that the fixed bound of 64 would have evicted through."""
    from bot.poly_us.maker import MAX_PARKED
    one = _maker(FakeClient(), shadow=False)
    many = _maker(FakeClient(), slugs=[f"mkt-{i}" for i in range(33)], shadow=False)
    assert one.max_parked == MAX_PARKED
    assert many.max_parked >= 2 * 33 * 12, f"bound {many.max_parked} does not cover 33 markets"


def test_verify_reads_are_COUNTED_in_the_cycle_stats(tmp_path, monkeypatch):
    """The other half of the same fix: the first version ran the verify BEFORE the stats reset, so its
    reads were erased from stats.requests and the rate line could never show them. A cycle that
    performs a verify read must report it."""
    client = _LagClient(blind=99)              # verify read always blind -> parked stays, 1 read/cycle
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    monkeypatch.setattr(maker, "LAG_HORIZON_S", 0.0)
    monkeypatch.setattr(maker, "VERIFY_RETRY_S", 0.0)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    baseline = asyncio.run(m.run_cycle()).requests
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    with_verify = asyncio.run(m.run_cycle()).requests
    assert with_verify > baseline, (
        f"the verify read must appear in stats.requests ({with_verify} vs baseline {baseline}) — "
        f"an uncounted read is invisible to the rate budget")


def test_teardown_does_NOT_retire_a_stale_early_answer(tmp_path, monkeypatch):
    """⛔ A reproduction of the real failure, pinned. At teardown the reads run early (ignore_due),
    and the old code RETIRED an entry on any readable answer — so a stale cum=0 answered, booked
    nothing, retired, and three confident FLAT lines printed over a real +12. Early answers may
    BOOK (cum is monotone; deltas are real) but must not CERTIFY: inside its horizon the entry
    stays parked, and the later pass that sees the true cum books it."""
    client = _StaleClient()
    m = _maker(client, size=12, shadow=False, fill_csv=str(tmp_path / "f.csv"),
               flatten_wait_s=0.0)
    # Collapse only the 20s gaps between teardown passes; the horizon itself stays REAL, which is
    # the point — nothing is due, so only the retire rule decides whether the truth gets booked.
    real_sleep = asyncio.sleep
    async def _fast(_s):
        await real_sleep(0)
    monkeypatch.setattr(maker.asyncio, "sleep", _fast)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    # pre-cancel read: stale 0. teardown pass 1: stale 0 (books nothing, must NOT retire).
    # teardown pass 2: the truth, 12.
    client.schedules[bid.order_id] = [0, 0, 12]
    asyncio.run(m._apply_action(maker.CANCEL, "mkt-a", "bid", None, None, False))
    results = asyncio.run(m.teardown())
    assert m.inventory["mkt-a"] == Decimal(12), (
        "the stale early answer must not end the search — the later pass must book the truth")
    outcome = dict(results)["reconcile_pending"]
    assert "uncertified" in outcome or "reconciled" in outcome


# ── adverse cooldown ─────────────────────────────────────────────────────────────────────────────
# Policy from a recorded news-gap replay: the passive reduce-only chase beat every crossing
# stop, but shortly after the adverse close the maker re-entered the same book and bought the top of
# the deflating spike. The cooldown removes exactly that re-entry: after a round trip realizes
# worse than ADVERSE_RT_PER_CONTRACT per contract, the book quotes REDUCE-ONLY (flat → nothing)
# for adverse_cooldown_s.

def _acct(*steps, prev="0", avg="0"):
    """Run fill_accounting over (side, price, qty) steps; return final state + last `completed`."""
    inv, a = Decimal(prev), Decimal(avg)
    rt_r = rt_c = Decimal(0)
    completed = None
    for side, price, qty in steps:
        inv, a, rt_r, rt_c, completed = maker.fill_accounting(
            inv, a, rt_r, rt_c, side, Decimal(price), Decimal(qty))
    return inv, a, rt_r, rt_c, completed


def test_fill_accounting_a_losing_short_round_trip_realizes_the_chase_cost():
    """A losing short round trip: sell 5, chase the book back up, buy the same 5 back higher.

    The realized loss is the CHASE COST, and it must land in full — the shape of a news gap.
    """
    inv, avg, _, _, completed = _acct(("ask", "0.589", 5), ("bid", "0.668", 5))
    assert inv == Decimal(0)
    assert completed == (Decimal("-0.395"), Decimal(5))
    assert avg == Decimal(0), "flat must reset the entry basis"


def test_fill_accounting_a_long_round_trip_signs_the_other_way():
    _, _, _, _, completed = _acct(("bid", "0.406", 5), ("ask", "0.390", 5))
    assert completed == (Decimal("-0.080"), Decimal(5))
    _, _, _, _, completed = _acct(("bid", "0.553", 5), ("ask", "0.554", 5))
    assert completed == (Decimal("0.005"), Decimal(5))


def test_fill_accounting_adding_averages_the_entry_and_partial_reduce_keeps_the_trip_open():
    inv, avg, rt_r, rt_c, completed = _acct(("bid", "0.40", 5), ("bid", "0.42", 5),
                                            ("ask", "0.39", 4))
    assert inv == Decimal(6)
    assert avg == Decimal("0.41")
    assert rt_r == (Decimal("0.39") - Decimal("0.41")) * 4
    assert rt_c == Decimal(4)
    assert completed is None, "a partial reduce must NOT read as a completed round trip"


def test_fill_accounting_a_through_zero_fill_completes_the_trip_and_opens_fresh_at_the_fill_px():
    inv, avg, rt_r, rt_c, completed = _acct(("ask", "0.60", 5), ("bid", "0.65", 10))
    assert inv == Decimal(5)
    assert completed == (Decimal("-0.25"), Decimal(5))
    assert avg == Decimal("0.65"), "the remainder is a NEW position at the crossing fill's price"
    assert (rt_r, rt_c) == (Decimal(0), Decimal(0)), "fresh trip, fresh accumulators"


def _drive_fill(m, side, price, qty, oid):
    order = maker.RestingOrder(slug="mkt-a", side=side, price=Decimal(price), size=qty,
                               order_id=oid, intent_id=f"i-{oid}", placed_ts=time.time(),
                               queue_ahead=None, improved=False)
    fill = m._book_fill(order, _order_read(oid, qty))
    assert fill is not None
    return fill


def test_an_adverse_round_trip_arms_the_cooldown_and_a_mild_one_does_not():
    client = FakeClient()
    m = _maker(client)
    _drive_fill(m, "ask", "0.589", 5, "OID-A1")
    _drive_fill(m, "bid", "0.668", 5, "OID-A2")     # −7.9¢/contract: well past the 2¢ trigger
    assert m.cooldown_until.get("mkt-a", 0.0) > time.time() + m.adverse_cooldown_s - 5

    m2 = _maker(FakeClient())
    _drive_fill(m2, "bid", "0.406", 5, "OID-B1")
    _drive_fill(m2, "ask", "0.390", 5, "OID-B2")    # −1.6¢/contract: ordinary drift exit
    assert m2.cooldown_until.get("mkt-a", 0.0) == 0.0

    m3 = _maker(FakeClient(), adverse_cooldown_s=0.0)
    _drive_fill(m3, "ask", "0.589", 5, "OID-C1")
    _drive_fill(m3, "bid", "0.668", 5, "OID-C2")
    assert m3.cooldown_until.get("mkt-a", 0.0) == 0.0, "0 must disable the trigger entirely"


def test_a_cooling_flat_book_pulls_both_quotes_and_requotes_after_expiry():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert len(client.placed) == 2
    m.cooldown_until["mkt-a"] = time.time() + 60.0
    asyncio.run(m.run_cycle())
    assert len(client.cancelled) == 2, "flat + cooling must CANCEL both resting quotes"
    assert len(client.placed) == 2, "and place nothing new"
    m.cooldown_until["mkt-a"] = time.time() - 1.0
    asyncio.run(m.run_cycle())
    assert len(client.placed) == 4, "an expired cooldown must quote again"


def test_a_cooling_book_with_inventory_keeps_ONLY_the_reducer_working():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(-5)              # short: the bid is the reducer
    m.cooldown_until["mkt-a"] = time.time() + 60.0
    asyncio.run(m.run_cycle())
    sides = {o["side"] for o in client.placed}
    assert sides == {"buy"}, (
        f"a cooling short must keep its buy-back working and place NO ask, got {sides} — "
        f"going fully dark would leave the inventory unmanaged, the flaw the through-zero "
        f"crossing case would otherwise hit")


def test_a_cooling_reducer_is_sized_to_the_inventory_never_to_size():
    """A cooling short 2 quoted a full-size 5-lot bid fills through zero
    to long 3 — a brand-new position opened DURING the stand-down that exists to prevent exactly
    that. The reducer is sized to the inventory it reduces, like the teardown residual."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(-2)
    m.cooldown_until["mkt-a"] = time.time() + 60.0
    asyncio.run(m.run_cycle())
    bids = [o for o in client.placed if o["side"] == "buy"]
    assert len(bids) == 1 and bids[0]["size"] == 2, (
        f"the cooling reducer must be 2 lots (the inventory), got {bids}")


def test_a_full_size_reducer_resting_from_before_the_cooldown_is_replaced_smaller():
    """The HOLD escape: same price but wrong size must REPLACE — a pre-cooldown 5-lot bid held on
    price equality would keep the through-zero flip live for the whole stand-down."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                      # rests bid+ask at full size on the touch
    m.inventory["mkt-a"] = Decimal(-2)
    m.cooldown_until["mkt-a"] = time.time() + 60.0
    asyncio.run(m.run_cycle())
    last_bid = [o for o in client.placed if o["side"] == "buy"][-1]
    assert last_bid["size"] == 2, (
        f"the resting full-size bid must be replaced at inventory size, got {last_bid}")


def test_a_late_booked_completion_never_arms_the_cooldown():
    """The delayed verify books up to 120s late, so arrival order is not
    fill order and a late delta can complete a FICTIONAL round trip. Inventory still books (it is
    correct either way); only the trigger declines to act on re-ordered history."""
    client = FakeClient()
    m = _maker(client)
    _drive_fill(m, "ask", "0.589", 5, "OID-L1")
    order = maker.RestingOrder(slug="mkt-a", side="bid", price=Decimal("0.668"), size=5,
                               order_id="OID-L2", intent_id="i-L2", placed_ts=time.time(),
                               queue_ahead=None, improved=False)
    fill = m._book_fill(order, _order_read("OID-L2", 5), late_booked=True)
    assert fill is not None and m.inventory["mkt-a"] == Decimal(0), "inventory must still book"
    assert m.cooldown_until.get("mkt-a", 0.0) == 0.0, (
        "a −7.9¢/contract trip completed by a LATE-BOOKED fill must not arm the cooldown")


def _maker_with_state(tmp_path, **kw):
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "maker_state.json"))
    store.begin_run("test-run", mode="real", loss_cap=kw.get("loss_cap", Decimal("3.00")),
                    tickers=["mkt-a"], inventory={})
    # real=True AND shadow=False: the first cut of these tests ran
    # shadow-with-store (a configuration production never builds) and the second cut fixed only
    # the `real` half — the shim refuses --shadow with --i-understand-real-money, so the honest
    # test mode is the production one.
    kw.setdefault("real", True)
    kw.setdefault("shadow", False)
    m = _maker(FakeClient(), state_store=store, **kw)
    return m, store


def test_a_realized_loss_past_the_cap_halts_and_the_ratchet_is_durable(tmp_path):
    """The maker ran real-money sessions with loss_cap=_ZERO and add_realized never called —
    begin_run's own docstring promised 'a lifetime ratchet across processes' that no maker in
    the codebase could actually reach. This drives the whole chain: a completed
    round trip feeds the durable total, breach halts through should_stop, and the state file
    carries the number a restart's assess_recovery will refuse on."""
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("3.00"))
    _drive_fill(m, "ask", "0.500", 5, "OID-LC1")
    _drive_fill(m, "bid", "0.900", 5, "OID-LC2")     # −$2.00 realized: under the cap
    assert m.should_stop is False
    assert store.snapshot().realized_pnl == Decimal("-2.00"), "the durable total must be fed"
    _drive_fill(m, "ask", "0.500", 5, "OID-LC3")
    _drive_fill(m, "bid", "0.800", 5, "OID-LC4")     # −$1.50 more → −$3.50 net: breach
    assert m.should_stop is True
    assert "LOSS CAP" in (m.halt_reason or "")
    assert store.snapshot().cap_breached, "assess_recovery must refuse the NEXT start on this"


def test_LAST_NIGHTS_profit_does_not_fund_tonights_losses(tmp_path):
    """⛔ The defect that quietly doubled a session's real risk budget: the halt compared the RAW
    durable net, so a carried profit had to be lost back before the cap even started counting —
    the run's true budget was the cap PLUS yesterday's gains, while every doc said the cap. The
    session axis binds on THIS run's own loss; the lifetime axis floors at zero (`loss_to_date`).
    Profit buys budget on neither."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "carried_profit.json"))
    store.begin_run("prior", mode="real", loss_cap=Decimal("3.00"), tickers=["mkt-a"], inventory={})
    store.add_realized(Decimal("3.40"))          # a good previous session
    m = _maker(FakeClient(), state_store=store, real=True, shadow=False,
               loss_cap=Decimal("3.00"))
    _drive_fill(m, "ask", "0.500", 12, "OID-CP1")
    _drive_fill(m, "bid", "0.800", 12, "OID-CP2")   # −$3.60 THIS run: past the cap
    assert m.should_stop is True, (
        "a $3.60 loss tonight must halt a $3.00 cap regardless of last night's +$3.40")
    assert "this run" in (m.halt_reason or ""), m.halt_reason
    assert store.snapshot().realized_pnl == Decimal("-0.20"), (
        "the lifetime ledger still nets, and still carries — only the BUDGET is session-scoped")


def test_profit_offsets_losses_in_the_net_but_never_banks_extra_budget(tmp_path):
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("3.00"))
    _drive_fill(m, "bid", "0.400", 5, "OID-LP1")
    _drive_fill(m, "ask", "0.900", 5, "OID-LP2")     # +$2.50 profit
    _drive_fill(m, "ask", "0.500", 5, "OID-LP3")
    _drive_fill(m, "bid", "0.900", 5, "OID-LP4")     # −$2.00 → net +$0.50: no halt
    assert m.should_stop is False
    _drive_fill(m, "ask", "0.500", 5, "OID-LP5")
    _drive_fill(m, "bid", "0.900", 5, "OID-LP6")     # −$2.00 more → net −$1.50: still no halt
    assert m.should_stop is False, "profit legitimately offsets in the NET semantics"


def test_a_late_booked_completion_STILL_feeds_the_loss_cap(tmp_path):
    """Money is order-invariant even when attribution is not: the cooldown ignores late-booked
    trips (re-ordered history), the cap must NOT — a late delta's dollars are real dollars."""
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("3.00"))
    _drive_fill(m, "ask", "0.500", 5, "OID-LL1")
    order = maker.RestingOrder(slug="mkt-a", side="bid", price=Decimal("0.900"), size=5,
                               order_id="OID-LL2", intent_id="i-LL2", placed_ts=time.time(),
                               queue_ahead=None, improved=False)
    m._book_fill(order, _order_read("OID-LL2", 5), late_booked=True)   # −$2.00, late
    assert store.snapshot().realized_pnl == Decimal("-2.00"), (
        "a late-booked round trip's realized loss must reach the durable cap")
    assert m.cooldown_until.get("mkt-a", 0.0) == 0.0, "…while still never arming the cooldown"


def test_a_breach_during_the_fill_polls_ends_the_cycle_before_any_quote(tmp_path):
    """The first cut set should_stop inside _book_fill and run_cycle never
    read it — a breach placed a full fresh round of quotes across every book before the shim's
    check. The breach must end the cycle the way the kill switch does: pull the resting quotes,
    attribute the halt, return."""
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("1.00"), shadow=False,
                                 slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    placed_before = len(m.client.placed)
    assert placed_before == 4, "sanity: both sides resting on both books"
    _drive_fill(m, "ask", "0.100", 5, "OID-BR1")
    _drive_fill(m, "bid", "0.900", 5, "OID-BR2")      # −$4.00 → breach
    assert m.should_stop is True
    fetches_before = len(m.client.fetches)
    stats = asyncio.run(m.run_cycle())
    # ⚠️ The discriminating observables, chosen the hard way: a first version asserted
    # "no placements + cancels happened" and stayed GREEN with the halt check deleted — the
    # breach trip also arms the adverse COOLDOWN, whose (N,N) on the breaching book cancels the
    # same quotes and HOLDs the rest, mimicking the halt. Only the halt check stops the cycle
    # BEFORE the book reads and attributes the reason on the cycle stats; the cooldown does
    # neither, and mkt-b (never in cooldown) would still be quoted without the check.
    assert len(m.client.fetches) == fetches_before, (
        "a breached cycle must end before ANY book read — a fetch here means the quote loop ran")
    assert stats.halt_reason is not None and "LOSS CAP" in stats.halt_reason, (
        "the halt must be attributed on the cycle tape, not only on the engine flag")
    assert len(m.client.placed) == placed_before, "and no fresh quotes anywhere, incl. mkt-b"


def test_prepare_writes_the_cap_to_the_durable_state(tmp_path):
    """Reverting begin_run's loss_cap back to _ZERO left the whole suite
    green while the RESTART refusal silently died — assess_recovery reads the DISK cap, and no
    test asserted what the engine writes there."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "s.json"))
    m = _maker(FakeClient(), state_store=store, real=True, shadow=False,
               loss_cap=Decimal("7.77"))
    asyncio.run(m.prepare())
    assert store.snapshot().loss_cap == Decimal("7.77"), (
        "the disk cap is the one the next start's refusal runs on")


def test_a_breach_raised_INSIDE_the_quote_loop_stops_the_remaining_books(tmp_path):
    """`_book_fill` is also reachable from a requote's cancel read-back INSIDE the quote loop,
    and the post-poll check cannot see a breach raised there — a reproduction placed a fresh
    round of orders after exactly such a breach. The loop must break on the
    flag before quoting the next book."""
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("1.00"),
                                 slugs=["mkt-a", "mkt-b"])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                      # rests both sides on both books
    _drive_fill(m, "bid", "0.900", 12, "OID-QL0")   # long 12 @ 0.90, no trip yet
    # ⚠️ The touch must actually MOVE, or every book HOLDs and the test cannot tell a broken
    # break from a working one (the first version set the book to its own default value and the
    # mutation SURVIVED). Moved → both books want REPLACE → mkt-a's cancel read-back books the
    # 12-lot sale at its resting 0.46 → −$5.28 trip → breach fires mid-loop, before mkt-b
    # ('mkt-a' sorts first) is reached.
    m.client.book = {"marketData": {"bids": [{"px": "0.50", "qty": "376"}],
                                    "offers": [{"px": "0.53", "qty": "500"}]}}
    ask = m.resting.get(("mkt-a", "ask"))
    assert ask is not None, "sanity: an ask rests from cycle 1"
    m.client.orders[ask.order_id]["cumQuantity"] = 12
    # ⚠️ The fill must be INVISIBLE to poll_fills and visible only to the cancel read-back —
    # otherwise the breach fires on the poll path and this test cannot distinguish the two
    # checks (the first version did exactly that, and the in-loop mutation SURVIVED). This is
    # the documented "fills UNVERIFIED this poll" branch: an open-orders read that fails.
    async def _open_orders_fail(slugs=None):
        raise RuntimeError("open-orders read failed")
    m.client.get_open_orders = _open_orders_fail
    placed_before = len(m.client.placed)
    stats = asyncio.run(m.run_cycle())
    assert m.should_stop is True, "the read-back trip must breach"
    placed_after = [o for o in m.client.placed[placed_before:]]
    assert not any(o["token"] == "mkt-b" for o in placed_after), (
        f"mkt-b was quoted AFTER an in-loop breach: {placed_after}")
    assert stats.halt_reason is not None and "LOSS CAP" in stats.halt_reason, (
        "the in-loop breach must still be attributed on the cycle tape")
    # ⛔ The first `break` reached the cycle's NORMAL exit, which cancels
    # nothing — four orders stayed resting and the tape read healthy, and THIS test was green
    # over it because it only checked mkt-b. A halt must pull the quotes.
    assert not m.resting, f"a halted cycle must leave NOTHING resting, got {list(m.resting)}"
    # The ask is the side whose cancel read-back booked the breaching fill. `_apply_action`
    # cancels then places, so without the should_stop re-check between them it re-quotes the
    # very order that spent the budget. (The bid was placed earlier in the same _quote_market
    # call, BEFORE the breach existed — legitimate, and the halt cancels it anyway.)
    assert not any(o["token"] == "mkt-a" and o["side"] == "sell" for o in placed_after), (
        f"the breaching side was re-placed after the halt: {placed_after}")


# ── the unrealized-mark tripwire + per-book cap-fills ─────────────────────────────────────────
# The durable cap is price-REALIZED-only and the adverse cooldown fires only after a COMPLETED
# round trip. Over a long recorded run the cooldown never fired once: every completed trip came
# in well inside the threshold, while the ACCUMULATION phase — which neither rail can see — is
# exactly where inventory risk builds. The tripwire watches the MARK — inventory against the
# LIQUIDATION side of the live touch — and forces the proven reduce-only path when it breaches.

def test_mark_pnl_marks_against_the_LIQUIDATION_side():
    """A long liquidates into the BID, a short covers at the ASK — marking a long at the ask
    (or mid) flatters exactly when the book is running away."""
    # long 22 @ avg 0.84, touch 0.78/0.79 → (0.78 − 0.84) × 22 = −1.32
    assert maker.mark_pnl(Decimal("22"), Decimal("0.84"),
                          Decimal("0.78"), Decimal("0.79")) == Decimal("-1.32")
    # short 12 opened @ 0.545, touch 0.59/0.60 → (0.545 − 0.60) × 12 = −0.66
    assert maker.mark_pnl(Decimal("-12"), Decimal("0.545"),
                          Decimal("0.59"), Decimal("0.60")) == Decimal("-0.66")
    # a winning long marks positive
    assert maker.mark_pnl(Decimal("10"), Decimal("0.50"),
                          Decimal("0.55"), Decimal("0.56")) == Decimal("0.50")
    assert maker.mark_pnl(Decimal("0"), Decimal("0"),
                          Decimal("0.5"), Decimal("0.51")) is None


def _adverse_long_maker(mark_trip_per_ct, avg=Decimal("0.84"), quote_csv=None):
    """A maker holding long 22 @ avg into a book whose touch has collapsed to 0.78/0.79 —
    marked at −0.06 per contract against an avg of 0.84, and invisible to both the realized
    cap and the post-trip cooldown."""
    client = FakeClient(book={"marketData": {"bids": [{"px": "0.78", "qty": "300"}],
                                             "offers": [{"px": "0.79", "qty": "300"}]}})
    kw = {"quote_csv": quote_csv} if quote_csv else {}
    m = _maker(client, shadow=False, size=22, mark_trip_per_ct=mark_trip_per_ct, **kw)
    m.inventory["mkt-a"] = Decimal("22")
    if avg is not None:
        m.avg_entry["mkt-a"] = avg
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    return client, m


def test_the_mark_tripwire_forces_reduce_only_DURING_accumulation(tmp_path):
    # a 0.06/contract mark against a 0.05/contract threshold → breach.
    import csv as _csv
    client, m = _adverse_long_maker(Decimal("0.05"), quote_csv=str(tmp_path / "q.csv"))
    sides = {p["side"] for p in client.placed if p["token"] == "mkt-a"}
    assert "buy" not in sides, "the adding side must be blocked while the mark is breached"
    assert "sell" in sides, "the exit must keep working — reduce-only, not a halt"
    assert m.cooldown_until.get("mkt-a", 0) > 0, "rides the proven cooldown machinery"
    assert m.mark_trips.get("mkt-a", 0) >= 1, "counted, never silent"
    # The tape ATTRIBUTION is the load-bearing half: the tripwire arms the same reduce-only
    # clock an A/B arm measures, so the arm's own effect must be computable EXCLUDING these
    # cycles, straight off the status column — taping them as "cooldown" would confound the
    # experiment with its own safety rail.
    m._close_writers()
    rows = list(_csv.DictReader(open(tmp_path / "q.csv")))
    assert rows and rows[-1]["status"] == "mark_trip", (
        f"tripwire cycles must tape as mark_trip, got {rows[-1]['status']!r}")


def test_the_tripwire_stays_quiet_above_its_threshold():
    # a 0.06/contract mark against a 0.08/contract threshold → no breach.
    client, m = _adverse_long_maker(Decimal("0.08"))
    sides = {p["side"] for p in client.placed if p["token"] == "mkt-a"}
    assert sides == {"buy", "sell"}, "no breach → both sides quote normally"


def test_a_zero_threshold_disables_the_tripwire():
    client, m = _adverse_long_maker(Decimal("0"))
    sides = {p["side"] for p in client.placed if p["token"] == "mkt-a"}
    assert sides == {"buy", "sell"}


def test_an_INHERITED_position_with_no_basis_arms_the_tripwire():
    """avg_entry is populated by THIS run's fills, so a carried position
    has no basis — and reading absence as zero marked a carried 22-lot as +$18.48 of GAIN,
    blinding the rail on the one position class nothing else watches. Absence is not a
    basis: un-markable inventory arms reduce-only until it exits."""
    client, m = _adverse_long_maker(Decimal("0.05"), avg=None)
    sides = {p["side"] for p in client.placed if p["token"] == "mkt-a"}
    assert "buy" not in sides and "sell" in sides
    assert m.mark_trips.get("mkt-a", 0) >= 1
    # And the pure function refuses the flattering read outright:
    assert maker.mark_pnl(Decimal("22"), Decimal("0"),
                          Decimal("0.84"), Decimal("0.85")) is None


def test_per_book_cap_fills_compose_with_per_book_sizes():
    """cap-fills accepts the same spec discipline as --size: one int for all, or per-book
    naming EVERY slug (an unnamed book refuses rather than inheriting a headroom the operator
    never chose)."""
    m = _maker(FakeClient(), slugs=["mkt-a", "mkt-b"], size="mkt-a:22,mkt-b:12",
               cap_fills="mkt-a:2,mkt-b:1")
    assert m.caps == {"mkt-a": 44, "mkt-b": 12}
    m2 = _maker(FakeClient(), slugs=["mkt-a", "mkt-b"], size="mkt-a:22,mkt-b:12",
                cap_fills=2)
    assert m2.caps == {"mkt-a": 44, "mkt-b": 24}
    import pytest as _pytest
    with _pytest.raises(ValueError):
        _maker(FakeClient(), slugs=["mkt-a", "mkt-b"], size="mkt-a:22,mkt-b:12",
               cap_fills="mkt-a:2")


# ── tape attribution: every row carries the run_id ────────────────────────────────────────────

def test_every_tape_row_carries_the_run_id_in_the_LAST_column(tmp_path):
    """Analyses keep segmenting the tapes by timestamp guesswork, and the append-mode trap has
    caught log readers more than once. run_id makes attribution a join instead. LAST column on
    purpose: the DictReader consumers are name-keyed, so appending at the end is the one schema
    change that leaves every existing consumer working."""
    import csv as _csv
    client = FakeClient()
    m = _maker(client, shadow=False, quote_csv=str(tmp_path / "q.csv"),
               cycle_csv=str(tmp_path / "c.csv"), fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id].update(
        cumQuantity=5, state="ORDER_STATE_FILLED",
        commissionNotionalTotalCollected={"value": "-0.01", "currency": "USD"})
    client.open_orders = []
    asyncio.run(m.poll_fills())
    m._close_writers()
    assert m.run_id.startswith("polymm-"), "venue prefix first — the recovery split keys on it"
    assert "-dry-" in m.run_id, (
        "the MODE is stamped in: dry/shadow/real share the default tapes and "
        "nothing durable maps an unstamped id back to its kind")
    for name, hdr in (("q.csv", maker._QUOTE_HDR), ("c.csv", maker._CYCLE_HDR),
                      ("f.csv", maker._FILL_HDR)):
        assert hdr[-1] == "run_id", name
        rows = list(_csv.DictReader(open(tmp_path / name)))
        assert rows, f"{name}: no rows written"
        assert all(r["run_id"] == m.run_id for r in rows), name


def test_cycle_rows_carry_avail_and_swap_free(tmp_path, monkeypatch):
    """Several recorded memory halts were un-diagnosable because nothing logged avail/swap as a
    time series — the halt reason existed only at the moment of the halt. Every cycle row now
    carries both, so the memory floor gets validated (or refuted) by the next long run instead
    of by assertion. Wiring pin: values flow from memguard.check's MemStatus, not from a second
    read."""
    import csv as _csv
    from bot.core import memguard as mg
    monkeypatch.setattr(mg, "check", lambda limits=None, label="": mg.MemStatus(
        "ok", "", rss_mb=77.0, avail_mb=433.0, tmpfs_used_mb=1.0, swap_free_mb=6141.0))
    client = FakeClient()
    m = _maker(client, quote_csv=str(tmp_path / "q.csv"),
               cycle_csv=str(tmp_path / "c.csv"), fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m._close_writers()
    rows = list(_csv.DictReader(open(tmp_path / "c.csv")))
    assert rows and rows[-1]["avail_mb"] == "433" and rows[-1]["swap_free_mb"] == "6141"


def test_non_real_makers_default_to_their_OWN_tape_paths():
    """The incident: two dry makers constructed OUTSIDE pytest (a debug harness importing this
    file's fixtures directly — so no conftest sandbox) wrote to the real default tapes, and the
    schema-rotation logic then renamed the LIVE real-money tape out from under a running
    real-money process, mid-session. The structural
    close: a non-real maker's DEFAULT tape paths are its own (.dry.csv), so no future
    out-of-pytest dry construction can touch the real tapes — the sandbox becomes
    defense-in-depth instead of the only wall."""
    dry = _maker(FakeClient(), shadow=False, real=False)
    assert dry._quote_path.endswith(".dry.csv") and dry._quote_path != maker.DEFAULT_QUOTE_CSV
    assert dry._cycle_path.endswith(".dry.csv") and dry._cycle_path != maker.DEFAULT_CYCLE_CSV
    assert dry._fill_path.endswith(".dry.csv") and dry._fill_path != maker.DEFAULT_FILL_CSV
    shadow = _maker(FakeClient(), shadow=True, real=False)
    assert shadow._quote_path.endswith(".dry.csv")
    real = _maker(FakeClient(), shadow=False, real=True)
    assert real._quote_path == maker.DEFAULT_QUOTE_CSV, "a real maker keeps the real tape"
    explicit = _maker(FakeClient(), shadow=False, real=False, quote_csv="x/q.csv")
    assert explicit._quote_path == "x/q.csv", "an explicit path is always honoured"


def test_a_tape_schema_change_rotates_the_old_file_never_misaligns(tmp_path):
    """The Kalshi tape once wrote run_id as a 7th column against a 6-column header on disk —
    DictReader filed it under None. The Poly writer now freeze-rotates a header-mismatched tape
    into rotated/<stem>.pre-<tag>.csv, so the old rows stay readable under the schema they were
    written with."""
    p = tmp_path / "tape.csv"
    p.write_text("a,b\n1,2\n")
    handle, _w = maker._open_writer(str(p), ["a", "b", "run_id"], rotate_tag="runid")
    handle.close()
    rotated = tmp_path / "rotated" / "tape.pre-runid.csv"
    assert rotated.exists() and rotated.read_text() == "a,b\n1,2\n"
    assert p.read_text().splitlines()[0] == "a,b,run_id"
    handle, w = maker._open_writer(str(p), ["a", "b", "run_id"], rotate_tag="runid")
    w.writerow(["1", "2", "polymm-x"])
    handle.close()
    assert len(p.read_text().splitlines()) == 2, "matching header appends in place"
    # A TUPLE header must compare equal too — the hint says Sequence, and without the list()
    # coercion a tuple caller would rotate on EVERY open, shredding the tape into one
    # timestamped file per writer.
    handle, _w = maker._open_writer(str(p), ("a", "b", "run_id"), rotate_tag="runid")
    handle.close()
    assert len(p.read_text().splitlines()) == 2, "tuple header: no spurious rotation"
    assert len(list((tmp_path / "rotated").iterdir())) == 1, "still only the one rotation"


# ── free book-stats on the quote tape ─────────────────────────────────────────────────────────
# sharesTraded deltas are per-trade-exact (a recorded sample put Δcounter/quantity at exactly
# 1.0 in all but one case) and ride FREE on the book read the maker already makes — the quote tape
# becomes the prints-resolution flow series, closing the "flow denominators are hourly floors"
# gap (and the sweep-size / traded-without-us blind spots) at zero extra requests.


def test_the_quote_tape_carries_the_free_book_stats(tmp_path):
    import csv as _csv
    assert maker._QUOTE_HDR[-1] == "run_id" and maker._QUOTE_HDR[-6:-2] == [
        "shares_traded", "last_trade_px", "last_trade_qty", "last_trade_age_s"], (
        "the flow columns still slot before run_id; book_src is now the last before run_id")
    assert maker._QUOTE_HDR[-2] == "book_src"
    client = FakeClient(book={"marketData": {
        "bids": [{"px": "0.44", "qty": "376"}], "offers": [{"px": "0.47", "qty": "500"}],
        "stats": {"sharesTraded": "1234567", "lastTradePx": {"value": "0.45", "currency": "USD"},
                  "lastTradeQty": "22"}}})
    m = _maker(client, quote_csv=str(tmp_path / "q.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    rows = list(_csv.DictReader(open(tmp_path / "q.csv")))
    assert rows, "shadow cycles still tape quotes"
    assert rows[0]["book_src"] == "rest", "default source stamps rest on every row"
    # The counter must stay digit-exact past 1e6 — ":g" renders 1234567 as
    # "1.23457e+06" (= 1,234,570), silently quantizing deltas to 10-100 shares, which
    # destroys the per-trade exactness this column exists for.
    assert rows[0]["shares_traded"] == "1234567" and rows[0]["last_trade_px"] == "0.45"
    assert rows[0]["last_trade_qty"] == "22"
    assert rows[0]["last_trade_age_s"] == "", (
        "no lastTradeSetTime in the body → BLANK, never 0 — absent is not zero")


def test_a_statsless_book_tapes_blanks_not_zeros(tmp_path):
    import csv as _csv
    client = FakeClient()   # fixture book carries no stats block
    m = _maker(client, quote_csv=str(tmp_path / "q.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    rows = list(_csv.DictReader(open(tmp_path / "q.csv")))
    assert rows and rows[0]["shares_traded"] == "" and rows[0]["last_trade_px"] == "", (
        "a venue that omits stats must tape BLANK — 0 would read as a real frozen counter")


# ── the private-WS fill accelerator (order_feed) drain seam ──────────────────────────────────
# Built after a real run's REST poll missed a fill outright: local belief carried a full lot the
# venue said was flat. (The first version of this note claimed the miss rate was lower than it
# was; the tape refuted it — which is why the count is not quoted here at all.)
# The feed pushes venue order bodies; the drain books them through the SAME cum-idempotent path
# the poll verifies, so acceleration is its only possible effect.

class _StubFeed:
    def __init__(self, bodies):
        self.queue: asyncio.Queue = asyncio.Queue()
        for b in bodies:
            self.queue.put_nowait(b)
        self.echoes_expected: list[str] = []
        self.liveness_checks = 0
        self.liveness_reason: str | None = None

    def expect_echo(self, order_id: str) -> None:
        self.echoes_expected.append(order_id)

    def check_liveness(self) -> str | None:
        self.liveness_checks += 1
        return self.liveness_reason


def _ws_body(order_id: str, cum: int, price: str = "0.45") -> dict:
    return {"id": order_id, "marketSlug": "mkt-a", "side": "ORDER_SIDE_BUY",
            "price": {"value": price, "currency": "USD"}, "quantity": 12,
            "cumQuantity": cum, "leavesQuantity": 12 - cum, "state": "ORDER_STATE_FILLED",
            "commissionNotionalTotalCollected": {"value": "-0.02", "currency": "USD"}}


def test_a_ws_pushed_fill_is_booked_and_the_poll_does_not_double_book():
    """The whole point: the fill lands the moment the venue pushes it — and when the REST poll
    later reads the SAME cumQuantity, the cum-delta is zero, so nothing double-books."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                               # rests a bid, order id known
    bid = m.resting[("mkt-a", "bid")]
    m.order_feed = _StubFeed([_ws_body(bid.order_id, 12, price="0.45")])
    asyncio.run(m.poll_fills())                              # drain + poll in one call
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(12), (
        "the pushed fill must book immediately")
    client.orders[bid.order_id]["cumQuantity"] = 12          # the venue agrees
    asyncio.run(m.poll_fills())                              # the verifying poll
    assert m.inventory["mkt-a"] == Decimal(12), (
        "the poll re-reading the same cum must be a zero-delta no-op, not a double-book")


def test_a_ws_booked_fill_syncs_the_DURABLE_inventory_record(tmp_path):
    """The WS drain booked fills but never wrote `state.set_inventory` — the poll's sync is
    gated on ITS OWN fills list, and a WS-booked fill leaves the later poll a zero delta, so
    the sync never ran on any path. Observed live: every fill booked via ws while the durable
    state file still carried the PRIOR run's inventory, at the OPPOSITE SIGN, on both books.
    A SIGKILL there leaves a crash record whose remediation text names a position we do not
    hold, and a close derived from it would have DOUBLED both positions."""
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    m.order_feed = _StubFeed([_ws_body(bid.order_id, 12, price="0.45")])
    asyncio.run(m.poll_fills())      # drain books via ws; the poll's own read is zero-delta
    assert m.inventory.get("mkt-a") == Decimal(12)
    assert store.snapshot().inventory.get("mkt-a") == Decimal(12), (
        "the DURABLE record must carry a ws-booked fill the moment it books — a crash "
        "before the next poll-path fill otherwise records stale inventory")


def test_the_fill_tape_names_which_path_booked_it(tmp_path):
    """The accepted risk of turning the WS path on is corruption via a bad WS body — and without
    a `booked_via` column the tape cannot distinguish a WS-booked fill from a poll-booked one,
    so that risk would be unmeasurable. The read it enables is ONE-SIDED: a healthy ws booking
    leaves a ws row with NO later poll/verify row — a zero-delta poll writes nothing, so ABSENCE
    is the evidence; a later positive-delta row is an under-stating body. An over-stating body is
    invisible on this tape (negative deltas book nothing) — that direction belongs to a
    post-teardown order diff against the venue."""
    import csv as _csv
    # header discipline: the new column slots BEFORE run_id, so run_id stays last (the
    # append-last convention the name-keyed DictReader consumers rely on).
    # ⚠️ POSITION-FREE except for run_id. This asserted `_FILL_HDR[-2] == "booked_via"`, which
    # broke the moment a column was appended before run_id — and the invariant it was guarding
    # (this test's own docstring, and the run_id-last test's) is that consumers are NAME-keyed.
    # A positional assertion in the test that documents name-keying was pinning the opposite of
    # what it claimed. run_id-last IS a real convention and keeps its positional check.
    assert maker._FILL_HDR[-1] == "run_id"
    assert "booked_via" in maker._FILL_HDR

    # ws: the drain books it, and the verifying poll's zero delta writes NO second row
    client = FakeClient()
    m = _maker(client, shadow=False, fill_csv=str(tmp_path / "ws.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    m.order_feed = _StubFeed([_ws_body(bid.order_id, 12, price="0.45")])
    asyncio.run(m.poll_fills())
    client.orders[bid.order_id]["cumQuantity"] = 12
    asyncio.run(m.poll_fills())
    rows = list(_csv.DictReader(open(tmp_path / "ws.csv")))
    assert [r["booked_via"] for r in rows] == ["ws"], (
        "one row, labelled ws — a second row here means the verifying poll double-booked")

    # poll: no feed, the REST read-back books it
    client2 = FakeClient()
    m2 = _maker(client2, shadow=False, fill_csv=str(tmp_path / "poll.csv"))
    asyncio.run(m2.prepare())
    asyncio.run(m2.run_cycle())
    bid2 = m2.resting[("mkt-a", "bid")]
    client2.orders[bid2.order_id]["cumQuantity"] = 12
    asyncio.run(m2.poll_fills())
    rows2 = list(_csv.DictReader(open(tmp_path / "poll.csv")))
    assert [r["booked_via"] for r in rows2] == ["poll"]


def test_a_cum_REGRESSION_is_counted_and_named_not_swallowed_with_the_zeros():
    """`cumQuantity` is monotone by contract, so a NEGATIVE delta means one of the two
    reads is wrong — and the pre-existing `increment <= 0` branch swallowed it on the same
    silent path as a healthy zero. That silence is exactly the blind spot of the one-sided
    ws success read: an OVER-stating WS body's later honest poll produces a negative delta
    and nothing anywhere records it. Booking is still refused (which read is right is
    undecidable here — poly_order_diff against the venue's activities is the arbiter); the
    event just stops being invisible."""
    client = FakeClient()
    m = _maker(client)
    order = maker.RestingOrder(slug="mkt-a", side="bid", price=Decimal("0.44"), size=12,
                               order_id="OID-REG", intent_id="i-REG", placed_ts=time.time(),
                               queue_ahead=None, improved=False)
    assert m._book_fill(order, _order_read("OID-REG", 12)) is not None
    assert m.cum_regressions == 0
    fill = m._book_fill(order, _order_read("OID-REG", 10))       # cum went BACKWARDS
    assert fill is None, "a regression must not book"
    assert m.inventory["mkt-a"] == Decimal(12), "belief must not change on an undecidable read"
    assert m.cum_regressions == 1, "the regression must be counted"
    # and a healthy zero-delta re-read stays silent — zeros are not regressions
    assert m._book_fill(order, _order_read("OID-REG", 12)) is None
    assert m.cum_regressions == 1


def test_a_ws_body_for_an_unknown_order_is_ignored():
    """We cannot attribute a side/slug for an order we never placed — the teardown sweep remains
    the net for strays. Booking it would corrupt belief with someone else's order shape."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    m.order_feed = _StubFeed([_ws_body("NEVER-OURS", 12)])
    asyncio.run(m.poll_fills())
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(0)
    assert m.order_feed_unattributed == 1, (
        "the skip is correct but must be COUNTED — foreign orders on the same account are "
        "indistinguishable from our own dropped by a mis-keyed `resting`")


def test_place_registers_an_echo_expectation_but_cancel_does_not():
    """Placement-driven liveness: the recorded evidence is placements→NEW, every one echoed.
    Cancel echoes are OBSERVED on the tape too, but no per-order cancel timestamps exist, so
    neither their rate nor their latency is computable — and an expectation built on an
    unvalidated behaviour is a false-death reconnect storm waiting. So the cancel leg is
    deliberately unarmed."""
    client = FakeClient()
    m = _maker(client, shadow=False, real=True)
    m.order_feed = _StubFeed([])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    assert bid.order_id in m.order_feed.echoes_expected
    n_before = len(m.order_feed.echoes_expected)
    asyncio.run(m._cancel("mkt-a", "bid"))
    assert len(m.order_feed.echoes_expected) == n_before, (
        "a cancel must NOT register an expectation")


def test_a_non_real_maker_never_expects_echoes():
    """A dry client's orders never reach the venue, so nothing can echo — expecting one would
    declare every dry --order-ws run dead within a cycle."""
    client = FakeClient()
    m = _maker(client, shadow=False, real=False)
    m.order_feed = _StubFeed([])
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert m.order_feed.echoes_expected == []


def test_the_drain_checks_feed_liveness_every_cycle():
    """Liveness was computed and never read in-run, so a feed death mid-session surfaced hours
    late — at teardown, not when it happened. The check rides the same per-cycle seam as the
    drain."""
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    m.order_feed = _StubFeed([])
    asyncio.run(m.poll_fills())
    assert m.order_feed.liveness_checks >= 1


# ── the poll's pop condition: positive terminal match ─────────────────────────────────────────
# The venue's order-state vocabulary, measured over every recorded WS body: only NEW, CANCELED,
# FILLED and PARTIALLY_FILLED ever appear — NOTHING containing "OPEN" (the SDK enum
# adds EXPIRED/REJECTED/REPLACED and four PENDING_* states, all unobserved). The old condition
# `"OPEN" not in state` was therefore vacuously true for every real state, live ones included,
# so one stale or truncated get_open_orders listing popped a LIVE order — the maker forgot it,
# stopped cancelling it, and left it resting on the venue untracked. The listing is a HINT;
# the authoritative pop decision is the per-order read's state.


def _resting_maker():
    client = FakeClient()
    m = _maker(client, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                   # rests a bid and an ask, ids known
    return client, m


def test_a_live_order_missing_from_the_listing_is_NOT_forgotten():
    """The money defect: ORDER_STATE_NEW is a LIVE resting order whatever the listing says —
    popping it leaves a live venue order no code path ever cancels."""
    client, m = _resting_maker()
    client.open_orders = []                      # stale replica / truncated listing
    asyncio.run(m.poll_fills())
    assert ("mkt-a", "bid") in m.resting and ("mkt-a", "ask") in m.resting, (
        "a listing miss must never outvote a live per-order state")


def test_a_partial_fill_missing_from_the_listing_books_and_stays():
    """ORDER_STATE_PARTIALLY_FILLED is live — the remainder still rests. It must book the
    partial AND keep being tracked."""
    client, m = _resting_maker()
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id]["state"] = "ORDER_STATE_PARTIALLY_FILLED"
    client.orders[bid.order_id]["cumQuantity"] = 2
    client.orders[bid.order_id]["commissionNotionalTotalCollected"] = {
        "value": "-0.01", "currency": "USD"}
    client.open_orders = []
    asyncio.run(m.poll_fills())
    assert m.inventory.get("mkt-a", Decimal(0)) == Decimal(2)
    assert ("mkt-a", "bid") in m.resting, "the unfilled remainder is still live on the venue"


def test_a_terminal_order_missing_from_the_listing_is_parked_and_popped():
    for terminal in ("ORDER_STATE_FILLED", "ORDER_STATE_CANCELED",
                     "ORDER_STATE_EXPIRED", "ORDER_STATE_REJECTED"):
        client, m = _resting_maker()
        bid = m.resting[("mkt-a", "bid")]
        client.orders[bid.order_id]["state"] = terminal
        client.open_orders = [o for o in client.open_orders if o["id"] != bid.order_id]
        asyncio.run(m.poll_fills())
        assert ("mkt-a", "bid") not in m.resting, terminal
        assert bid.order_id in m.pending_reconcile, (
            f"{terminal}: popped orders park for the delayed verify, never vanish")


def test_every_non_terminal_state_missing_from_the_listing_is_kept(caplog):
    """The classification IS the safety authority now, so every non-terminal member is pinned.
    REPLACED is the load-bearing one: we never send replaces, so its appearance means someone
    else acted on this account and the order must stay tracked — a claim the module comment
    asserted while `REPLACED` could be moved into the terminal set and stay green against the
    ENTIRE suite — exactly that mutant survived until this pin. The PENDING_* family
    can still fill until the venue confirms. Kept LOUDLY — wrongly keeping costs a warning
    while wrongly popping costs an invisible live order."""
    for state in ("ORDER_STATE_NEW", "ORDER_STATE_PARTIALLY_FILLED",
                  "ORDER_STATE_PENDING_NEW", "ORDER_STATE_PENDING_CANCEL",
                  "ORDER_STATE_PENDING_REPLACE", "ORDER_STATE_PENDING_RISK",
                  "ORDER_STATE_REPLACED"):
        client, m = _resting_maker()
        bid = m.resting[("mkt-a", "bid")]
        client.orders[bid.order_id]["state"] = state
        client.open_orders = []
        asyncio.run(m.poll_fills())
        assert ("mkt-a", "bid") in m.resting, state
    assert "keeping it" in caplog.text, (
        "the listing/state disagreement must be visible, not silent")


def test_an_unreadable_state_missing_from_the_listing_is_kept_and_SAID(caplog):
    """Neither authority answered (absent from the listing, read-back carried no state).
    Keeping is right — cannot-verify is not terminal — but silence is not: under a get_order
    outage EVERY resting order lands here at once, and a blind poll must not look like a
    quiet one. This was the only silent path left in the poll."""
    client, m = _resting_maker()
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id]["state"] = ""
    client.open_orders = []
    asyncio.run(m.poll_fills())
    assert ("mkt-a", "bid") in m.resting
    assert "NO state" in caplog.text


def test_the_poll_scopes_the_listing_and_the_sweep_deliberately_does_NOT(tmp_path):
    """The poll's listing is a per-cycle HINT (the state authority
    decides the pop), so it takes the server-side `slugs` bound — all the budget and
    truncation benefit lives there. The SWEEP's listing certifies `swept_clean`, and this
    venue is documented to accept-and-silently-ignore a filter param (`volumeMin`) — a
    mishandled filter returning [] would close the crash record over live orders, so the
    once-per-run certification read stays ACCOUNT-WIDE with the client-side `mine` filter
    scoping what gets cancelled."""
    client, m = _resting_maker()
    asyncio.run(m.poll_fills())
    assert client.open_orders_calls[-1] == ["mkt-a"], "poll_fills must scope to the slate"
    n_before = len(client.open_orders_calls)
    asyncio.run(m._teardown_sweep())
    # The sweep's OWN listing is its first call; it polls again afterwards (parked verify).
    assert client.open_orders_calls[n_before] is None, (
        "the certification read must be account-wide — scoping it puts swept_clean behind "
        "an unverified venue filter")


def test_a_terminal_state_still_in_the_listing_is_kept_this_poll():
    """Venue self-contradiction (terminal state, still listed): wait for the listing to catch
    up rather than popping against it — pins the not-in-listing conjunct."""
    client, m = _resting_maker()
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id]["state"] = "ORDER_STATE_FILLED"
    client.orders[bid.order_id]["cumQuantity"] = 5
    client.orders[bid.order_id]["commissionNotionalTotalCollected"] = {
        "value": "-0.01", "currency": "USD"}
    asyncio.run(m.poll_fills())                  # still present in client.open_orders
    assert ("mkt-a", "bid") in m.resting, (
        "terminal-but-listed is a venue contradiction; keep until the listing agrees")


def test_no_feed_means_yesterdays_behaviour_exactly():
    client = FakeClient()
    m = _maker(client, shadow=False)
    assert m.order_feed is None
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                               # must not raise, no drain effects


def test_a_dry_maker_handed_a_store_never_writes_the_durable_ledger(tmp_path):
    """The `and self.real` guard survived mutation because no test built the
    configuration it guards against. A DRY/shadow maker with a store must not push fictional
    round trips into the cross-process budget a real launch is refused on."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "dry.json"))
    store.begin_run("dry-run", mode="real", loss_cap=Decimal("3.00"), tickers=["mkt-a"], inventory={})
    m = _maker(FakeClient(), state_store=store, real=False)      # shadow default: True
    _drive_fill(m, "ask", "0.100", 5, "OID-DR1")
    _drive_fill(m, "bid", "0.900", 5, "OID-DR2")                 # −$4.00 fictional trip
    assert store.snapshot().realized_pnl == Decimal("0"), (
        "a non-real maker wrote the durable ledger — DRY losses would refuse real launches")
    assert m.should_stop is False


def test_a_breach_never_overwrites_an_existing_halt_reason(tmp_path):
    """First-cause-wins was unpinned — dropping it silently relabels a
    kill-switch exit as a loss-cap exit in the durable record and the heartbeat."""
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("1.00"))
    m.halt_reason = "kill switch (pause.json)"
    _drive_fill(m, "ask", "0.100", 5, "OID-FC1")
    _drive_fill(m, "bid", "0.900", 5, "OID-FC2")
    assert m.should_stop is True
    assert m.halt_reason == "kill switch (pause.json)", (
        "the breach must not rewrite the first cause")


def test_a_carried_breach_halts_the_first_cycle_even_with_no_fills(tmp_path):
    """The in-run check only ran at add_realized time, so a relaunch over an
    already-breached ledger (a tightened cap) with zero completed trips ran to --seconds on a
    budget the operator believed spent. The per-cycle carried check closes it."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "carried.json"))
    store.begin_run("old-run", mode="real", loss_cap=Decimal("5.00"), tickers=["mkt-a"], inventory={})
    store.add_realized(Decimal("-2.50"))
    m = _maker(FakeClient(), state_store=store, real=True, shadow=False,
               loss_cap=Decimal("2.00"))            # tightened below the carried loss
    asyncio.run(m.prepare())
    stats = asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "lifetime" in (m.halt_reason or ""), (
        "the carried total already exceeds the tightened cap — cycle 1 must halt, no fill needed")


# ── per-book sizing ──────────────────────────────────────────────────────────────────────────
# Built after a long run's worth of fills on the highest-flow book showed almost no partials,
# a front-of-queue position on most placements, and fast fills — i.e. clear headroom there —
# while a UNIFORM size-up would have doubled capital sitting behind the enormous queues of the
# join-only books, for no extra fills. Size is a per-book property, not a run-wide one.

# ── venue-truth reconciliation ───────────────────────────────────────────────────────────────
# The regression this section exists for: the local belief read FLAT while the venue held a real
# short several times the position cap. Repeated asks had filled whose confirmations never landed
# inside the read-back window; every belief-side rail was evaluating a fiction, and an EXTERNAL
# read of the venue was the only thing that could see it. Trust the venue read, not the belief.

def test_venue_truth_blocks_the_adding_side_when_belief_is_behind():
    """THE INCIDENT, replayed: belief 0, venue short but still inside the halt threshold. The
    ask (which would add to the short) must be blocked and only the buy-back may quote."""
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(0)                       # the fiction
    m.venue_inventory = {"mkt-a": Decimal(-18)}             # the truth: past cap, under cap+size
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    sides = {o["side"] for o in client.placed}
    assert sides == {"buy"}, (
        f"belief 0 + venue −18 must quote ONLY the buy-back, got {sides} — an ask here is the "
        f"maker adding to a short it cannot see")


def test_the_venue_block_is_NOT_overridden_by_beliefs_own_reducer():
    """⛔ The regression that matters most. An earlier fix ('never take away belief's reducing
    side') PLACED the order the code before it had blocked: with belief slightly LONG and the
    venue short, belief's 'reducer' is an ASK, and permitting it quotes a full-size sell into a
    growing short. Belief spends real time positive while the venue is short, so this state is
    not hypothetical. The venue's adding side stays blocked."""
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(1)                       # belief: barely long
    m.venue_inventory = {"mkt-a": Decimal(-18)}             # truth: short, past cap
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    sells = [o for o in client.placed if o["side"] == "sell"]
    assert not sells, (
        f"a sell here ADDS to an 18-contract short the maker cannot see — belief's reducer must "
        f"not override the venue's block: {sells}")
    # …and the mirror, because a carve-out reintroduced on either side is the same defect.
    client2 = FakeClient()
    m2 = _maker(client2, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m2.prepare())
    m2.inventory["mkt-a"] = Decimal(-1)                     # belief: barely short
    m2.venue_inventory = {"mkt-a": Decimal(18)}             # truth: long, past cap
    m2.venue_inventory_ts = time.time()
    asyncio.run(m2.run_cycle())
    buys = [o for o in client2.placed if o["side"] == "buy"]
    assert not buys, f"a buy here ADDS to an 18-contract long the maker cannot see: {buys}"


def test_the_venue_halt_spares_the_lawful_bound_and_fires_one_past_it():
    """STRICT `>`: |venue| == cap + size (24 here) is LAWFUL — at belief == cap the adding side
    stops being PLACED, but an already-resting full-size order keeps filling until the next
    cycle cancels it — so an inclusive bound halts healthy runs. Measured against a recorded
    series of venue position reads: |cap + size| shows up regularly across many episodes, and so
    do values that are not multiples of the quote size — positions are NOT quantised to it, and
    cap + size is not a ceiling. (An earlier docstring here argued the opposite rule from
    measurements the venue record contradicts; both were retracted.)"""
    for qty, expect_halt in ((Decimal(-24), False), (Decimal(-25), True)):
        client = FakeClient()
        m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
        asyncio.run(m.prepare())
        m.venue_inventory = {"mkt-a": qty}                  # cap 12 + size 12 = 24
        m.venue_inventory_ts = time.time()
        asyncio.run(m.run_cycle())
        assert m.should_stop is expect_halt, (
            f"venue {qty} against cap+size 24: expected halt={expect_halt}")


def test_a_sign_disagreement_does_NOT_halt_because_it_is_the_normal_state():
    """⛔ A sign-disagreement halt was tried and REMOVED — this test exists so it is not re-added.
    Measured against the venue field itself over a recorded run: belief and venue agree on only
    about a third of samples, and the venue is non-zero while belief reads zero on nearly half.
    Disagreement is simply what a fast-filling book looks like, and halting on it stopped EVERY
    simulated run within the hour. MAGNITUDE is the condition that separates a real divergence
    from ordinary operation."""
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(-12)                     # belief: short
    m.venue_inventory = {"mkt-a": Decimal(12)}              # venue: long, inside the bound
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is False, (
        "opposite signs INSIDE the magnitude bound must not halt — that rail cost 30/30 runs")
    assert m._venue_breach() is None


def test_a_hot_LOWERING_does_not_turn_lawful_inventory_into_a_halt(tmp_path, monkeypatch):
    """⛔ The external guard is process-fixed, so RAISES are the hazard there; `_venue_breach`
    recomputes from the LIVE caps, so LOWERINGS are the hazard here — and lowerings are the only
    direction a hot settings change permits. Both shapes are driven through the REAL apply path:
    a size lowered under a position the venue lawfully holds (a whole-run halt if the breach is
    computed against the live caps), and a cap_fills lowering with the venue one routine
    partial-fill past the shrunk cap. The threshold anchors to the FROZEN launch reach, and
    still fires one past it."""
    from bot.core import config as _config
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()

    # shape one: size 85 × cap_fills 2 → launch reach 255; the venue lawfully holds 85.
    client = FakeClient()
    m = _maker(client, size=85, cap_fills=2)
    hot = tmp_path / "hot_a.json"
    hot.write_text('{"updated": "x", "books": {"mkt-a": {"size": 28}}}')
    monkeypatch.setattr(_config, "HOT_SETTINGS_FILE", str(hot), raising=False)
    m.maybe_apply_hot_settings()
    assert m.sizes["mkt-a"] == 28, "the lowering itself must apply"
    m.venue_inventory = {"mkt-a": Decimal(85)}
    assert m._venue_breach() is None, (
        "venue 85 was lawful under the launch config (reach 255) — a hot lowering to 28 "
        "must not halt-and-teardown the run over it")
    m.venue_inventory = {"mkt-a": Decimal(256)}
    assert m._venue_breach() is not None, "one past the launch reach must still halt"

    # shape two: size 5 × cap_fills 2 → launch reach 15; the single permitted cap_fills
    # lowering, with the venue one routine partial-fill overshoot past the shrunk cap.
    client2 = FakeClient()
    m2 = _maker(client2, size=5, cap_fills=2)
    hot2 = tmp_path / "hot_b.json"
    hot2.write_text('{"updated": "x", "books": {"mkt-a": {"cap_fills": 1}}}')
    monkeypatch.setattr(_config, "HOT_SETTINGS_FILE", str(hot2), raising=False)
    m2.maybe_apply_hot_settings()
    m2.venue_inventory = {"mkt-a": Decimal(-11)}
    assert m2._venue_breach() is None, (
        "-11 is inside the launch reach 15 — a cap_fills cut must not end the run on one "
        "contract of documented-lawful overshoot")
    m2.venue_inventory = {"mkt-a": Decimal(-16)}
    assert m2._venue_breach() is not None, "past the launch reach must still halt"


def test_an_unknown_venue_row_is_judged_against_the_FROZEN_launch_max(tmp_path, monkeypatch):
    """⛔ `_refresh_venue_inventory` keeps EVERY venue row, including
    books this run never quoted — another run's carry, an operator position. Their halt bound
    is the launch-time max (`cap + size` at construction) and must not TIGHTEN when a hot
    lowering shrinks the live scalars: un-anchored, a `size 12→6` edit dropped an unrelated
    position's threshold 24→12 mid-run. The same asserts pin the other half: the scalars
    themselves must recompute (a neutralized recompute leaves 12/12 and goes RED here)."""
    from bot.core import config as _config
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1)          # launch max: cap 12 + size 12 = 24
    m.venue_inventory = {"someone-elses-book": Decimal(24)}
    assert m._venue_breach() is None, "24 == the launch max is lawful (strict >)"
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()
    hot = tmp_path / "hot.json"
    hot.write_text('{"updated": "x", "books": {"mkt-a": {"size": 6}}}')
    monkeypatch.setattr(_config, "HOT_SETTINGS_FILE", str(hot), raising=False)
    m.maybe_apply_hot_settings()
    assert m.size == 6 and m.cap == 6, "the scalar recompute must run [verification (e)]"
    assert m._venue_breach() is None, (
        "the unknown row's bound must stay the FROZEN launch max 24 after the lowering — "
        "a shrunk live max halting an unrelated position is the (e) defect")
    m.venue_inventory = {"someone-elses-book": Decimal(25)}
    assert m._venue_breach() is not None, "one past the launch max must still halt"


def test_a_second_hot_file_is_judged_against_what_the_FIRST_one_applied(tmp_path,
                                                                        monkeypatch):
    """⛔ The one precondition that went unpinned for a long time. The reach invariant holds
    only because the parser receives the RUNNING sizes; an engine that passed a frozen
    launch slate restores the two-file exploit byte-for-byte and stays green in every
    pure-parser test (they supply the running slate explicitly). Drive the real engine
    through both files: A takes the paid raise (127 × cf1 = 254 ≤ 255), B "restores"
    cap_fills to launch — legal against the frozen size 85, reach 381 against the running
    127. B must be refused whole and the running config must not move."""
    from bot.core import config as _config
    monkeypatch.chdir(tmp_path)
    (tmp_path / "logs").mkdir()
    client = FakeClient()
    m = _maker(client, size=85, cap_fills=2)          # launch reach 255
    hot = tmp_path / "hot.json"
    monkeypatch.setattr(_config, "HOT_SETTINGS_FILE", str(hot), raising=False)
    hot.write_text('{"updated": "a", "books": {"mkt-a": {"size": 127, "cap_fills": 1}}}')
    m.maybe_apply_hot_settings()
    assert m.sizes["mkt-a"] == 127 and m.cap_fills_by_slug["mkt-a"] == 1, (
        "file A is legal and must apply")
    hot.write_text('{"updated": "b", "books": {"mkt-a": {"cap_fills": 2}}}')
    m.maybe_apply_hot_settings()
    assert m.cap_fills_by_slug["mkt-a"] == 1 and m.sizes["mkt-a"] == 127, (
        "file B read against the frozen launch size looks like a lowering; against the "
        "running 127 it is reach 381 on a guard sized at 280 — it must be ignored whole")


class _VenueClient(FakeClient):
    """FakeClient plus a positions endpoint, so the teardown flatten's mandatory fresh read can
    succeed. `rows` is {slug: qty-string}; set `fail=True` to make the read raise."""

    def __init__(self, rows: dict[str, str], fail: bool = False) -> None:
        super().__init__()
        self.rows, self.fail = rows, fail
        self._sdk = types.SimpleNamespace(
            portfolio=types.SimpleNamespace(positions=self._positions))

    async def _positions(self, params):
        if self.fail:
            raise RuntimeError("positions endpoint down")
        return {"positions": {s: {"netPosition": q, "netPositionDecimal": q} for s, q in self.rows.items()},
                "nextCursor": "", "eof": True}


def test_the_teardown_flatten_reads_the_venue_FRESH_before_deciding_to_trade():
    """⛔ The gate read a row that teardown never refreshed — up to 60s of
    scheduled cadence plus the 4×20s reconcile-pending sleep ahead of it, so 60–140s old. A gate
    that decides whether to TRADE on evidence that stale would decline to flatten inventory we
    know about with perfect confidence."""
    client = _VenueClient({"mkt-a": "3"})
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(3)
    m.venue_inventory = {}                                  # nothing cached: must go read
    asyncio.run(m._teardown_flatten())
    sells = [o for o in client.placed if o["side"] == "sell"]
    assert len(sells) == 1 and sells[0]["size"] == 3, (
        f"the fresh read corroborates 3 long, so the flatten must place it: {sells}")


def test_the_teardown_flatten_REFUSES_when_the_venue_read_FAILS():
    """The other direction: an unreadable venue at teardown reports rather than trades. Costly
    (the operator closes by hand) but it is the direction that cannot grow a position."""
    client = _VenueClient({}, fail=True)
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(3)
    out = asyncio.run(m._teardown_flatten())
    assert "NOT FLATTENED" in out and "unreadable at teardown" in out
    assert not client.placed


def test_the_flatten_clamp_takes_the_SMALLER_number_in_both_directions():
    """The min() half was unpinned: `if True: qty = venue_qty` — venue SIZES the order, the rule
    the field's docstring forbids in bold — survived every test, because no test had
    |venue| > |belief| with the same sign. Both directions now: the flatten never places more
    than EITHER number."""
    client = _VenueClient({"mkt-a": "12"})                  # venue larger, same sign
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(5)                       # belief smaller
    asyncio.run(m._teardown_flatten())
    sells = [o for o in client.placed if o["side"] == "sell"]
    assert len(sells) == 1 and sells[0]["size"] == 5, (
        f"belief 5 / venue 12 must place 5 — the venue may block or shrink, never size up: "
        f"{sells}")


def test_the_unsafe_slugs_are_not_double_listed_as_also_holding():
    """`left` re-listed every unsafe slug under 'Other
    residual', which an operator could read as twice the position."""
    client = _VenueClient({"mkt-a": "-24", "mkt-b": "7"})
    m = _maker(client, slugs=["mkt-a", "mkt-b"], size=12, cap_fills=1, shadow=False,
               real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(12)                      # opposite sign → unsafe
    m.inventory["mkt-b"] = Decimal(7)                       # corroborated → flattens
    out = asyncio.run(m._teardown_flatten())
    assert "mkt-a" in out and "NOT FLATTENED" in out
    assert out.count("mkt-a") == 1, f"the unsafe slug must be listed once, not re-listed: {out}"


def test_a_junk_FOREIGN_row_does_not_stop_the_flatten_on_our_books():
    """⛔ One unparseable row on a market this run never
    touches failed the WHOLE read, and the flatten refused every book ('venue unreadable') while
    the venue had read our slug perfectly. Freshness is per-slug now."""
    class _MixedClient(_VenueClient):
        async def _positions(self, params):
            return {"positions": {"mkt-a": {"netPosition": "3", "netPositionDecimal": "3"},
                                  "foreign-expired-market": {"qtyAvailable": "5"}},
                    "nextCursor": "", "eof": True}

    client = _MixedClient({})
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(3)
    out = asyncio.run(m._teardown_flatten())
    sells = [o for o in client.placed if o["side"] == "sell"]
    assert len(sells) == 1 and sells[0]["size"] == 3, (
        f"our corroborated 3-long must flatten despite the junk foreign row: {sells} / {out}")
    assert m.venue_stale_rows == {"foreign-expired-market"}, (
        "the junk row is flagged per-slug, not fatal per-read")


def test_the_teardown_flatten_REFUSES_when_the_venue_says_FLAT():
    """A venue row of zero is 'the venue does not think we hold this' — it must REPORT, not
    trade, and it must not fall through to the sub-contract branch, which used to print
    'residual 0' over a real −12 after the qty rebind."""
    client = _VenueClient({"mkt-a": "0"})
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(-12)
    out = asyncio.run(m._teardown_flatten())
    assert "NOT FLATTENED" in out and "venue says flat" in out
    assert not client.placed, "nothing may be placed against a venue row of zero"
    assert "residual 0" not in out


def test_a_halt_on_a_FAILED_re_read_says_so_instead_of_claiming_confirmation():
    """⛔ The refresh returned None on success AND
    failure, so a 503 produced a halt asserting '(confirmed by a fresh read)' over two failed
    reads — a false confirmation that feeds an operator hand-flatten of a position that may not
    exist."""
    client = _VenueClient({}, fail=True)
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.venue_inventory = {"mkt-a": Decimal(-30)}          # stale row, beyond the bound
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "RE-READ FAILED" in (m.halt_reason or ""), m.halt_reason
    assert "confirmed by a fresh read" not in (m.halt_reason or "")


def test_the_teardown_flatten_REFUSES_when_the_venue_contradicts_belief():
    """⛔ The worst defect this file pins: the flatten sized and SIGNED off
    belief, so after a venue halt whose own message says 'we do not know our position' it placed
    a post-only SELL 12 against a venue −24, taking a 24-short to 36. The one path that can send
    an order in the direction that grows a position nothing is measuring."""
    client = _VenueClient({"mkt-a": "-24"})             # the FRESH read says short
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(12)                  # belief: long
    out = asyncio.run(m._teardown_flatten())
    assert "NOT FLATTENED" in out and "does not corroborate" in out
    assert not any(o["side"] == "sell" for o in client.placed), (
        f"a sell here grows the venue's 24-short to 36: {client.placed}")


def test_the_teardown_flatten_SIZES_to_the_smaller_of_belief_and_venue():
    """Where the two agree in sign the flatten may trade — but never for more than the venue
    says we hold, or it opens a position in the opposite direction."""
    client = _VenueClient({"mkt-a": "5"})               # the fresh read says only 5
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True, flatten_wait_s=0.0)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(12)                  # belief says 12 long
    asyncio.run(m._teardown_flatten())
    sells = [o for o in client.placed if o["side"] == "sell"]
    assert len(sells) == 1 and sells[0]["size"] == 5, (
        f"must flatten the venue's 5, not belief's 12: {sells}")


def test_a_breach_that_a_FRESH_read_clears_does_not_halt_the_run():
    """Operator's design call: the venue row can be a minute old, so the likeliest cause of a
    contradiction is a position we have since closed. Re-read before halting — a healthy run
    stopped on stale evidence costs earnings and hands back a manual reconcile for nothing."""
    class _Client(FakeClient):
        def __init__(self):
            super().__init__()
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=self._positions))

        async def _positions(self, params):
            return {"positions": {"mkt-a": {"netPosition": "0", "netPositionDecimal": "0"}}, "eof": True}   # since closed

    client = _Client()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.venue_inventory = {"mkt-a": Decimal(-30)}          # stale row, would breach
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is False, "a breach the fresh read clears must not halt"
    assert m.venue_inventory == {"mkt-a": Decimal(0)}, "and the fresh answer replaces the stale"
    assert client.placed, "the run carries on quoting"


def test_a_breach_the_fresh_read_CONFIRMS_still_halts():
    """The other half: a contradiction that survives a re-read means belief is corrupt — it is
    the corruptible one — and stopping is correct."""
    class _Client(FakeClient):
        def __init__(self):
            super().__init__()
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=self._positions))

        async def _positions(self, params):
            return {"positions": {"mkt-a": {"netPosition": "-30", "netPositionDecimal": "-30"}}, "eof": True}

    m = _maker(_Client(), size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.venue_inventory = {"mkt-a": Decimal(-30)}
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "confirmed by a fresh read" in (m.halt_reason or "")


def test_the_venue_breach_is_checked_even_when_the_book_cannot_be_read():
    """Divergence correlates with the venue stress that makes book reads fail, and an unreadable
    book still holds whatever it holds — so the breach check cannot live behind the quote path's
    early returns."""
    client = FakeClient()
    client.book_read_fails = True
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    m.ticks["mkt-a"] = Decimal("0.01")
    m.venue_inventory = {"mkt-a": Decimal(-30)}
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is True and "VENUE INVENTORY" in (m.halt_reason or "")


def test_the_venue_may_only_RESTRICT_never_widen():
    """The direction that matters: a venue read showing us flat must not re-open a side the
    BELIEF has capped. Belief sizes; the venue subtracts."""
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(12)                      # at cap per belief → ask-only
    m.venue_inventory = {"mkt-a": Decimal(0)}               # venue disagrees, says flat
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    sides = {o["side"] for o in client.placed}
    assert sides == {"sell"}, f"the belief cap must still bind, got {sides}"


def test_a_venue_position_beyond_cap_plus_overshoot_halts_the_run():
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.venue_inventory = {"mkt-a": Decimal(-36)}             # the real number from the incident
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "VENUE INVENTORY" in (m.halt_reason or "")
    assert not m.resting, "a halted cycle pulls its quotes"


def test_agreeing_signs_still_leave_the_exit_working():
    """The half of the original finding that survives: when the two AGREE in sign, the AND must not
    pull both sides — a real short at cap keeps its buy-back working."""
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = Decimal(-12)                 # at cap: the BID is the exit
    m.venue_inventory = {"mkt-a": Decimal(-12)}         # venue agrees
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    sides = {o["side"] for o in client.placed}
    assert sides == {"buy"}, (
        f"a capped short must keep quoting its buy-back, got {sides} — pulling both sides parks "
        f"the position with nothing working to take it off")


def test_the_venue_read_PARSES_a_recorded_payload_and_announces_it(caplog):
    """⛔ Every test assigned venue_inventory by hand, so the entire parse
    could be mutated to `{}` — the rail inert — with the whole suite still green. This drives the
    real reader against a recorded envelope shape and asserts it announces, so cycle 1 of a live
    run tells the operator whether the rail exists."""
    import logging

    pages = [{"positions": {"mkt-a": {"netPosition": "-24", "netPositionDecimal": "-24"},
                            "mkt-b": {"netPosition": "0", "netPositionDecimal": "0"}},
              "nextCursor": "", "eof": True}]

    class _Client(FakeClient):
        def __init__(self):
            super().__init__()
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=self._positions))

        async def _positions(self, params):
            return pages[0]

    m = _maker(_Client(), size=12, cap_fills=1, shadow=False, real=True)
    # ⛔ Capture at WARNING, the level the run log actually keeps: `bot.poly_us.maker` has no
    # handler of its own and inherits root's WARNING, so the first version of this announce was
    # `log.info` — dropped entirely in production and visible only because this test forced the
    # level to INFO. Forcing the level tested the test, not the rail.
    with caplog.at_level(logging.WARNING):
        asyncio.run(m._refresh_venue_inventory())
    assert m.venue_inventory == {"mkt-a": Decimal(-24), "mkt-b": Decimal(0)}
    assert m.venue_inventory_ts > 0
    assert any("venue inventory" in r.message for r in caplog.records), (
        "a rail that logs only on failure is indistinguishable from one that never ran — and at "
        "INFO this announce never reaches the run log at all")


def test_a_junk_position_row_keeps_the_previous_answer_rather_than_reading_flat():
    """No `qtyAvailable` fallback and no silent row-drop: the SIGN decides which side is blocked,
    so an unverified fallback could apply the restriction BACKWARDS, and a dropped row restores
    the blindness. Cannot-verify keeps the last known answer."""
    class _Client(FakeClient):
        def __init__(self):
            super().__init__()
            self._sdk = types.SimpleNamespace(
                portfolio=types.SimpleNamespace(positions=self._positions))

        async def _positions(self, params):
            return {"positions": {"mkt-a": {"qtyAvailable": "-24"}}, "eof": True}

    m = _maker(_Client(), size=12, cap_fills=1, shadow=False, real=True)
    m.venue_inventory = {"mkt-a": Decimal(-12)}
    asyncio.run(m._refresh_venue_inventory())
    assert m.venue_inventory == {"mkt-a": Decimal(-12)}, (
        "a row without netPosition must not be read via an unverified field, nor dropped")


def test_a_venue_halt_on_the_LAST_book_still_pulls_every_quote():
    """⛔ The halt's own test was VACUOUS — the breach fired before
    anything was placed on the only book, so `assert not m.resting` was true before the cycle
    began, and both the early `return` and the post-market check survived mutation. Two books
    with the breach on the SECOND makes the assertion real: mkt-a rests quotes, mkt-b breaches,
    and the halt must cancel mkt-a's."""
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], size=12, cap_fills=1, shadow=False, real=True)
    asyncio.run(m.prepare())
    m.venue_inventory = {"mkt-b": Decimal(-36)}          # breach on the LAST book quoted
    m.venue_inventory_ts = time.time()
    asyncio.run(m.run_cycle())
    assert m.should_stop is True and "VENUE INVENTORY" in (m.halt_reason or "")
    assert not m.resting, (
        f"mkt-a's quotes must be pulled by the halt, got {list(m.resting)} — a breach on the "
        f"last book used to exit the loop normally, cancelling nothing")
    assert not any(o["token"] == "mkt-b" for o in client.placed), (
        "the breaching book must not be quoted on its way out")


def test_an_unknown_book_is_not_restricted_and_a_failed_read_keeps_the_last_answer():
    client = FakeClient()
    m = _maker(client, size=12, cap_fills=1, shadow=False, real=True)
    assert m._venue_restriction("never-seen", 12) == (True, True), (
        "no venue row for a book must not silently block it")
    m.venue_inventory = {"mkt-a": Decimal(-24)}
    m.venue_inventory_ts = 1.0

    async def _boom():
        raise RuntimeError("venue down")
    client._sdk = types.SimpleNamespace(portfolio=types.SimpleNamespace(
        positions=lambda *a, **k: _boom()))
    asyncio.run(m._refresh_venue_inventory())
    assert m.venue_inventory == {"mkt-a": Decimal(-24)}, (
        "a failed read must KEEP the restriction — clearing it restores the exact blindness "
        "this exists to remove")


def test_parse_sizes_uniform_and_per_book():
    slugs = ["mkt-a", "mkt-b"]
    assert maker.parse_sizes("12", slugs) == {"mkt-a": 12, "mkt-b": 12}
    assert maker.parse_sizes("mkt-a:25,mkt-b:12", slugs) == {"mkt-a": 25, "mkt-b": 12}


def test_parse_sizes_REFUSES_an_unnamed_book_rather_than_defaulting():
    """The whole hazard of per-book sizing: a forgotten book quoted at some fallback the operator
    never chose, on the axis that scales BOTH the rebate and the adverse tail."""
    with pytest.raises(ValueError, match="no size for"):
        maker.parse_sizes("mkt-a:25", ["mkt-a", "mkt-b"])
    with pytest.raises(ValueError, match="not in --slugs"):
        maker.parse_sizes("mkt-a:25,ghost:12", ["mkt-a"])
    with pytest.raises(ValueError, match="not an integer"):
        maker.parse_sizes("mkt-a:big", ["mkt-a"])


def test_parse_sizes_refuses_a_duplicate_slug_and_tolerates_whitespace():
    """Review N1/N2: last-wins on a duplicate was the one spec shape that yielded a wrong size
    with no error and no trace; and `a : 25` used to refuse by claiming the slug was MISSING,
    which is a confusing message to hit at a prelaunch."""
    with pytest.raises(ValueError, match="twice"):
        maker.parse_sizes("mkt-a:25,mkt-a:12,mkt-b:12", ["mkt-a", "mkt-b"])
    assert maker.parse_sizes(" mkt-a : 25 , mkt-b : 12 ", ["mkt-a", "mkt-b"]) == {
        "mkt-a": 25, "mkt-b": 12}


def test_each_book_quotes_and_caps_at_ITS_OWN_size():
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], size="mkt-a:25,mkt-b:12", shadow=False,
               cap_fills=1)                      # the production config
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    by_book = {}
    for o in client.placed:
        by_book.setdefault(o["token"], set()).add(o["size"])
    assert by_book == {"mkt-a": {25}, "mkt-b": {12}}, f"got {by_book}"
    assert m.caps == {"mkt-a": 25, "mkt-b": 12}, "cap follows the book's own size × cap_fills"
    assert m.max_total_contracts == 37, "the default global cap sums the PER-BOOK caps"
    assert m.cap == 25 and m.size == 25, (
        "the scalar convenience fields are the MAX across books — any stray use over-states an "
        "exposure bound rather than under-stating it")


def test_the_SMALL_books_cap_binds_at_its_own_size_not_the_run_max():
    """The dangerous direction of per-book sizing: if the cap check reads the run's MAX, the
    small book keeps adding past its own limit — 25 contracts of exposure on a book the operator
    sized at 12. Inventory exactly at mkt-b's cap must make it reduce-only while mkt-a, still
    inside its larger cap, quotes both sides."""
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], size="mkt-a:25,mkt-b:12", shadow=False,
               cap_fills=1)
    asyncio.run(m.prepare())
    m.inventory["mkt-b"] = Decimal(12)           # at ITS cap, under the run max of 25
    m.inventory["mkt-a"] = Decimal(12)           # inside ITS cap
    # A healthy basis near the touch — injected inventory with NO basis now (correctly)
    # arms the mark tripwire's inherited-position rule, which is not what this test pins.
    m.avg_entry["mkt-a"] = Decimal("0.45")
    m.avg_entry["mkt-b"] = Decimal("0.45")
    asyncio.run(m.run_cycle())
    sides_b = {o["side"] for o in client.placed if o["token"] == "mkt-b"}
    sides_a = {o["side"] for o in client.placed if o["token"] == "mkt-a"}
    assert sides_b == {"sell"}, (
        f"mkt-b is AT its cap of 12 and must quote only its reducer, got {sides_b}")
    assert sides_a == {"buy", "sell"}, (
        f"mkt-a is inside its cap of 25 and must still quote both sides, got {sides_a}")


def test_a_per_book_size_below_the_floor_refuses_naming_the_book():
    with pytest.raises(ValueError, match="mkt-b"):
        _maker(FakeClient(), slugs=["mkt-a", "mkt-b"], size="mkt-a:25,mkt-b:2")


def test_the_cooldown_reducer_uses_the_BOOKS_size_not_the_run_max():
    """The reducer is sized to inventory but bounded by the book's quote size; reading the run's
    max would let a small book's exit exceed the position it is reducing."""
    client = FakeClient()
    m = _maker(client, slugs=["mkt-a", "mkt-b"], size="mkt-a:25,mkt-b:12", shadow=False)
    asyncio.run(m.prepare())
    m.inventory["mkt-b"] = Decimal(-30)          # beyond mkt-b's own size
    m.cooldown_until["mkt-b"] = time.time() + 60.0
    asyncio.run(m.run_cycle())
    bids_b = [o for o in client.placed if o["token"] == "mkt-b" and o["side"] == "buy"]
    assert bids_b and bids_b[-1]["size"] == 12, (
        f"mkt-b's reducer must cap at ITS size 12, not the run max 25: {bids_b[-1]}")


def test_a_zero_cap_disables_the_halt_but_still_records(tmp_path):
    m, store = _maker_with_state(tmp_path, loss_cap=Decimal("0"))
    _drive_fill(m, "ask", "0.100", 5, "OID-LZ1")
    _drive_fill(m, "bid", "0.900", 5, "OID-LZ2")     # −$4.00, way past any cap
    assert m.should_stop is False, "0 disables the halt"
    assert store.snapshot().realized_pnl == Decimal("-4.00"), "…but the ledger still records"


def test_the_trigger_threshold_is_two_cents_per_contract_bracketed_tightly():
    """The constant itself could be mutated three different ways without a red test. −2.0¢
    arms (boundary inclusive), −1.9¢ does not — any silent widen or narrow goes RED."""
    m = _maker(FakeClient())
    _drive_fill(m, "ask", "0.500", 5, "OID-T1")
    _drive_fill(m, "bid", "0.520", 5, "OID-T2")
    assert m.cooldown_until.get("mkt-a", 0.0) > 0.0, "−2.0¢/contract is the inclusive boundary"
    m2 = _maker(FakeClient())
    _drive_fill(m2, "ask", "0.500", 5, "OID-T3")
    _drive_fill(m2, "bid", "0.519", 5, "OID-T4")
    assert m2.cooldown_until.get("mkt-a", 0.0) == 0.0, "−1.9¢/contract must not trigger"


# ── the durable record closes only on the VENUE's sweep confirmation ─────────────────────────
# Poly copy of test_maker_ops_rails: the Kalshi maker clears its crash record only when
# `sweep_venue_strays` returns True on the venue's own evidence. Until these tests, the Poly
# teardown called `end_run` unconditionally and left every order record in place (parked
# verifies outlive the ~80s teardown window), so opswatch reported "N orders may be RESTING"
# after EVERY clean run — a false alarm that trains the operator to ignore the real one.


def _collapse_teardown_sleeps(monkeypatch):
    """Teardown legitimately spends 4x20s on parked verifies; the test collapses only the gaps."""
    real_sleep = asyncio.sleep
    async def _fast(_s):
        await real_sleep(0)
    monkeypatch.setattr(maker.asyncio, "sleep", _fast)


def test_a_completed_run_leaves_no_maybe_live_orders_in_the_durable_state(tmp_path, monkeypatch):
    """Every order the venue confirms gone must be forgotten, or opswatch cries wolf on every
    clean exit (and the one real crash reads as more of the same noise)."""
    _collapse_teardown_sleeps(monkeypatch)
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert store.snapshot().orders, "precondition: the run must have recorded order intents"
    asyncio.run(m.teardown())
    from bot.core import maker_state
    state = maker_state.load_state(str(tmp_path / "maker_state.json"))
    assert state.maybe_live_orders == 0, state.orders
    assert state.clean_exit is True


def test_a_completed_clean_run_is_SAFE_TO_START_for_recovery(tmp_path, monkeypatch):
    """The rails compose: what the Poly teardown writes is what recovery reads."""
    _collapse_teardown_sleeps(monkeypatch)
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    asyncio.run(m.teardown())
    from bot.core import maker_state
    plan = maker_state.assess_recovery_from_disk(
        str(tmp_path / "maker_state.json"), venue_orders=[], venue_positions={})
    assert plan.action == "start"
    # "start" alone is vacuous — assess_recovery also says "start" for unclean_exit_but_flat,
    # so without the reason this test stays green with the entire close-record block deleted.
    assert plan.reason == "clean", plan


def test_a_sweep_cancel_the_venue_REFUSES_leaves_the_crash_record_OPEN(tmp_path, monkeypatch):
    """⛔ `cancel_order` returns False by DESIGN (never raises in cleanup paths), so a sweep that
    counts the call as swept records a clean exit over a LIVE order. Kalshi's mutation run proved
    this exact gate unpinned (`if swept_clean:` → `if True:` survived); this is the Poly pin."""
    _collapse_teardown_sleeps(monkeypatch)
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m.client.cancel_fails = True
    asyncio.run(m.teardown())
    from bot.core import maker_state
    state = maker_state.load_state(str(tmp_path / "maker_state.json"))
    assert state.clean_exit is False, "a refused cancel must not record a clean exit"
    assert state.maybe_live_orders > 0, "the order the venue kept must stay on the record"


def test_the_ghost_diagnosis_is_offered_ONLY_when_the_venue_actually_refused_a_cancel(
        tmp_path, monkeypatch, caplog):
    """`not _swept_clean` has THREE causes — refused cancel,
    unattributable order, listing raised — and the terminal-but-listed ("ghost") sentence is
    a correct diagnosis for only the first. Handing it to the cannot-verify case tells the
    operator to resolve an unreadable venue by eyeballing, and the remedy it blesses deletes
    the state file — which resets the lifetime loss ratchet to zero. The confirmation tool
    must be the API dump (same credentials that placed the orders), never the UI alone: a
    wrong-account login also shows nothing."""
    import logging
    _collapse_teardown_sleeps(monkeypatch)

    # cause 1: the venue REFUSED a cancel → the ghost sentence and the API tool both appear
    m, _ = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m.client.cancel_fails = True
    with caplog.at_level(logging.ERROR):
        asyncio.run(m.teardown())
    assert "terminal-but-listed" in caplog.text
    assert "poly_us_orders" in caplog.text, "confirmation must point at the API dump"
    assert "Polymarket UI" not in caplog.text, "the UI is the wrong-account-blind observation"
    caplog.clear()

    # cause 2: refused == 0 but an UNATTRIBUTABLE order (no readable id) held the record
    # open — the ghost sentence must NOT appear. Without this half, widening `if
    # self._sweep_refused:` to `is not None` survives, and that mutant IS the bug — it
    # hands the ghost diagnosis to exactly this population.
    d1 = tmp_path / "unattributable"
    d1.mkdir()
    m1, _ = _maker_with_state(d1)
    asyncio.run(m1.prepare())
    asyncio.run(m1.run_cycle())
    m1.client.open_orders = [{"marketSlug": "mkt-a"}]        # listed, but no id to cancel by
    with caplog.at_level(logging.ERROR):
        asyncio.run(m1.teardown())
    assert "terminal-but-listed" not in caplog.text, (
        "the ghost diagnosis was handed to the unattributable case")
    caplog.clear()

    # cause 3: the LISTING RAISED — the venue was never read; the ghost sentence must NOT
    # appear, and the message must forbid acting on a cannot-verify
    d2 = tmp_path / "unreadable"
    d2.mkdir()
    m2, _ = _maker_with_state(d2)
    asyncio.run(m2.prepare())
    asyncio.run(m2.run_cycle())
    async def _unreadable(slugs=None):
        raise RuntimeError("listing unreadable")
    monkeypatch.setattr(m2.client, "get_open_orders", _unreadable)
    with caplog.at_level(logging.ERROR):
        asyncio.run(m2.teardown())
    assert "terminal-but-listed" not in caplog.text, (
        "the ghost diagnosis was handed to a cannot-verify")
    assert "cannot-verify" in caplog.text


def test_the_heartbeat_exit_must_not_say_clean_while_the_crash_record_stays_open(tmp_path,
                                                                                monkeypatch):
    """`mark_exit` ran unconditionally AFTER the close-record gate, so the two
    shutdown artifacts could disagree: state file says the record is OPEN (clean_exit False,
    next start refuses), heartbeat says "clean" — and Deadman.ok reads "clean" as
    nothing-to-see. The heartbeat must carry the same verdict the record does; any non-"clean"
    status makes Deadman.ok False, so opswatch surfaces it."""
    _collapse_teardown_sleeps(monkeypatch)
    hb = RecordingHeartbeat()
    m, store = _maker_with_state(tmp_path, heartbeat=hb)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    m.client.cancel_fails = True
    asyncio.run(m.teardown())
    from bot.core import maker_state
    assert maker_state.load_state(str(tmp_path / "maker_state.json")).clean_exit is False
    assert len(hb.exits) == 1 and hb.exits[0] != "clean", (
        f"heartbeat said {hb.exits!r} over an OPEN crash record")
    assert hb.exits[0].startswith("record_open"), hb.exits

    # and the counterpart: a venue-confirmed clean run still stamps exactly "clean"
    # (fresh dir — the first half's OPEN record correctly refuses a begin_run on its file)
    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()
    hb2 = RecordingHeartbeat()
    m2, _ = _maker_with_state(clean_dir, heartbeat=hb2)
    asyncio.run(m2.prepare())
    asyncio.run(m2.run_cycle())
    asyncio.run(m2.teardown())
    assert hb2.exits == ["clean"]


def test_a_sweep_that_cannot_READ_the_venue_leaves_the_crash_record_OPEN(tmp_path, monkeypatch):
    """'No open orders' and 'we could not tell' must never be the same answer — the
    get_positions lesson, applied to the flag that tells recovery there is nothing to do."""
    _collapse_teardown_sleeps(monkeypatch)
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    async def _unreadable(slugs=None):
        raise RuntimeError("shape we cannot read — refusing to report 'no open orders'")
    monkeypatch.setattr(m.client, "get_open_orders", _unreadable)
    asyncio.run(m.teardown())
    from bot.core import maker_state
    state = maker_state.load_state(str(tmp_path / "maker_state.json"))
    assert state.clean_exit is False, "an unreadable venue listing must not certify a clean exit"


def test_an_unattributable_resting_order_leaves_the_crash_record_OPEN(tmp_path, monkeypatch):
    """An order with no readable id/slug MIGHT be ours (a create whose response was lost records
    no venue id). The sweep already refuses to cancel it; the record must refuse to close over
    it — same rule as Kalshi's unnameable-stray case."""
    _collapse_teardown_sleeps(monkeypatch)
    m, store = _maker_with_state(tmp_path)
    asyncio.run(m.prepare())
    m.client.open_orders = [{"state": "ORDER_STATE_NEW"}]       # no id, no slug
    asyncio.run(m.teardown())
    from bot.core import maker_state
    state = maker_state.load_state(str(tmp_path / "maker_state.json"))
    assert state.clean_exit is False, "an order we cannot attribute is one we cannot rule out"


# ── WIND-DOWN (--passive-exit-s): exit by ceasing to add, not by crossing the spread ─────────


class TestWinddown:
    """The engine half of the passive exit: cap-0 reduce-only with the FLIP GUARD. The shim
    half (entry conditions, halt guard, teardown-after) is pinned in test_poly_live_mm_e2e."""

    def test_winddown_quotes_only_the_reduce_side_clamped_to_inventory(self):
        """Long 12 at size 88: the bid (adds) must not quote, and the ask must be size 12 —
        a full-size 88 reduce order filling through flat would FLIP to a short of 76 — the
        overshoot-through-flat shape, observed for real. The clamp makes the flip
        unrepresentable."""
        client = FakeClient()
        m = _maker(client, size=88, shadow=False)
        asyncio.run(m.prepare())
        m.inventory["mkt-a"] = Decimal("12")
        # A basis near the touch, so the NO-BASIS tripwire does not arm: that rail's own
        # reduce-only carries its own clamp (maker.py "the reducer is sized to the INVENTORY"),
        # and a fixture routing through it pins the WRONG clamp — the wind-down path must be
        # the only reducer here or the mutant survives behind the tripwire's.
        m.avg_entry["mkt-a"] = Decimal("0.45")
        m.winddown = True
        asyncio.run(m.run_cycle())
        buys = [p for p in client.placed if p["side"] == "buy"]
        sells = [p for p in client.placed if p["side"] == "sell"]
        assert buys == [], "wind-down on a long book must never quote the side that ADDS"
        assert len(sells) == 1, f"expected exactly the reduce-side quote, got {client.placed}"
        assert int(sells[0]["size"]) == 12, (
            f"reduce size {sells[0]['size']} — must clamp to |inventory|, or a fill flips the "
            f"position through flat instead of closing it")

    def test_winddown_flat_book_quotes_nothing(self):
        """cap-0 + flat = neither side (`_reduce_only(0)`): with nothing to reduce, every
        quote ADDS — the exact opposite of an exit."""
        client = FakeClient()
        m = _maker(client, shadow=False)
        asyncio.run(m.prepare())
        m.winddown = True
        asyncio.run(m.run_cycle())
        assert client.placed == [], f"a flat book in wind-down placed {client.placed}"

    def test_winddown_sub_contract_residue_quotes_nothing(self):
        """0 < |inv| < 1: `_reduce_only` would allow a side but no runnable passive close
        exists below one contract (poly_close's own rule) — quote nothing, teardown reports."""
        client = FakeClient()
        m = _maker(client, shadow=False)
        asyncio.run(m.prepare())
        m.inventory["mkt-a"] = Decimal("0.5")
        m.avg_entry["mkt-a"] = Decimal("0.45")     # basis present — the no-basis rail stays out
        m.winddown = True
        asyncio.run(m.run_cycle())
        assert client.placed == [], (
            f"a sub-contract residue must not quote (size would floor to 0): {client.placed}")

    def test_normal_mode_is_unchanged_when_winddown_is_off(self):
        """The control: winddown False on a flat book → both sides quote at full size.
        Without it, a mutant forcing cap-0 unconditionally would pass every test above.
        (Flat, because seeded NON-flat inventory carries no basis and the no-basis tripwire
        — an existing rail, correctly — arms reduce-only in normal mode too.)"""
        client = FakeClient()
        m = _maker(client, size=88, shadow=False)
        asyncio.run(m.prepare())
        asyncio.run(m.run_cycle())
        sides = {p["side"] for p in client.placed}
        assert sides == {"buy", "sell"}, f"normal mode must quote both sides: {client.placed}"
        assert all(int(p["size"]) == 88 for p in client.placed), (
            "normal mode must not inherit the wind-down clamp")


class TestPerBookPull:
    """`--pull-at slug:epoch` — the wind-down treatment scoped to ONE book on a wall clock.
    The lever that makes a book seatable when its own information window opens BEFORE the
    run ends (a temp strike's heating, a game's first pitch). Parser pins live in
    tests/test_poly_live_mm_e2e.py."""

    def test_a_pulled_book_goes_reduce_only_clamped_while_the_slate_is_untouched(self):
        """The whole point: one book's exit must never stand the others down."""
        client = FakeClient()
        m = _maker(client, slugs=["mkt-a", "mkt-b"], size=88, shadow=False)
        asyncio.run(m.prepare())
        m.inventory["mkt-a"] = Decimal("12")
        m.avg_entry["mkt-a"] = Decimal("0.45")     # basis present: the no-basis rail stays out
        m.pull_at["mkt-a"] = time.time() - 1       # past → pulled
        asyncio.run(m.run_cycle())
        a = [p for p in client.placed if p["token"] == "mkt-a"]
        b = [p for p in client.placed if p["token"] == "mkt-b"]
        assert [p["side"] for p in a] == ["sell"], (
            f"the PULLED book must quote only its reducing side: {a}")
        assert int(a[0]["size"]) == 12, "the pulled book's reduce order must clamp to inventory"
        assert {p["side"] for p in b} == {"buy", "sell"} and all(int(p["size"]) == 88 for p in b), (
            f"the UNPULLED book must quote both sides at full size: {b}")

    def test_a_book_before_its_deadline_quotes_normally(self):
        """The control — without it, a mutant that pulls unconditionally passes above."""
        client = FakeClient()
        m = _maker(client, size=88, shadow=False)
        asyncio.run(m.prepare())
        m.pull_at["mkt-a"] = time.time() + 3600    # future → not yet pulled
        asyncio.run(m.run_cycle())
        assert {p["side"] for p in client.placed} == {"buy", "sell"}, (
            f"a book BEFORE its pull deadline must quote normally: {client.placed}")

    def test_the_pull_is_announced_exactly_once(self):
        """An operator watching the log must see the transition; repeating it every 10s for
        hours would train them to ignore the line that matters."""
        client = FakeClient()
        m = _maker(client, shadow=False)
        asyncio.run(m.prepare())
        m.pull_at["mkt-a"] = time.time() - 1
        assert m.book_pulled("mkt-a") is True
        assert m.book_pulled("mkt-a") is True      # still pulled…
        assert m._pull_announced == {"mkt-a"}      # …but announced once

    def test_a_book_with_no_deadline_is_never_pulled(self):
        client = FakeClient()
        m = _maker(client, shadow=False)
        asyncio.run(m.prepare())
        assert m.book_pulled("mkt-a") is False, "absent from pull_at = never pulled"


def test_refresh_venue_inventory_reads_the_EXACT_decimal_field():
    """netPositionDecimal ("-10.6000") is the venue's exact holding;
    netPosition ("-11") is rounded display. The venue-breach halt threshold compares this
    map — a rounded read mis-sizes the breach check by up to half a contract per book, and
    a missing exact field keeps the PREVIOUS belief (cannot-verify), never the rounded one."""
    import types

    class _Client:
        def __init__(self):
            self._sdk = types.SimpleNamespace(portfolio=types.SimpleNamespace(
                positions=self._positions))

        async def _positions(self, params):
            return {"positions": {
                "mkt-a": {"netPosition": "-11", "netPositionDecimal": "-10.6000"},
                "mkt-b": {"netPosition": "7"}},                # no exact field
                "eof": True}

    m = _maker(_Client(), size=12, cap_fills=1, shadow=False, real=True)
    m.venue_inventory = {"mkt-b": Decimal("6.5")}
    asyncio.run(m._refresh_venue_inventory())
    assert m.venue_inventory["mkt-a"] == Decimal("-10.6000"), (
        f"must read the exact field, got {m.venue_inventory.get('mkt-a')}")
    assert m.venue_inventory["mkt-b"] == Decimal("6.5"), (
        "a row without netPositionDecimal keeps the previous belief — the rounded "
        "netPosition is not a substitute")


def test_prepare_records_the_SEEDED_inventory_in_the_durable_record(tmp_path):
    """⛔ The shim seeds accepted carries into engine.inventory BEFORE prepare(); prepare()'s
    begin_run must pass exactly that into the durable record. The {}-mutant at the call site
    passed the ENTIRE suite — a carry-start that records flat over live contracts is the same
    invisible-position class as a dropped fill, and a SIGKILL in the first minutes would turn
    it into an unknown_position refusal loop on relaunch."""
    from bot.core.maker_state import MakerStateStore
    store = MakerStateStore(str(tmp_path / "maker_state.json"))
    m = _maker(FakeClient(), state_store=store, real=True, shadow=False)
    m.inventory["mkt-a"] = Decimal("170")        # the shim's carry seeding, pre-prepare
    m.avg_entry["mkt-a"] = Decimal("0.8474")
    asyncio.run(m.prepare())
    inv = store.snapshot().inventory
    assert inv.get("mkt-a") == Decimal("170"), (
        f"the durable record must carry the seeded view, got {inv} — begin_run was "
        f"handed something other than engine.inventory")
    assert store.snapshot().run_id == m.run_id


def test_a_skipped_cycle_tapes_the_REAL_inventory_not_the_signature_default(tmp_path):
    """⛔ `_skip_market` omitted the
    `inventory=` argument, so every skipped cycle (book_read_failed, no_two_sided_touch…)
    taped inventory=0 regardless of holdings. The session report's carry baseline reads
    the EARLIEST pre-fill quote row — cycle 1, exactly when a book is most likely
    unreadable — so a carried book whose first cycle skipped read as "started flat":
    either un-refusing flat-start fiction or false-refusing a correct --carry."""
    import csv as _csv

    m = _maker(FakeClient())
    m.inventory["mkt-a"] = Decimal("-5")         # a carried short
    stats = maker.CycleStats(cycle=1, started_ts=time.time())
    asyncio.run(m._skip_market(stats, "mkt-a", Decimal("0.01"), None,
                               "book_read_failed", read_ms=None, gap_s=None))
    for handle, _w in m._writers.values():
        handle.flush()
    with open(m._quote_path) as fh:
        rows = list(_csv.DictReader(fh))
    assert rows and rows[-1]["status"] == "book_read_failed"
    assert rows[-1]["inventory"] == "-5", (
        f"skip row taped inventory={rows[-1]['inventory']!r} — the signature default "
        f"resurfaced; a carried book's skipped cycle 1 must never read as flat")


def test_the_fill_tape_stamps_BOTH_SIDES_of_the_touch_not_just_the_mid(tmp_path):
    """⛔ A passive fill's markout is `drift + half_spread` BY CONSTRUCTION, so a tape carrying
    only `mid_at_fill` cannot separate price quality from spread capture — the two components
    can even have OPPOSITE SIGNS and sum to a small positive raw markout, which then reads as
    "good fills" when it is really spread capture paying for adverse drift.

    ⛔ These are NOT the book's half-spread: `last_touch` comes from a book that CONTAINS OUR OWN
    RESTING ORDERS, and on a large fraction of improved cycles the next touch is our own quote
    on both sides. See the field comment in maker.py for the detection rule. Pinned here is only what is
    true: both sides written, from the SAME touch the mid came from, mid exactly their
    midpoint."""
    import csv as _csv
    client = FakeClient()
    m = _maker(client, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id].update(
        cumQuantity=5, state="ORDER_STATE_FILLED",
        commissionNotionalTotalCollected={"value": "-0.01", "currency": "USD"})
    client.open_orders = []
    asyncio.run(m.poll_fills())
    m._close_writers()

    with open(tmp_path / "f.csv") as fh:
        rows = list(_csv.DictReader(fh))
    assert rows, "no fill row written"
    row = rows[-1]
    touch = m.last_touch["mkt-a"]
    assert row["best_bid"] == str(touch[0]), "bid is not the touch the mid came from"
    assert row["best_ask"] == str(touch[1]), "ask is not the touch the mid came from"
    assert Decimal(row["mid_at_fill"]) == (Decimal(row["best_bid"]) + Decimal(row["best_ask"])) / 2, \
        "the mid must remain exactly the midpoint of the two stamped sides"
    # …and the spread the retraction needed is now readable off one row, for EITHER arm
    assert Decimal(row["best_ask"]) - Decimal(row["best_bid"]) > 0


def test_a_LATE_BOOKED_fill_leaves_both_touch_sides_blank(tmp_path):
    """Same rule the mid already follows, for the same reason: a late-booked fill happened up to
    LAG_HORIZON_S before its row ts, so any book near ts describes a market the fill did not
    occur in. Blank ON PURPOSE — do not backfill."""
    import csv as _csv
    client = FakeClient()
    m = _maker(client, shadow=False, fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    body = dict(client.orders[bid.order_id], cumQuantity=7, state="ORDER_STATE_FILLED")
    m._book_fill(bid, body, late_booked=True, booked_via="verify")
    m._close_writers()

    with open(tmp_path / "f.csv") as fh:
        row = list(_csv.DictReader(fh))[-1]
    assert row["mid_at_fill"] == "" and row["best_bid"] == "" and row["best_ask"] == "", \
        "a late-booked row must not carry a touch from the wrong moment"


def test_the_fill_tape_rotate_tag_names_THIS_schema_change(tmp_path):
    """⚠️ The rotate tag names the tape's LAST schema change, and the archive dir already holds a
    `poly_live_mm_fills.pre-runid.csv` from the previous one. Adding columns without bumping the
    tag files two different schema boundaries under one name — the collision path appends an
    epoch so nothing is lost, but `_open_writer`'s own docstring records that a misleading
    rotation name has cost real debugging time.

    Pinned functionally, not by string-matching the source: write a row under an OLD header,
    reopen under the current one, and assert where the old file landed."""
    import csv as _csv
    path = tmp_path / "f.csv"
    old = [c for c in maker._FILL_HDR if c not in ("best_bid", "best_ask")]
    h, w = maker._open_writer(str(path), old, rotate_tag="runid")
    w.writerow(["x"] * len(old))
    h.close()

    client = FakeClient()
    m = _maker(client, shadow=False, fill_csv=str(path))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    bid = m.resting[("mkt-a", "bid")]
    client.orders[bid.order_id].update(cumQuantity=5, state="ORDER_STATE_FILLED")
    client.open_orders = []
    asyncio.run(m.poll_fills())
    m._close_writers()

    rot = tmp_path / "rotated"
    names = sorted(p.name for p in rot.iterdir())
    assert names == ["f.pre-fillsrc.csv"], (
        f"expected the pre-columns file to freeze under the NEW tag, got {names}")
    with open(path) as fh:
        assert next(_csv.reader(fh)) == list(maker._FILL_HDR), "fresh file must carry the new header"


def test_a_fill_is_stamped_with_ITS_OWN_slugs_touch_not_another_books(tmp_path):
    """⛔ THE MUTATION THAT SURVIVED EVERYTHING. Replacing
    `self.last_touch.get(order.slug)` with `next(iter(self.last_touch.values()), None)` in
    `_book_fill` passed the ENTIRE suite, because `_maker` defaults to ONE slug and `FakeClient`
    returned the same book for every slug — so the suite could not tell "this slug's touch" from
    "any slug's touch". `mid_at_fill` never had this pin either; it inherits one here.

    A real run quotes several concurrent books at very different prices (say p≈0.54 and p≈0.84).
    Cross-stamping is a large error on the mid and the spread of every affected row — large
    enough to invert the book ranking these columns exist to inform."""
    import csv as _csv
    client = FakeClient()
    client.books = {
        "mkt-a": {"marketData": {"bids": [{"px": "0.44", "qty": "376"}],
                                 "offers": [{"px": "0.47", "qty": "500"}]}},
        "mkt-b": {"marketData": {"bids": [{"px": "0.82", "qty": "376"}],
                                 "offers": [{"px": "0.85", "qty": "500"}]}},
    }
    m = _maker(client, shadow=False, slugs=["mkt-a", "mkt-b"], fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())

    b = m.resting[("mkt-b", "bid")]
    client.orders[b.order_id].update(cumQuantity=3, state="ORDER_STATE_FILLED")
    client.open_orders = [o for o in client.open_orders
                          if o.get("orderId") != b.order_id] if client.open_orders else []
    asyncio.run(m.poll_fills())
    m._close_writers()

    with open(tmp_path / "f.csv") as fh:
        rows = [r for r in _csv.DictReader(fh) if r["slug"] == "mkt-b"]
    assert rows, "no mkt-b fill row written"
    row = rows[-1]
    assert row["best_bid"] == "0.82" and row["best_ask"] == "0.85", (
        f"mkt-b was stamped with another book's touch: {row['best_bid']}/{row['best_ask']} "
        f"— mkt-a's is 0.44/0.47")
    assert Decimal(row["mid_at_fill"]) == Decimal("0.835")


def test_shadow_disables_the_hard_ram_floor_and_real_keeps_it(tmp_path, monkeypatch):
    """The residency floor killed a SHADOW run outright, at an available-memory level with
    plenty of swap free — during exactly the market event it existed to record. A shadow run
    strands nothing on SIGKILL, so its hard floor is 0; the COMBINED floor (swap-exhaustion
    guard) stays for both lanes, and a REAL/DRY maker keeps the full residency floor."""
    from bot.core import memguard as mg
    seen = {}
    def spy(limits=None, label=""):
        seen[label or "x"] = limits
        return mg.MemStatus("ok", "", rss_mb=50.0, avail_mb=500.0, swap_free_mb=8000.0)
    monkeypatch.setattr(mg, "check", spy)
    sh = _maker(FakeClient(), shadow=True, quote_csv=str(tmp_path / "q.csv"),
                cycle_csv=str(tmp_path / "c.csv"), fill_csv=str(tmp_path / "f.csv"))
    asyncio.run(sh.prepare()); asyncio.run(sh.run_cycle()); sh._close_writers()
    lim = seen["poly_live_mm"]
    assert lim is not None and lim.hard_ram_floor_mb == 0.0, "shadow must drop the residency floor"
    assert lim.min_avail_mb > 0, "the combined swap-exhaustion floor must SURVIVE in shadow"
    seen.clear()
    dry = _maker(FakeClient(), shadow=False, real=False, quote_csv=str(tmp_path / "q2.csv"),
                 cycle_csv=str(tmp_path / "c2.csv"), fill_csv=str(tmp_path / "f2.csv"))
    asyncio.run(dry.prepare()); asyncio.run(dry.run_cycle()); dry._close_writers()
    assert seen["poly_live_mm"].hard_ram_floor_mb == 80.0, (
        "a non-shadow maker must keep the deployed residency floor")


# ── WS book-source seam (_read_book_md) ───────────────────────────────────────────────────────

class _Stats:
    """Minimal CycleStats stand-in — the seam touches .requests and .ws_stale_rereads."""
    def __init__(self):
        self.requests = 0
        self.ws_stale_rereads = 0
        self.ws_capped = 0
        self.ws_down_s = 0.0
        self.ws_outage = 0


class _FakeBookFeed:
    """Minimal PolyUSOrderBookCache stand-in — get_book_md + connected, as the maker reads."""
    def __init__(self, md_by_slug, connected=True):
        self._md = md_by_slug
        self.calls = []
        self.connected = connected
    def get_book_md(self, slug):
        self.calls.append(slug)
        return self._md.get(slug)


def _ws_md(bid="0.44", ask="0.45", age_s=1.0):
    import datetime as _dt
    tt = (_dt.datetime.now(_dt.timezone.utc)
          - _dt.timedelta(seconds=age_s)).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
    return {"marketSlug": "mkt-a", "transactTime": tt,
            "offers": [{"px": {"value": ask}, "qty": "40"}],
            "bids": [{"px": {"value": bid}, "qty": "60"}],
            "stats": {"sharesTraded": "10"}}


def test_read_book_md_rest_is_unchanged_and_hits_fetch():
    client = FakeClient()
    m = _maker(client)   # default book_source="rest"
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert err is None and md is not None
    assert client.fetches == [("mkt-a", True)], "rest path must cache-bust via _fetch_book"


def test_read_book_md_ws_fresh_uses_the_feed_and_skips_rest():
    client = FakeClient()
    feed = _FakeBookFeed({"mkt-a": _ws_md(age_s=2.0)})
    m = _maker(client, book_source="ws", book_feed=feed)
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert err is None and md is not None and feed.calls == ["mkt-a"]
    assert client.fetches == [], "a FRESH WS book must not trigger any REST read"
    from bot.poly_us.client import touch_from_md
    from decimal import Decimal
    bid, ask, _ = touch_from_md(md)
    assert bid == Decimal("0.44") and ask == Decimal("0.45")


def test_read_book_md_ws_STALE_falls_back_to_fresh_rest():
    client = FakeClient()
    feed = _FakeBookFeed({"mkt-a": _ws_md(age_s=999.0)})  # > WS_BOOK_STALE_S (150)
    m = _maker(client, book_source="ws", book_feed=feed)
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert err is None and md is not None
    assert client.fetches == [("mkt-a", True)], "a stale WS book MUST fresh-REST re-read that book"


def test_read_book_md_ws_absent_book_falls_back_to_rest():
    client = FakeClient()
    feed = _FakeBookFeed({})   # feed has no book for the slug yet
    m = _maker(client, book_source="ws", book_feed=feed)
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert err is None and client.fetches == [("mkt-a", True)]


def test_read_book_md_ws_with_no_feed_degrades_to_rest():
    client = FakeClient()
    m = _maker(client, book_source="ws", book_feed=None)   # ws requested, no feed wired
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert err is None and client.fetches == [("mkt-a", True)], "no feed → REST, never stale/empty"


def test_read_book_md_ws_unverifiable_freshness_falls_back_to_rest():
    """A WS book whose transactTime is missing/unparseable is NOT trusted — freshness cannot be
    verified, so the conservative path is the fresh-REST backstop, not the unverified book."""
    client = FakeClient()
    md_no_tt = _ws_md()
    del md_no_tt["transactTime"]
    feed = _FakeBookFeed({"mkt-a": md_no_tt})
    m = _maker(client, book_source="ws", book_feed=feed)
    md, read_ms, err = asyncio.run(m._read_book_md("mkt-a", _Stats()))
    assert client.fetches == [("mkt-a", True)], "unverifiable freshness → REST backstop"


def test_read_book_md_request_accounting_reflects_actual_http():
    """[WS-maker book feed] a WS-FRESH cycle spends NO request; a WS-STALE fallback and a plain
    REST read each spend ONE. stats.requests must count actual HTTP, or the rate budget is blind
    to the saving WS exists to produce."""
    client = FakeClient()
    fresh = _FakeBookFeed({"mkt-a": _ws_md(age_s=2.0)})
    m = _maker(client, book_source="ws", book_feed=fresh)
    st = _Stats(); asyncio.run(m._read_book_md("mkt-a", st))
    assert st.requests == 0, "a fresh WS read must not spend a request"
    stale = _FakeBookFeed({"mkt-a": _ws_md(age_s=999.0)})
    m2 = _maker(client, book_source="ws", book_feed=stale)
    st2 = _Stats(); asyncio.run(m2._read_book_md("mkt-a", st2))
    assert st2.requests == 1, "a stale WS fallback spends the REST re-read"
    m3 = _maker(client)  # rest
    st3 = _Stats(); asyncio.run(m3._read_book_md("mkt-a", st3))
    assert st3.requests == 1, "rest always spends one"


def test_ws_stale_reread_cap_bounds_the_burst_and_then_skips():
    """A PARTIAL feed freeze must not re-read the whole slate in one
    cycle. Up to WS_STALE_REREAD_MAX stale books fresh-REST re-read; beyond it they SKIP
    (ws_stale_capped, no REST, no quote on an unverified book)."""
    from bot.poly_us.maker import WS_STALE_REREAD_MAX
    client = FakeClient()
    stale = _FakeBookFeed({})   # every book absent → every read is a would-be re-read
    m = _maker(client, book_source="ws", book_feed=stale)
    st = _Stats()
    st.ws_stale_rereads = 0
    results = [asyncio.run(m._read_book_md(f"s{i}", st))
               for i in range(WS_STALE_REREAD_MAX + 3)]
    # first MAX re-read via REST; the rest are capped/skipped
    n_rest = sum(1 for _md, _rm, err in results if err is None)
    n_capped = sum(1 for _md, _rm, err in results if err == "ws_stale_capped")
    assert n_rest == WS_STALE_REREAD_MAX, "re-reads must be bounded to the cap"
    assert n_capped == 3, "past the cap, stale books skip — never a whole-slate poll burst"
    assert len(client.fetches) == WS_STALE_REREAD_MAX, "no REST beyond the cap"


def test_ws_fresh_books_do_not_consume_the_reread_cap():
    """The cap counts only STALE re-reads — a fresh WS book spends nothing, so a healthy feed
    never approaches the cap however many books it serves."""
    client = FakeClient()
    feed = _FakeBookFeed({f"s{i}": _ws_md(age_s=1.0) for i in range(20)})
    m = _maker(client, book_source="ws", book_feed=feed)
    st = _Stats(); st.ws_stale_rereads = 0
    for i in range(20):
        _md, _rm, err = asyncio.run(m._read_book_md(f"s{i}", st))
        assert err is None
    assert st.ws_stale_rereads == 0 and client.fetches == []


def test_cycle_tape_carries_ws_stale_rereads(tmp_path):
    """[WS-maker book feed] the per-cycle stale-re-read count is the partial-feed-freeze signal;
    it must ride the cycle tape (0 in the default rest path, the C-a cap reads it live)."""
    import csv as _csv
    assert maker._CYCLE_HDR[-1] == "run_id"
    assert "ws_stale_rereads" in maker._CYCLE_HDR
    client = FakeClient()
    m = _maker(client, cycle_csv=str(tmp_path / "c.csv"))
    asyncio.run(m.prepare()); asyncio.run(m.run_cycle()); m._close_writers()
    rows = list(_csv.DictReader(open(tmp_path / "c.csv")))
    assert rows and rows[-1]["ws_stale_rereads"] == "0"


def test_book_source_ws_with_no_feed_taped_as_rest_end_to_end(tmp_path):
    """[WS-maker book feed] --book-source ws with no feed wired (the config-knob brick) must be a
    SAFE no-op: the seam degrades to REST and the tape stamps book_src 'rest', proving the knob
    can be set today without changing behavior."""
    import csv as _csv
    client = FakeClient()
    m = _maker(client, book_source="ws", book_feed=None, quote_csv=str(tmp_path / "q.csv"))
    asyncio.run(m.prepare()); asyncio.run(m.run_cycle()); m._close_writers()
    rows = list(_csv.DictReader(open(tmp_path / "q.csv")))
    assert rows and rows[0]["book_src"] == "rest", "ws with no feed → rest, taped as rest"
    assert client.fetches, "the read still happened via REST"


# ── WS whole-connection fallback→halt ────────────────────────────────────────────────────────

def test_ws_fallback_slugs_held_books_first_never_pruning_inputs():
    from decimal import Decimal as D
    inv = {"held-z": D("5"), "flat": D("0"), "held-a": D("-2")}
    ticks = {f"b{i:02d}": None for i in range(20)} | {"held-z": None, "held-a": None,
                                                     "flat": None}
    out = maker.ws_fallback_slugs(inv, ticks, cap=6)
    assert out[:2] == ["held-a", "held-z"]          # held first, sorted; flat is NOT held
    assert len(out) == 6
    assert len(ticks) == 23 and len(inv) == 3       # inputs untouched


def test_ws_near_cap_boundary():
    from decimal import Decimal as D
    assert maker.ws_near_cap(D("7"), 10) is True    # 0.7 exactly → near
    assert maker.ws_near_cap(D("6.9"), 10) is False
    assert maker.ws_near_cap(D("999"), None) is False   # no cap → never near


def test_a_SIBLING_lanes_live_loss_halts_this_lane(tmp_path):
    """N lanes must not mean N times the cap. The lifetime axis reads the ACCOUNT ledger live — a sibling
    lane's in-run loss must halt this lane once the account total passes the cap, even though
    this lane's own ledger is clean. Reverting the engine to a snapshot-only read goes RED."""
    from bot.core.maker_state import MakerStateStore
    p = str(tmp_path / "maker_state.json")
    own = MakerStateStore(p, lane="main")
    own.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("3.00"),
                  tickers=["mkt-a"], inventory={})
    sib = MakerStateStore(p, lane="park")
    sib.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("3.00"),
                  tickers=["mkt-z"], inventory={})
    sib.add_realized(Decimal("-3.50"))              # sibling burns past the cap DURING our run
    m = _maker(FakeClient(), state_store=own, real=True, shadow=False,
               loss_cap=Decimal("3.00"))
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "ACCOUNT" in (m.halt_reason or "")
    # the HEALTHY read's scope claim is load-bearing too: inverting
    # the scope ternary to the degraded literal must go RED here, not just in the degraded
    # test — an operator reading "OWN LANE / DEGRADED" over a working ledger clears the cap
    # believing the halt was a read fault.
    assert "Σ all lanes" in (m.halt_reason or "")
    assert "DEGRADED" not in (m.halt_reason or "")


def test_account_ledger_read_failure_falls_back_LOUDLY_and_own_breach_still_halts(
        tmp_path, caplog, monkeypatch):
    """A failed account-ledger read must degrade to the own-lane snapshot AUDIBLY, never
    silently — the silent `except Exception` hid every route into the fallback (an import
    typo would have disabled the account-wide axis forever with no trace). And the fallback
    must never be weaker than the own-lane check: our own breach halts even while the
    account read is failing. Requires `account_loss` as a MODULE-LEVEL name (a lazy in-method
    import is unpatchable and fails at first use, not at startup)."""
    from bot.core.maker_state import MakerStateStore

    def _boom(path=None):
        raise OSError("shared ledger unreadable")

    monkeypatch.setattr(maker, "account_loss", _boom)
    p = str(tmp_path / "maker_state.json")
    own = MakerStateStore(p, lane="main")
    own.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("3.00"),
                  tickers=["mkt-a"], inventory={})
    m = _maker(FakeClient(), state_store=own, real=True, shadow=False,
               loss_cap=Decimal("3.00"))
    asyncio.run(m.prepare())
    with caplog.at_level("ERROR"):
        asyncio.run(m.run_cycle())
    assert m.should_stop is False, "own lane clean — the designed fallback keeps quoting"
    assert any("account-ledger" in r.message for r in caplog.records), (
        "the fallback must be LOUD — a silent degrade is the bug this test pins")
    # ...but LOUD means ON TRANSITION, not per cycle: repeats are stderr noise today (this
    # module's logger has NO push handler) and become a flood only if one is ever attached.
    first_count = sum(1 for r in caplog.records if "account-ledger" in r.message)
    with caplog.at_level("ERROR"):
        asyncio.run(m.run_cycle())
    assert sum(1 for r in caplog.records
               if "account-ledger" in r.message) == first_count, (
        "second failing cycle must not re-log — the degrade latch is the pin")
    # recovery re-arms the account axis and says so ONCE — and ONLY on a real transition
    # (`if True:` on the recovery branch survived the old assert, i.e. every healthy cycle
    # would log RECOVERED — the same flood shape on the other axis)
    monkeypatch.setattr(maker, "account_loss", lambda path=None: Decimal("0"))
    with caplog.at_level("WARNING"):
        asyncio.run(m.run_cycle())
    recovered = sum(1 for r in caplog.records if "RECOVERED" in r.message)
    assert recovered == 1
    assert m._account_read_degraded is False
    with caplog.at_level("WARNING"):
        asyncio.run(m.run_cycle())     # second HEALTHY cycle: no transition, no repeat
    assert sum(1 for r in caplog.records if "RECOVERED" in r.message) == 1, (
        "RECOVERED must fire on the fail→ok transition only, never on steady healthy cycles")
    # own-lane breach still halts while the account read is failing (never weaker)
    monkeypatch.setattr(maker, "account_loss", _boom)
    own.add_realized(Decimal("-3.50"))
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "lifetime" in (m.halt_reason or "")
    # the halt line must not claim an account-wide sum it never read
    assert "OWN LANE" in (m.halt_reason or "")
    assert "Σ all lanes" not in (m.halt_reason or "")


def test_a_presend_refusal_clears_the_durable_intent_but_a_generic_error_keeps_it(tmp_path):
    """`PreSendRefusal` is raised BEFORE any venue call, so nothing rested —
    the durable intent is a phantom that would read as `maybe_live_orders` and block a
    sibling lane's start. A GENERIC exception keeps the intent (the order may rest); only
    the typed refusal clears."""
    from bot.core.maker_state import MakerStateStore
    from bot.poly_us.client import PreSendRefusal

    class RefusingClient(FakeClient):
        exc: Exception = PreSendRefusal("bad price")

        async def place_limit_gtc(self, *a, **kw):
            raise self.exc

    p = str(tmp_path / "maker_state.json")
    store = MakerStateStore(p, lane="main")
    store.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("3.00"),
                    tickers=["mkt-a"], inventory={})
    m = _maker(RefusingClient(), state_store=store, real=True, shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m._place("mkt-a", "bid", Decimal("0.44"), improved=False,
                         queue_ahead=Decimal("0")))
    assert store.snapshot().maybe_live_orders == 0, (
        "a KNOWN-unplaced order left a phantom intent")
    RefusingClient.exc = RuntimeError("socket died mid-send")
    asyncio.run(m._place("mkt-a", "bid", Decimal("0.44"), improved=False,
                         queue_ahead=Decimal("0")))
    assert store.snapshot().maybe_live_orders == 1, (
        "an UNKNOWN-outcome order must keep its intent for the teardown sweep")


def test_maker_hands_the_client_a_Decimal_price_not_a_float(tmp_path):
    """[Decimal boundary] The maker computes prices in exact Decimal; casting to float at
    `place_limit_gtc` launders them through binary at the last step before the wire. The
    client accepts Decimal natively — the cast must be gone."""
    m = _maker(FakeClient(), shadow=False)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert m.client.placed, "cycle should have quoted against the default book"
    for order in m.client.placed:
        assert isinstance(order["price"], Decimal), (
            f"price left the maker as {type(order['price']).__name__} — "
            f"the float cast is back")


# ── WS whole-connection guard wiring in run_cycle (was unpinned) ─────────────────────────────

def test_ws_disconnect_enters_fallback_after_streak_then_halts_at_window_expiry(monkeypatch):
    """Disconnect must persist TWO cycles to enter fallback (a reconnect blip must not cost
    the slate's queue); then reduced-set REST (taped ws_outage), and the
    window's expiry HALTS. Commenting the guard out goes RED here."""
    feed = _FakeBookFeed({}, connected=False)
    m = _maker(FakeClient(), book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    assert m._ws_down_since is None, "one disconnected cycle is a blip, not an outage"
    asyncio.run(m.run_cycle())
    assert m._ws_down_since is not None, "two consecutive → fallback window"
    assert m.should_stop is False
    assert "ws_outage" in set(m.last_book_src.values())
    # expire the window → next cycle halts before quoting
    m._ws_down_since -= (maker.WS_FALLBACK_WINDOW_S + 1.0)
    asyncio.run(m.run_cycle())
    assert m.should_stop is True
    assert "fallback window expired" in (m.halt_reason or "")


def test_ws_disconnect_near_cap_halts_on_the_FIRST_dark_cycle():
    """Near the cap, ONE dark cycle halts — the blip tolerance does
    not apply at the risk limit (a 150s-old cache near cap is the forbidden exposure)."""
    from decimal import Decimal as D
    feed = _FakeBookFeed({}, connected=False)
    m = _maker(FakeClient(), book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    m.inventory["mkt-a"] = D(str(m.max_total_contracts))   # gross >= 0.7 * cap
    asyncio.run(m.run_cycle())
    assert m.should_stop is True and "near cap" in (m.halt_reason or "")


def test_ws_near_cap_does_NOT_preempt_true_recovery():
    """The recovery cycle — connected, previous cycle 100%%
    WS-fresh — must RECOVER even at near-cap exposure, not halt blaming a 'dark' feed that
    is provably healthy. De-hoisting/re-hoisting the check wrongly goes RED here."""
    from decimal import Decimal as D
    feed = _FakeBookFeed({}, connected=True)
    slugs = [f"mkt-{c}" for c in "abcdef"]
    m = _maker(FakeClient(), slugs=slugs, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                   # all stale → escalate (below cap: no halt)
    assert m._ws_down_since is not None
    # feed comes fully back; a fill during the episode pushes us near the cap:
    for slug in slugs:
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    asyncio.run(m.run_cycle())                   # clean cycle (reduced==full at 6 books)
    m.inventory[slugs[0]] = D(str(m.max_total_contracts))
    asyncio.run(m.run_cycle())                   # probation (full slate) AT near-cap — no halt
    asyncio.run(m.run_cycle())                   # recovery evaluation AT near-cap
    assert m._ws_down_since is None and m.should_stop is False, (
        "a provably-healthy cycle at near-cap must recover, never halt as 'dark'")


def test_ws_probation_reduced_clean_does_not_clear_but_full_clean_does():
    """Slate > WS_FALLBACK_MAX_BOOKS with the
    dark books OUTSIDE the reduced set: a clean REDUCED cycle must NOT clear the window
    (probation only); the window stays monotonic through probation/relapse; deleting the
    `not self._last_cycle_reduced` term from the recovery condition goes RED."""
    healthy = [f"aa-{c}" for c in "abcdefghijkl"]      # 12 — fill the reduced set alone
    dark = [f"zz-{c}" for c in "abcdef"]               # 6 dark, outside it
    feed = _FakeBookFeed({}, connected=True)
    m = _maker(FakeClient(), slugs=healthy + dark, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    for slug in healthy:
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    asyncio.run(m.run_cycle())                         # full slate: dark cap → escalate
    assert m._ws_down_since is not None
    t0 = m._ws_down_since
    asyncio.run(m.run_cycle())                         # reduced (12 healthy) — ALL ws-fresh
    assert m._ws_down_since == t0, (
        "reduced-set evidence must NOT clear the window — probation only")
    asyncio.run(m.run_cycle())                         # probation full slate → dark books cap
    assert m._ws_down_since == t0, "window monotonic through probation relapse"
    # now the dark books come back too → clean FULL cycle → true recovery
    for slug in dark:
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    for _ in range(3):
        asyncio.run(m.run_cycle())
        if m._ws_down_since is None:
            break
    assert m._ws_down_since is None, "a clean FULL-SLATE cycle must recover"
    assert m.ws_feed_deaths == 1


def test_ws_partial_freeze_never_false_recovers_and_the_window_expires(monkeypatch):
    """The oscillation case. A PERSISTENT partial freeze (socket up, books
    dark) must STAY in fallback (no false 'RECOVERED': fallback cycles now try WS first, so
    zero-evidence cycles keep ws_outage > 0) and must HALT when the window expires."""
    feed = _FakeBookFeed({}, connected=True)
    slugs = [f"mkt-{c}" for c in "abcdef"]
    m = _maker(FakeClient(), slugs=slugs, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())                       # capped cycle → escalates
    assert m._ws_down_since is not None
    t0 = m._ws_down_since
    for _ in range(4):
        asyncio.run(m.run_cycle())
        if m.should_stop:
            break
        assert m._ws_down_since is not None, "still-dark books must not false-recover"
        assert m._ws_down_since == t0, "the window must stay MONOTONIC (no restart)"
    m._ws_down_since -= (maker.WS_FALLBACK_WINDOW_S + 1.0)
    asyncio.run(m.run_cycle())
    assert m.should_stop is True and "expired" in (m.halt_reason or "")


def test_ws_partial_freeze_cancels_dropped_books_orders_once(monkeypatch):
    """The dropped-books cancel must fire on the PARTIAL-FREEZE path
    too (per-episode flag, not the entering transition)."""
    feed = _FakeBookFeed({}, connected=True)
    slugs = [f"mkt-{c}" for c in "abcdefghijklmn"]   # 14 books > fallback cap 10
    m = _maker(FakeClient(), slugs=slugs, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    cancelled = []
    async def _cancel(slug, side):
        cancelled.append((slug, side))
        m.resting.pop((slug, side), None)
    m._cancel = _cancel
    dropped = set(slugs) - set(maker.ws_fallback_slugs(m.inventory, m.ticks, sizes=m.sizes))
    assert dropped, "fixture must actually drop books"
    for sl in dropped:      # stubs only on DROPPED books (quoted books' orders need real shape)
        m.resting[(sl, "bid")] = object()
    asyncio.run(m.run_cycle())                       # escalates at cycle end
    asyncio.run(m.run_cycle())                       # fallback cycle → episode cancel fires
    assert {sl for sl, _ in cancelled} == dropped, (
        "every dropped book's resting order is cancelled exactly on the freeze episode")


def test_ws_partial_freeze_escalates_and_recovery_needs_a_clean_cycle():
    """Socket ALIVE but books stale past the C-a cap → escalate into the fallback window
    (the skip-forever gap); a still-capped cycle must NOT clear it, a clean one must."""
    feed = _FakeBookFeed({}, connected=True)      # connected, but serves nothing → all stale
    slugs = [f"mkt-{c}" for c in "abcdef"]        # > WS_STALE_REREAD_MAX so the cap bites
    m = _maker(FakeClient(), slugs=slugs, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    asyncio.run(m.run_cycle())
    # every book missed WS; 4 re-read over REST, the rest capped → escalation armed
    assert m._ws_down_since is not None, "a capped cycle on a live socket must escalate"
    # feed recovers: fresh books → clean REDUCED cycle → PROBATION (full slate, window kept)
    # → clean FULL cycle → recovery (reduced-set evidence alone never clears)
    for slug in list(m.ticks):
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    asyncio.run(m.run_cycle())          # reduced set, all WS-fresh
    assert m._ws_down_since is not None, "reduced-set evidence must NOT clear the window"
    asyncio.run(m.run_cycle())          # probation: full slate, clean
    asyncio.run(m.run_cycle())          # guard sees clean FULL cycle → true recovery
    assert m._ws_down_since is None, "a clean FULL-SLATE cycle must recover"


def test_ws_PARTIAL_freeze_with_healthy_books_still_halts_at_expiry():
    """The cross-episode oscillation. Half the slate served fresh,
    half permanently dark: the episode must stay ONE episode (deaths=1, window monotonic
    across probation attempts) and HALT at expiry — never oscillate forever."""
    healthy = [f"aa-{c}" for c in "abcdefgh"]          # sort first → in the reduced set
    dark = [f"zz-{c}" for c in "abcdef"]
    feed = _FakeBookFeed({}, connected=True)
    m = _maker(FakeClient(), slugs=healthy + dark, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    for slug in healthy:
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    asyncio.run(m.run_cycle())                         # full slate: dark books cap → escalate
    assert m._ws_down_since is not None
    t0 = m._ws_down_since
    for _ in range(5):                                 # fallback/probation churn window
        asyncio.run(m.run_cycle())
        if m.should_stop:
            break
        assert m._ws_down_since is not None and m._ws_down_since == t0, (
            "the episode's window must stay MONOTONIC — no restart across probation")
    assert m.ws_feed_deaths == 1, "one episode, not one per oscillation"
    m._ws_down_since -= (maker.WS_FALLBACK_WINDOW_S + 1.0)
    asyncio.run(m.run_cycle())
    assert m.should_stop is True and "expired" in (m.halt_reason or "")


def test_ws_flapping_connection_eventually_enters_fallback():
    """An alternating (flapping) socket must ACCUMULATE via the 0.5 decay and
    enter a fallback episode — a zeroing reset let a flapping feed disable the guard forever."""
    feed = _FakeBookFeed({}, connected=False)
    m = _maker(FakeClient(), book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    for i in range(12):
        feed.connected = (i % 2 == 1)     # dc, c, dc, c, …
        asyncio.run(m.run_cycle())
        if m._ws_down_since is not None or m.should_stop:
            break
    assert m._ws_down_since is not None or m.should_stop, (
        "sustained flapping must reach the streak threshold (decay, not reset)")


def test_ws_probation_relapse_recancels_orders_placed_on_dark_books():
    """Probation places on still-dark books (REST-priced);
    the relapse must cancel THOSE too — deleting the re-arm leaves them resting unwatched."""
    healthy = [f"aa-{c}" for c in "abcdefghijkl"]
    dark = [f"zz-{c}" for c in "abcdef"]
    feed = _FakeBookFeed({}, connected=True)
    m = _maker(FakeClient(), slugs=healthy + dark, book_source="ws", book_feed=feed)
    asyncio.run(m.prepare())
    for slug in healthy:
        feed._md[slug] = _ws_md(age_s=1.0) | {"marketSlug": slug}
    cancelled = []
    real_cancel = m._cancel
    async def _cancel(slug, side):
        cancelled.append(slug)
        await real_cancel(slug, side)
    m._cancel = _cancel
    asyncio.run(m.run_cycle())        # escalate
    asyncio.run(m.run_cycle())        # fallback (wave 1: nothing resting on dark yet)
    asyncio.run(m.run_cycle())        # probation: PLACES on dark books (REST-priced)
    placed_dark = {s for s, _ in m.resting if s in set(dark)}
    assert placed_dark, "probation must have placed on the dark books"
    asyncio.run(m.run_cycle())        # relapse: re-armed cancel must pull them
    assert placed_dark <= set(cancelled), (
        "orders probation placed on dark books must be cancelled on the relapse")
