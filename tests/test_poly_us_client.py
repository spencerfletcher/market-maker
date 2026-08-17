"""Tests for PolyUSClient — parsing & translation against a mocked SDK."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from bot.poly_us.client import (
    PolyUSClient, PreSendRefusal, _amount_to_float, order_is_filled, order_filled_qty,
    transact_age_s, _parse_book_stats, _EMPTY_BOOK_STATS, _wire_price, _wire_quantity,
    touch_from_md,
)


def test_order_is_filled_true_when_execution_state_filled():
    resp = {"id": "1", "executions": [{"order": {"state": "ORDER_STATE_FILLED"}}]}
    assert order_is_filled(resp) is True


def test_order_filled_qty_reads_cumquantity():
    # Poly coerces FOK→IOC; reconcile to actual filled qty (max cumQuantity).
    assert order_filled_qty(
        {"executions": [{"order": {"cumQuantity": 0}}, {"order": {"cumQuantity": 3}}]}
    ) == 3.0
    assert order_filled_qty({"executions": []}) == 0.0
    assert order_filled_qty(None) == 0.0


def test_order_filled_qty_ignores_the_lying_first_execution():
    """max() across executions is LOAD-BEARING — executions[0] LIES.

    Pinned to a REAL response captured from the venue: we asked for
    tif=FILL_OR_KILL, the exchange echoed IMMEDIATE_OR_CANCEL and PARTIAL-FILLED 255/300 —
    and the FIRST execution's order still carried cumQuantity=0 / leavesQuantity=300, with only
    a LATER execution carrying 255.

    Reading executions[0] returns 0 → "the leg missed" on a leg that filled 255. Under
    poly_first that books no hedge and leaves a naked, UNRECORDED Poly position — invisible to
    the tracker, the exposure caps and the strand alert. Never reduce this to first/last.
    """
    real = {
        "id": "B8G89C9MC75G",
        "executions": [
            {"id": "B8GTKWM4A6AH", "order": {
                "id": "B8G89C9MC75G", "marketSlug": "tec-mls-winner-2026-11-07-dcu",
                "side": "ORDER_SIDE_BUY", "type": "ORDER_TYPE_LIMIT",
                "price": {"value": "0.009", "currency": "USD"},
                "quantity": 300, "cumQuantity": 0, "leavesQuantity": 300,
                "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",   # we SENT FILL_OR_KILL
                "intent": "ORDER_INTENT_BUY_LONG"}},
            {"id": "B8GTKWM4A6AI", "order": {
                "id": "B8G89C9MC75G", "quantity": 300, "cumQuantity": 255,
                "leavesQuantity": 45, "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"}},
        ],
    }
    assert order_filled_qty(real) == 255.0, "must read the REAL fill, not the first execution"
    assert real["executions"][0]["order"]["cumQuantity"] == 0, "the trap this guards"


def test_order_is_filled_false_for_killed_fok():
    # A killed/expired FOK returns a dict but no filled execution.
    assert order_is_filled({"id": "1", "executions": []}) is False
    assert order_is_filled(
        {"id": "1", "executions": [{"order": {"state": "ORDER_STATE_CANCELED"}}]}
    ) is False


def test_order_is_filled_false_for_non_dict():
    assert order_is_filled(None) is False
    assert order_is_filled("oops") is False


def test_amount_to_float_parses_value():
    assert _amount_to_float({"value": "0.6500", "currency": "USD"}) == pytest.approx(0.65)


def test_amount_to_float_none_returns_none():
    assert _amount_to_float(None) is None


class _FakeMarkets:
    def __init__(self, bbo_resp):
        self._bbo_resp = bbo_resp
    async def bbo(self, slug):
        self._last_slug = slug
        return self._bbo_resp


class _FakeAccount:
    def __init__(self, balances_resp):
        self._balances_resp = balances_resp
    async def balances(self):
        return self._balances_resp


@pytest.mark.asyncio
async def test_get_best_ask_parses_marketdata(monkeypatch):
    client = PolyUSClient.__new__(PolyUSClient)  # bypass __init__/auth
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeMarkets(
        {"marketData": {"marketSlug": "x", "bestAsk": {"value": "0.2400", "currency": "USD"}}}
    )
    ask = await client.get_best_ask("x")
    assert ask == pytest.approx(0.24)


class _FakeSettlementMarkets:
    def __init__(self, resp):
        self._resp = resp
    async def settlement(self, slug):
        self._last_slug = slug
        return self._resp


@pytest.mark.asyncio
async def test_get_settlement_parses_numeric_field():
    # Live API returns {"slug":..,"settlement":<number>} (NOT settlementPrice:Amount).
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeSettlementMarkets({"slug": "x", "settlement": 1})
    assert await client.get_settlement("exg-mlb-tor-bos") == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_get_settlement_zero_is_valid_not_missing():
    # A definite long-LOSS settles at 0 (falsy); must NOT read as not-yet-settled.
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeSettlementMarkets({"slug": "x", "settlement": 0})
    assert await client.get_settlement("exg-mlb-tor-bos::short") == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_get_settlement_amount_dict_fallback():
    # Tolerate the SDK-typed settlementPrice:Amount shape too.
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeSettlementMarkets(
        {"settlementPrice": {"value": "0.4300", "currency": "USD"}}
    )
    assert await client.get_settlement("x") == pytest.approx(0.43)


@pytest.mark.asyncio
async def test_get_settlement_missing_returns_none():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeSettlementMarkets({"slug": "x"})   # no settlement field
    assert await client.get_settlement("x") is None


class _FakeBookMarkets:
    def __init__(self, resp):
        self._resp = resp
    async def book(self, slug):
        return self._resp


def _book(state, offers=None, bids=None):
    md = {"state": state}
    if offers is not None:
        md["offers"] = [{"px": {"value": f"{p}", "currency": "USD"}, "qty": "100"} for p in offers]
    if bids is not None:
        md["bids"] = [{"px": {"value": f"{p}", "currency": "USD"}, "qty": "100"} for p in bids]
    return {"marketData": md}


@pytest.mark.asyncio
async def test_get_fill_quote_long_open():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_OPEN", offers=[0.48, 0.49]))
    ask, state, levels, _tx, _stats = await client.get_fill_quote("exg-mlb-tor-bos")
    assert ask == pytest.approx(0.48) and state == "MARKET_STATE_OPEN"
    # long: offers as-is in ask-space, both levels carried (caller sums fillable-at-limit)
    assert levels == [(0.48, 100.0), (0.49, 100.0)]


@pytest.mark.asyncio
async def test_get_fill_quote_short_one_minus_bid():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_OPEN", bids=[0.47, 0.46]))
    ask, state, levels, _tx, _stats = await client.get_fill_quote("exg-mlb-tor-bos::short")
    assert ask == pytest.approx(0.53)   # 1 − best bid 0.47
    # short: bids normalized to ASK space (1−px), so the caller's `p <= poly_limit` is
    # apples-to-apples — this is the one place a space-mismatch would hide.
    assert levels == [(0.53, 100.0), (0.54, 100.0)]


@pytest.mark.asyncio
async def test_get_fill_quote_reports_suspended_state():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_SUSPENDED", offers=[0.38]))
    ask, state, _levels, _tx, _stats = await client.get_fill_quote("exg-fwc-sui-bih")
    assert state == "MARKET_STATE_SUSPENDED"   # caller must refuse this


@pytest.mark.asyncio
async def test_get_fill_quote_failclosed_on_error():
    class _BoomMarkets:
        async def book(self, slug):
            raise RuntimeError("network")
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _BoomMarkets()
    res = await client.get_fill_quote("slug")
    assert res[:4] == (None, "?", [], None)            # fail-closed (4 fire-path values)
    assert all(v is None for v in res[4].values())     # stats all-None, never raises


# ── cache-bust (fresh=) + transactTime ───────────────────────────────────────────────

class _FakeSDK:
    """SDK with both the resource method (markets.book) and the raw .get() the fresh path
    uses; both return the SAME book so the two read paths can be compared structurally."""
    def __init__(self, book):
        self._book = book
        self.markets = _FakeBookMarkets(book)
        self.get_calls = []
    async def get(self, path, *, query=None):
        self.get_calls.append((path, query))
        return self._book


@pytest.mark.asyncio
async def test_get_fill_quote_returns_transact_time():
    book = _book("MARKET_STATE_OPEN", offers=[0.48])
    book["marketData"]["transactTime"] = "2026-06-22T19:30:20.818756170Z"
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(book)
    _ask, _state, _levels, tx, _stats = await client.get_fill_quote("exg-mlb-tor-bos")
    assert tx == "2026-06-22T19:30:20.818756170Z"


# ── marketData.stats → liquidity/activity (logging-only; FREE on the same fetch) ──────────────

def test_parse_book_stats_present_with_ages():
    md = {"stats": {
        "openInterest": "507004.69",
        "openInterestSetTime": "2026-06-23T00:00:00.000000Z",
        "lastTradePx": {"value": "0.8500", "currency": "USD"},
        "lastTradeQty": "9.34",
        "lastTradeSetTime": "2026-06-23T00:00:00.000000Z",
        "sharesTraded": "530166.54",
        "notionalTraded": {"value": "44274039.23", "currency": "USD"},
    }}
    now = datetime(2026, 6, 23, 0, 0, 0, tzinfo=timezone.utc).timestamp() + 5.0
    s = _parse_book_stats(md, now)
    assert s["open_interest"] == pytest.approx(507004.69)
    assert s["last_trade_px"] == pytest.approx(0.85)
    assert s["last_trade_qty"] == pytest.approx(9.34)
    assert s["shares_traded"] == pytest.approx(530166.54)
    assert s["notional_traded"] == pytest.approx(44274039.23)
    assert s["oi_age_s"] == pytest.approx(5.0)            # now − openInterestSetTime
    assert s["last_trade_age_s"] == pytest.approx(5.0)


def test_parse_book_stats_absent_is_all_none_and_never_raises():
    # No / garbled / missing stats → all-None, no raise (logging must never break the fire path).
    assert _parse_book_stats({}, 1000.0) == _EMPTY_BOOK_STATS
    assert _parse_book_stats({"stats": "garbage"}, 1000.0) == _EMPTY_BOOK_STATS
    assert _parse_book_stats(None, 1000.0) == _EMPTY_BOOK_STATS
    # one unparseable field → None for it, the others still parse (independent).
    s = _parse_book_stats({"stats": {"openInterest": "abc", "sharesTraded": "12"}}, 1000.0)
    assert s["open_interest"] is None and s["shares_traded"] == pytest.approx(12.0)


@pytest.mark.asyncio
async def test_get_fill_quote_carries_stats():
    book = _book("MARKET_STATE_OPEN", offers=[0.48])
    book["marketData"]["stats"] = {"openInterest": "777",
                                   "lastTradePx": {"value": "0.48", "currency": "USD"}}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(book)
    _ask, _state, _levels, _tx, stats = await client.get_fill_quote("exg-mlb-tor-bos")
    assert stats["open_interest"] == pytest.approx(777.0)
    assert stats["last_trade_px"] == pytest.approx(0.48)


@pytest.mark.asyncio
async def test_get_fill_quote_fresh_busts_cache_via_nonce_get():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    sdk = client._sdk = _FakeSDK(_book("MARKET_STATE_OPEN", offers=[0.48]))
    await client.get_fill_quote("exg-mlb-tor-bos")            # default → resource method
    assert sdk.get_calls == []                                # no raw .get()
    await client.get_fill_quote("exg-mlb-tor-bos", fresh=True)  # fresh → raw .get() w/ nonce
    assert len(sdk.get_calls) == 1
    path, query = sdk.get_calls[0]
    assert path == "/v1/markets/exg-mlb-tor-bos/book"
    assert "_" in query and query["_"]                        # nonce present, non-empty


@pytest.mark.asyncio
async def test_get_fill_quote_cached_and_fresh_paths_structurally_equivalent():
    """The cache-bust must change ONLY the cache key, never the parsed shape — else the fresh
    fire-path read silently produces different asks/depths (a money-path bug). Same book through
    both branches → identical (ask, state, levels, transact_time)."""
    book = _book_q("MARKET_STATE_OPEN", offers=[(0.48, 40), (0.49, 100)])
    book["marketData"]["transactTime"] = "2026-06-22T19:30:20.818000000Z"
    c1 = PolyUSClient.__new__(PolyUSClient); c1._dry_run = False; c1._sdk = _FakeSDK(book)
    c2 = PolyUSClient.__new__(PolyUSClient); c2._dry_run = False; c2._sdk = _FakeSDK(book)
    cached = await c1.get_fill_quote("exg-mlb-tor-bos")              # markets.book path
    fresh = await c2.get_fill_quote("exg-mlb-tor-bos", fresh=True)   # nonce .get() path
    assert cached == fresh
    assert cached[0] == pytest.approx(0.48) and cached[3] == "2026-06-22T19:30:20.818000000Z"


def test_transact_age_s_parses_ns_iso():
    # ns fraction truncated to µs; age = now − transactTime.
    tx = "2026-06-22T19:30:20.818756170Z"
    base = datetime(2026, 6, 22, 19, 30, 20, 818756, tzinfo=timezone.utc).timestamp()
    assert transact_age_s(tx, base + 10.0) == pytest.approx(10.0, abs=1e-3)


def test_transact_age_s_none_on_missing_or_junk():
    assert transact_age_s(None, 1000.0) is None
    assert transact_age_s("", 1000.0) is None
    assert transact_age_s("not-a-timestamp", 1000.0) is None


# ── would-fire sampler depth comparability ──────────────────────────────────────────
# The sampler (kalshi_arb._sample_book_evolution) switched its REST source from
# get_book_depth → get_fill_quote so one fetch yields the REST ask AND best-level depth.
# rest_poly_depth must stay equal to the old get_book_depth value or the WS-vs-REST depth
# comparison silently changes meaning. SHORT tokens are where a bid-space/ask-space mismatch
# would hide, so this is a PERMANENT guard (not a one-time live eyeball that may skip short).

def _book_q(state, offers=None, bids=None):
    """Like _book but per-level qty: pass (px, qty) tuples."""
    md = {"state": state}
    if offers is not None:
        md["offers"] = [{"px": {"value": f"{p}", "currency": "USD"}, "qty": f"{q}"} for p, q in offers]
    if bids is not None:
        md["bids"] = [{"px": {"value": f"{p}", "currency": "USD"}, "qty": f"{q}"} for p, q in bids]
    return {"marketData": md}


def _sampler_best_level_depth(levels):
    """The exact best-level reduction the sampler runs on get_fill_quote's ask_levels:
    best = min ask price (drawn from the list, so the minimal level satisfies p <= best
    exactly — no fragile float-equality), sum qty at-or-below it."""
    if not levels:
        return None
    best = min(p for p, _ in levels)
    return sum(q for p, q in levels if p <= best)


@pytest.mark.asyncio
async def test_sampler_depth_matches_get_book_depth_long():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(
        _book_q("MARKET_STATE_OPEN", offers=[(0.48, 40), (0.49, 100)])
    )
    old = await client.get_book_depth("exg-mlb-tor-bos")
    _ask, _state, levels, _tx, _stats = await client.get_fill_quote("exg-mlb-tor-bos")
    assert old == _sampler_best_level_depth(levels) == 40.0   # qty at best offer 0.48


@pytest.mark.asyncio
async def test_sampler_depth_matches_get_book_depth_short():
    # Short bid 0.30 (qty 40) is an ASK at 1−0.30=0.70. get_book_depth sums qty at the
    # MAX bid (0.30); the new path sums qty at the MIN ask (0.70) — same physical level,
    # so the same qty. This is the case the comparability claim could be quietly false on.
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(
        _book_q("MARKET_STATE_OPEN", bids=[(0.30, 40), (0.29, 100)])
    )
    old = await client.get_book_depth("exg-mlb-tor-bos::short")
    _ask, _state, levels, _tx, _stats = await client.get_fill_quote("exg-mlb-tor-bos::short")
    assert _ask == pytest.approx(0.70)                       # 1 − best bid 0.30
    assert old == _sampler_best_level_depth(levels) == 40.0  # space-invariant qty


@pytest.mark.asyncio
async def test_get_best_ask_none_when_no_ask(monkeypatch):
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeMarkets({"marketData": {"bestAsk": None}})
    assert await client.get_best_ask("x") is None


@pytest.mark.asyncio
async def test_get_usdc_balance_returns_buying_power():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.account = _FakeAccount(
        {"balances": [{"currentBalance": 200.0, "buyingPower": 187.62, "currency": "USD"}]}
    )
    assert await client.get_usdc_balance() == pytest.approx(187.62)


@pytest.mark.asyncio
async def test_get_usdc_balance_dry_run_returns_sentinel():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = True
    assert await client.get_usdc_balance() == 9999.0


def test_amount_to_float_malformed_returns_none():
    assert _amount_to_float({"value": "not-a-number"}) is None
    assert _amount_to_float({}) is None


@pytest.mark.asyncio
async def test_get_usdc_balance_no_usd_balance_returns_zero():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.account = _FakeAccount({"balances": [{"currency": "EUR", "buyingPower": 50.0}]})
    assert await client.get_usdc_balance() == 0.0


@pytest.mark.asyncio
async def test_get_usdc_balance_error_returns_negative_one():
    class _BoomAccount:
        async def balances(self):
            raise RuntimeError("network down")
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.account = _BoomAccount()
    assert await client.get_usdc_balance() == -1.0


class _FakeOrders:
    def __init__(self, resp):
        self._resp = resp
        self.last_params = None
    async def create(self, params):
        self.last_params = params
        return self._resp


@pytest.mark.asyncio
async def test_place_limit_fok_dry_run_does_not_call_sdk():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = True
    client._sdk = type("S", (), {})()
    client._sdk.orders = _FakeOrders({"order": {"status": "should_not_be_used"}})
    resp = await client.place_limit_fok("slug-x", 0.24, 100, "[t]")
    assert resp["status"] == "dry_run"
    assert client._sdk.orders.last_params is None


@pytest.mark.asyncio
async def test_place_limit_fok_builds_fok_buy_long_order():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _FakeOrders({"order": {"status": "killed"}})
    await client.place_limit_fok("slug-x", 0.24, 100, "[t]")
    p = client._sdk.orders.last_params
    assert p["marketSlug"] == "slug-x"
    assert p["intent"] == "ORDER_INTENT_BUY_LONG"
    assert p["type"] == "ORDER_TYPE_LIMIT"
    # IOC, not FOK: Poly does not honor FOK — it silently rewrites it to IOC, observed on a real
    # order that came back partially filled. We now send what we actually get, so a
    # future Poly FOK rollout cannot silently turn our orders all-or-nothing.
    assert p["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert p["manualOrderIndicator"] == "MANUAL_ORDER_INDICATOR_AUTOMATIC"
    assert p["price"] == {"value": "0.2400", "currency": "USD"}
    assert p["quantity"] == 100


@pytest.mark.asyncio
async def test_place_limit_fok_RAISES_on_error_because_an_error_is_not_an_outcome():
    """It returned None, and that is how "we don't know" became "filled nothing".

    None flowed to order_filled_qty → 0.0 → _exec_poly_first's `P <= 0` branch → "MISSED (no
    fill)", under a comment reading "no exposure". But only timeout / 502 / reset reach here — a
    real IOC kill is a 200 with cumQuantity 0 — and `synchronousExecution` blocks the order ~61ms
    server-side, so a timeout lands INSIDE the fill window. Poly fires FIRST in the deployed
    config; the Kalshi client states this same rule for itself ("a FOK kill is an OUTCOME, NOT AN
    ERROR") and raises.

    Callers must be able to tell "the venue said zero" from "we do not know". Raising is the only
    way to say the second.
    """
    class _BoomOrders:
        async def create(self, params):
            raise RuntimeError("timeout")
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _BoomOrders()
    with pytest.raises(RuntimeError, match="timeout"):
        await client.place_limit_fok("slug-x", 0.24, 100, "[t]")


@pytest.mark.asyncio
async def test_place_limit_fok_still_returns_a_zero_fill_response_for_a_REAL_kill():
    """The other half: when the venue ANSWERS and fills nothing, that is an outcome, not an error.
    It must come back as a response so the normal P<=0 path runs — not as a raise."""
    class _KillOrders:
        async def create(self, params):
            return {"state": "ORDER_STATE_CANCELED", "executions": [{"cumQuantity": "0"}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _KillOrders()
    resp = await client.place_limit_fok("slug-x", 0.24, 100, "[t]")
    assert resp is not None and order_filled_qty(resp) == 0.0


@pytest.mark.asyncio
async def test_sell_back_dry_run_returns_mock_price():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = True
    client._sdk = type("S", (), {})()
    assert await client.sell_back("slug-x", 100, "[t]") == (0.50, 100.0)


@pytest.mark.asyncio
async def test_sell_back_no_bids_returns_none():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"bids": []}})   # unwind reads via the fresh path
    # (price, sold) — a BARE None here would TypeError in _unwind_poly_excess's unpack
    assert await client.sell_back("slug-x", 100, "[t]") == (None, 0.0)


@pytest.mark.asyncio
async def test_sell_back_reads_fresh_book_not_cdn_cache():
    # The unwind price MUST come from the live book — a 30s-cached price could miss the real
    # book and strand the leg. Assert sell_back routes through the nonce cache-bust (_sdk.get).
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    sdk = client._sdk = _FakeSDK({"marketData": {"bids": [
        {"px": {"value": "0.60", "currency": "USD"}, "qty": "10"}]}})
    sdk.orders = _FakeOrders({"executions": [{"order": {"state": "ORDER_STATE_FILLED"}}]})
    await client.sell_back("slug-x", 100, "[t]")
    assert len(sdk.get_calls) == 1                          # used .get(), not markets.book
    path, query = sdk.get_calls[0]
    assert path == "/v1/markets/slug-x/book" and "_" in query and query["_"]


@pytest.mark.asyncio
async def test_sell_back_sells_at_best_bid():
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_FILLED"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"bids": [
        {"px": {"value": "0.60", "currency": "USD"}, "qty": "10"},
        {"px": {"value": "0.55", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert price == pytest.approx(0.60)  # best bid
    assert sold == 100.0
    p = client._sdk.orders.calls[0]
    assert p["intent"] == "ORDER_INTENT_SELL_LONG"
    # IOC, not FOK: Poly does not honor FOK — it silently rewrites it to IOC, observed on a real
    # order that came back partially filled. We now send what we actually get, so a
    # future Poly FOK rollout cannot silently turn our orders all-or-nothing.
    assert p["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert p["price"] == {"value": "0.6000", "currency": "USD"}
    assert p["quantity"] == 100


@pytest.mark.asyncio
async def test_place_limit_fok_short_token_uses_buy_short_and_strips_suffix():
    """A '<slug>::short' token → BUY_SHORT on the bare slug, and the SHORT-space price the caller
    passes is complemented to YES space for the wire.

    The caller convention is unchanged (short-space, because `opp.poly_ask_raw` already carries
    that for a `::short` token). What the wire needs is different: the venue reads a BUY_SHORT
    price in YES space as a sell limit, so a caller's 0.53 must ship as 0.47. Sending 0.53
    unconverted still FILLED — an aggressive sell with too low a limit fills at the touch — but the
    caller's price bound was not the one being enforced, which is the whole point of a limit."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _FakeOrders({"order": {"status": "killed"}})
    await client.place_limit_fok("exg-mlb-tor-bos::short", 0.53, 50, "[t]")
    p = client._sdk.orders.last_params
    assert p["marketSlug"] == "exg-mlb-tor-bos"          # suffix stripped
    assert p["intent"] == "ORDER_INTENT_BUY_SHORT"
    assert p["price"] == {"value": "0.4700", "currency": "USD"}
    assert p["quantity"] == 50


@pytest.mark.asyncio
async def test_place_limit_fok_long_token_price_is_untouched():
    """The complement applies ONLY to a short token — a long price must go out verbatim."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _FakeOrders({"order": {"status": "killed"}})
    await client.place_limit_fok("exg-mlb-tor-bos", 0.53, 50, "[t]")
    p = client._sdk.orders.last_params
    assert p["intent"] == "ORDER_INTENT_BUY_LONG"
    assert p["price"] == {"value": "0.5300", "currency": "USD"}


@pytest.mark.asyncio
async def test_sell_back_short_position_sells_short_at_the_yes_ask():
    """Unwinding a SHORT leg: SELL_SHORT at the best yes ASK, **in yes space** (it buys the yes
    side back, so it crosses the offers, not the bids). Preserves the stranded-leg invariant.

    ⛔ This asserted `1 − best ask`, which made the short flatten IMPOSSIBLE rather than merely
    mispriced: against a 0.849/0.853 book it sent 0.147, i.e. "buy yes back at 0.147 or better"
    while yes was offered at 0.853 — it could never fill, and live it did not, with the leg
    reported stranded. Proven by a real order: SELL_SHORT at yes-space 0.8530 filled at 0.8530
    and closed the position."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_FILLED"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"offers": [
        {"px": {"value": "0.40", "currency": "USD"}, "qty": "10"},
        {"px": {"value": "0.45", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    price, sold = await client.sell_back("exg-mlb-tor-bos::short", 30, "[t]")
    assert price == pytest.approx(0.40)  # the best yes ask, in yes space
    p = client._sdk.orders.calls[0]
    assert p["marketSlug"] == "exg-mlb-tor-bos"
    assert p["intent"] == "ORDER_INTENT_SELL_SHORT"
    assert p["price"] == {"value": "0.4000", "currency": "USD"}


@pytest.mark.asyncio
async def test_sell_back_retries_at_discount_on_a_GENUINE_KILL():
    """A real IOC kill is a 200 with cumQuantity 0 — NOT an exception. That is the case the discount
    retry exists for, and it must keep working.

    ⛔ THIS TEST USED TO RAISE on the first attempt to simulate the kill, and assert the retry fired.
    That pinned the WRONG semantics — `place_limit_fok`'s docstring in the same file states plainly
    that a kill is a zero-fill 200 and that the raising path is ONLY timeout/502/reset. So the test
    asserted the bug (see the sibling test below) was intended behaviour, which is the strongest
    possible signal to a future reader not to fix it."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            if len(self.calls) == 1:                       # genuine kill: terminal, zero filled
                return {"executions": [{"order": {"state": "ORDER_STATE_CANCELED",
                                                  "cumQuantity": "0"}}]}
            return {"executions": [{"order": {"state": "ORDER_STATE_FILLED"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(
        {"marketData": {"bids": [{"px": {"value": "0.60", "currency": "USD"}, "qty": "10"}]}})
    client._sdk.orders = _Orders()
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert price == pytest.approx(0.58)  # 0.60 - 0.02
    assert len(client._sdk.orders.calls) == 2
    assert client._sdk.orders.calls[1]["price"] == {"value": "0.5800", "currency": "USD"}


async def test_sell_back_does_NOT_resell_when_the_reply_is_LOST():
    """⛔ THE OVERSELL. `synchronousExecution` blocks the order ~61ms server-side, so a timeout lands
    INSIDE the fill window — the engine may have sold everything. Returning 0.0 told the retry those
    shares were still held and it re-offered them, taking a long position NET SHORT, with `rem` at 0
    so the caller booked a clean flatten: no strand, no pause, no alert.

    `assert len(calls) == 1` is the load-bearing line. `pytest.raises` ALONE is vacuous here — a fix
    that raised only after the loop finished would satisfy it while still having sent order #2."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            raise RuntimeError("read timeout after the order was accepted")
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(
        {"marketData": {"bids": [{"px": {"value": "0.60", "currency": "USD"}, "qty": "10"}]}})
    client._sdk.orders = _Orders()
    with pytest.raises(Exception):
        await client.sell_back("slug-x", 100, "[t]")
    assert len(client._sdk.orders.calls) == 1, (
        "an unknown outcome must NOT be retried — the engine may already have sold the whole size, "
        "and re-offering it is how a long becomes a naked short")


# ── get_book_depth: authoritative REST depth for the would-fire sampler ──────────

@pytest.mark.asyncio
async def test_get_book_depth_sums_qty_at_best_level_long():
    client = PolyUSClient.__new__(PolyUSClient)
    client._sdk = type("S", (), {})()
    # two offers at the best price 0.24 (summed), one deeper at 0.25 (ignored). qty=100 each.
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_OPEN", offers=[0.24, 0.24, 0.25]))
    assert await client.get_book_depth("slug-x") == pytest.approx(200.0)


@pytest.mark.asyncio
async def test_get_book_depth_short_uses_best_bid_level():
    client = PolyUSClient.__new__(PolyUSClient)
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_OPEN", bids=[0.47, 0.46]))
    assert await client.get_book_depth("exg-mlb-tor-bos::short") == pytest.approx(100.0)  # best bid 0.47 only


@pytest.mark.asyncio
async def test_get_book_depth_none_on_empty_and_error():
    client = PolyUSClient.__new__(PolyUSClient)
    client._sdk = type("S", (), {})()
    client._sdk.markets = _FakeBookMarkets(_book("MARKET_STATE_OPEN", offers=[]))
    assert await client.get_book_depth("slug-x") is None

    class _Boom:
        async def book(self, slug):
            raise RuntimeError("network")
    client._sdk.markets = _Boom()
    assert await client.get_book_depth("slug-x") is None       # fail-closed


# ── sell_back partial fills (the IOC consequence) ─────────────────────────────────────────────
# Poly rewrites our FOK->IOC (observed on a real order), so a SELL can partial-fill.
# sell_back's "try best, retry 2c worse" design assumed all-or-nothing: attempt 1 either filled
# completely or did nothing. It doesn't. Pin the real state space.

class _PartialOrders:
    """Fills `fills` in order, one per create() call. Records each request."""
    def __init__(self, fills):
        self.fills, self.calls = list(fills), []

    async def create(self, params):
        self.calls.append(params)
        want = int(params["quantity"])
        got = min(self.fills.pop(0) if self.fills else 0, want)
        state = "ORDER_STATE_FILLED" if got >= want else "ORDER_STATE_PARTIALLY_FILLED"
        return {"executions": [{"order": {
            "state": state, "quantity": want, "cumQuantity": got}}]}


def _bid_book(px="0.60"):
    return {"marketData": {"bids": [{"px": {"value": px, "currency": "USD"}, "qty": "1000"}]}}


@pytest.mark.asyncio
async def test_sell_back_reports_the_quantity_actually_sold():
    """sell_back must report HOW MUCH sold, not just a price. A caller that only learns a price
    cannot tell a full sale from a partial one — and _unwind_poly_excess must strand only the
    unsold remainder."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(_bid_book())
    client._sdk.orders = _PartialOrders([100])          # sells all 100 first try
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert sold == 100.0
    assert price == pytest.approx(0.60)


@pytest.mark.asyncio
async def test_sell_back_retry_resizes_to_the_UNSOLD_remainder():
    """THE BUG: the retry closed over the ORIGINAL size. After attempt 1 partial-fills 60/100,
    attempt 2 re-sent quantity=100 while only 40 were still held — an OVERSELL. It must ask for
    the 40 that are left."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(_bid_book())
    client._sdk.orders = _PartialOrders([60, 40])       # 60 at best, 40 on the retry
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert [c["quantity"] for c in client._sdk.orders.calls] == [100, 40], \
        "retry must re-size to the unsold remainder, never re-send the original size"
    assert sold == 100.0


@pytest.mark.asyncio
async def test_sell_back_partial_on_both_attempts_reports_what_sold():
    """Neither attempt clears it: we still SOLD 80. Reporting 'failed / nothing sold' would
    strand 100 phantom shares and lose the proceeds of 80 from P&L."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(_bid_book())
    client._sdk.orders = _PartialOrders([60, 20])       # 60 + 20 = 80 of 100
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert sold == 80.0
    assert price is not None, "80 shares really sold — a price must be reported for the P&L"


@pytest.mark.asyncio
async def test_sell_back_nothing_sold_reports_zero():
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK(_bid_book())
    client._sdk.orders = _PartialOrders([0, 0])
    price, sold = await client.sell_back("slug-x", 100, "[t]")
    assert (price, sold) == (None, 0.0)


# ── quote_from_md: the parser get_fill_quote is a fetch around ────────────────────────────────
# Extracted so the would-fire sampler can read a book it already holds in BOTH directions (ask
# here, bid via kalshi_arb._poly_exit_from_book) off ONE fetch. Poly allows ~1 req/s sustained and
# THROTTLES over-limit with a late/stale 200, so a second fetch for the bid would have degraded
# the freshness of the ask read beside it — on a budget the fire path shares.
import bot.poly_us.client as client_mod
from bot.poly_us.client import quote_from_md
from bot.poly_us.sides import parse_token


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["exg-mlb-tor-bos", "exg-mlb-tor-bos::short"])
async def test_quote_from_md_is_exactly_what_get_fill_quote_returns(token, monkeypatch):
    """The anti-drift property, stated as an assertion.

    The sampler reads through quote_from_md and the fire path through get_fill_quote. If those
    could disagree, the sampler would be recording a bot we do not run — so pin that one IS the
    other, over both token orientations (short is where a space-mismatch would hide).

    The clock is pinned and the book carries a `stats` block ON PURPOSE. quote_from_md is not
    pure — it ages the stats off time.time() — so without both, the 5th tuple element is all-None
    on either side, the comparison passes trivially while pinning nothing about stats, and adding
    a realistic fixture later would turn it into a flake instead of a failure.
    """
    monkeypatch.setattr(client_mod.time, "time", lambda: 1_800_000_000.0)
    book = _book("MARKET_STATE_OPEN", offers=[0.48, 0.49], bids=[0.47, 0.46])
    book["marketData"]["stats"] = {
        "openInterest": "1200",
        "openInterestSetTime": "2026-07-15T20:00:00.000000000Z",
        "lastTradePx": {"value": "0.4850", "currency": "USD"},
        "lastTradeSetTime": "2026-07-15T20:00:05.000000000Z",
    }
    c = PolyUSClient.__new__(PolyUSClient)
    c._dry_run = False
    c._sdk = type("S", (), {})()
    c._sdk.markets = _FakeBookMarkets(book)

    via_fetch = await c.get_fill_quote(token)
    _slug, is_short = parse_token(token)
    direct = quote_from_md(book["marketData"], is_short)
    assert direct == via_fetch
    assert via_fetch[4]["open_interest"] == 1200.0      # stats really were parsed, not all-None
    assert via_fetch[4]["oi_age_s"] is not None


def test_quote_from_md_fails_soft_on_a_junk_marketdata():
    # It rides an observability read; a malformed book must yield the no-quote tuple, not raise.
    ask, state, levels, tx, stats = quote_from_md({}, False)
    assert (ask, state, levels, tx) == (None, "?", [], None)
    assert stats["open_interest"] is None


@pytest.mark.asyncio
async def test_preview_order_sends_the_fire_path_shape_and_places_nothing():
    """preview_order previews the SAME order shape place_limit_fok would send (intent, tick-space
    price, IOC tif) via orders.preview — which places nothing — and returns the echoed Order."""
    client = PolyUSClient.__new__(PolyUSClient)
    captured = {}

    async def _preview(params):
        captured["params"] = params
        return {"order": {"price": {"value": "0.4300"}}}

    client._sdk = type("S", (), {})()
    client._sdk.orders = type("O", (), {"preview": staticmethod(_preview)})()

    resp = await client.preview_order("game-slug", 0.4321, 5)
    req = captured["params"]["request"]
    assert req["marketSlug"] == "game-slug"
    assert req["intent"] == "ORDER_INTENT_BUY_LONG"
    assert req["price"] == {"value": "0.4321", "currency": "USD"}
    assert req["quantity"] == 5
    assert req["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"   # the real order's tif, not FOK
    assert resp["order"]["price"]["value"] == "0.4300"         # echoed handling returned


@pytest.mark.asyncio
async def test_preview_order_short_token_buys_short_and_rejects_return_None():
    client = PolyUSClient.__new__(PolyUSClient)
    captured = {}

    async def _preview(params):
        captured["params"] = params
        return {"order": {}}

    client._sdk = type("S", (), {})()
    client._sdk.orders = type("O", (), {"preview": staticmethod(_preview)})()
    await client.preview_order("game-slug::short", 0.6, 3)
    assert captured["params"]["request"]["intent"] == "ORDER_INTENT_BUY_SHORT"

    async def _boom(params):
        raise RuntimeError("off-tick reject")

    client._sdk.orders = type("O", (), {"preview": staticmethod(_boom)})()
    assert await client.preview_order("game-slug", 0.6, 3) is None   # reject → None, never raises


# ── place_limit_gtc post_only (the venue-side backstop BEHIND the crossing guard) ──────────────

class _GtcOrders:
    def __init__(self):
        self.calls = []

    async def create(self, params):
        self.calls.append(params)
        # the RECORDED create shape (logs/poly_postonly_probe.json): id top-level, not nested
        return {"id": "oid-1", "executions": []}


class _GtcCachedMarkets:
    """The two CDN-CACHED reads, wired to WORK so that a guard which used one fails the cache-bust
    assertion *specifically* rather than incidentally blowing up on a missing attribute.

    Every Poly public GET is Cloudflare max-age=30 and neither of these cache-busts, so a guard
    reading them can authorize a placement against a touch up to 30s old — fail-OPEN, because a
    stale-HIGH ask lets a crossing buy through while a stale-LOW one only false-refuses. `bbo` is
    the route the guard used before this was caught, so it raises: that regression should be
    loud in every test, not only the three that count reads."""

    def __init__(self, sdk: "_GtcSdk"):
        self._sdk = sdk

    async def book(self, slug):                     # _fetch_book(fresh=False) — CDN-cacheable
        self._sdk.cached_reads.append(slug)
        return self._sdk.book_payload()

    async def bbo(self, slug):
        raise AssertionError(
            "place_limit_gtc read the CDN-cached /bbo touch — the guard must read the "
            "cache-busted _fetch_book(fresh=True)")


class _GtcSdk:
    """The fake SDK place_limit_gtc actually drives: `get` (the cache-busted `/book` read behind
    `_fetch_book(fresh=True)`) and `orders.create`.

    A side passed as None is ABSENT from the book — that is how a one-sided or empty book reaches
    the guard. Reads are recorded on two separate lists, `reads` (cache-busted) and `cached_reads`,
    so a test can assert not merely that the book was read but that it was read the fresh way."""

    def __init__(self, best_bid: str | None, best_ask: str | None):
        self.orders = _GtcOrders()
        self.markets = _GtcCachedMarkets(self)
        self.reads: list[tuple[str, dict | None]] = []
        self.cached_reads: list[str] = []
        self._bid, self._ask = best_bid, best_ask

    def book_payload(self) -> dict:
        md: dict = {}
        if self._bid is not None:
            md["bids"] = [{"px": {"value": self._bid, "currency": "USD"}, "qty": "50"}]
        if self._ask is not None:
            md["offers"] = [{"px": {"value": self._ask, "currency": "USD"}, "qty": "50"}]
        return {"marketData": md}

    async def get(self, path, query=None):          # _fetch_book(fresh=True) — nonce → origin
        self.reads.append((path, query))
        return self.book_payload()


def _gtc_client(best_ask: str | None = None, best_bid: str | None = None) -> PolyUSClient:
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _GtcSdk(best_bid, best_ask)
    return client


@pytest.mark.asyncio
async def test_place_limit_gtc_post_only_sets_participate_dont_initiate():
    """post_only=True must reach the wire as `participateDontInitiate: True` — the venue-side
    maker guarantee the whole rebate case rests on. Proven enforced on real money: the order
    RESTS rather than being rejected."""
    client = _gtc_client(best_ask="0.60")
    await client.place_limit_gtc("slug-x", 0.50, 1, "[t]", post_only=True)
    p = client._sdk.orders.calls[0]
    assert p["participateDontInitiate"] is True
    assert p["tif"] == "TIME_IN_FORCE_GOOD_TILL_CANCEL"
    assert p["quantity"] == 1


@pytest.mark.asyncio
async def test_place_limit_gtc_default_omits_participate_dont_initiate():
    """Without post_only the flag must be ABSENT from the wire, not False — the default caller
    (unwind paths) must keep sending byte-identical payloads to what every prior real order sent."""
    client = _gtc_client(best_ask="0.60")
    await client.place_limit_gtc("slug-x", 0.50, 1, "[t]")
    p = client._sdk.orders.calls[0]
    assert "participateDontInitiate" not in p


@pytest.mark.asyncio
async def test_place_limit_gtc_post_only_still_refuses_crossing():
    """The client-side crossing guard stays IN FRONT of the venue flag: a would-cross price is
    refused with nothing placed, post_only or not. The flag is a backstop for the read-vs-place
    race, never a bypass (place_limit_gtc docstring: 'do not remove it or add a bypass flag')."""
    client = _gtc_client(best_ask="0.50")
    out = await client.place_limit_gtc("slug-x", 0.50, 1, "[t]", post_only=True)
    assert out is None
    assert client._sdk.orders.calls == []


# ── place_limit_gtc SELL side (resting ask) — plumbing for a Poly maker run, unused today ──────

class _TwoSidedGtcMarkets:
    """bbo() carrying BOTH touches — for the `get_best_bid`/`get_best_ask` unit tests only.

    ⚠️ NOT the placement guard's read any more: `place_limit_gtc` reads the cache-busted `/book`
    (see _GtcSdk). These two accessors survive for slug-level reporting reads, where a 30s-old
    touch is acceptable."""

    def __init__(self, best_bid: str, best_ask: str):
        self._bid, self._ask = best_bid, best_ask

    async def bbo(self, slug):
        return {"marketData": {"bestBid": {"value": self._bid, "currency": "USD"},
                               "bestAsk": {"value": self._ask, "currency": "USD"}}}


def _bbo_client(best_bid: str, best_ask: str) -> PolyUSClient:
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _GtcOrders()
    client._sdk.markets = _TwoSidedGtcMarkets(best_bid, best_ask)
    return client


def _gtc_client_2s(best_bid: str, best_ask: str) -> PolyUSClient:
    """A client whose cache-busted book read carries BOTH touches, so the buy guard (needs the
    ask) and the sell guard (needs the bid) can be exercised against one book — which is also
    what the guard now does for real: ONE fetch, both sides."""
    return _gtc_client(best_ask=best_ask, best_bid=best_bid)


@pytest.mark.asyncio
async def test_get_best_bid_parses_marketdata():
    """The bbo-side mirror of get_best_ask — one bbo call, `marketData.bestBid`."""
    client = _bbo_client(best_bid="0.40", best_ask="0.45")
    assert await client.get_best_bid("slug-x") == 0.40


@pytest.mark.asyncio
async def test_get_best_bid_none_when_absent_or_unreadable():
    client = _bbo_client(best_bid="0.40", best_ask="0.45")

    class _NoBid:
        async def bbo(self, slug):
            return {"marketData": {"bestAsk": {"value": "0.45", "currency": "USD"}}}

    client._sdk.markets = _NoBid()
    assert await client.get_best_bid("slug-x") is None

    class _Boom:
        async def bbo(self, slug):
            raise RuntimeError("venue 502")

    client._sdk.markets = _Boom()
    assert await client.get_best_bid("slug-x") is None


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_rests_a_synthetic_ask_as_BUY_SHORT_at_the_complement():
    """THE direction test, and the one that cost the most to get right.

    Poly's intent names are {verb}_{POSITION}, where the verb acts ON the position: BUY_LONG and
    SELL_SHORT both END UP LONG yes (acquire a long / dispose of a short), and BUY_SHORT and
    SELL_LONG both end up SHORT yes. So an OPENING resting ask — sell yes exposure we do not hold —
    is BUY_SHORT, **not** SELL_SHORT.

    [MEASURED on 327,673 real prints in logs/passive_markout_trades.csv, where the venue's own
    `taker_side` is stored raw: SELL_SHORT is `ORDER_SIDE_BUY` and prints ABOVE the yes mid 98.9% of
    the time (it lifts the offer, like BUY_LONG); BUY_SHORT is `ORDER_SIDE_SELL` and hits the yes bid
    97.8% of the time. The mapping is 1:1 with zero exceptions.] Sending SELL_SHORT for an ask would
    have rested a second BID — the arm would never sell, would ratchet long, and every "the ask side
    filled X%" number off it would be fiction.

    ⚠️ The DIRECTION above is the durable part. The price-space claim that used to follow it — that
    a `*_SHORT` intent is priced in SHORT space — was WRONG and is corrected below: the venue reads
    both short intents in YES space, so the ask goes on the wire at p, not 1−p."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    await client.place_limit_gtc("slug-x", 0.44, 2, "[t]", side="sell")
    p = client._sdk.orders.calls[0]
    assert p["intent"] == "ORDER_INTENT_BUY_SHORT"
    assert p["tif"] == "TIME_IN_FORCE_GOOD_TILL_CANCEL"
    # ⛔ YES SPACE, NOT the complement. This used to assert "0.5600". The venue reads a
    # BUY_SHORT price as a yes-space SELL limit, so 0.5600 meant "sell down to 0.56" — against a
    # 0.40 bid that is deeply marketable, which is why every real attempt was either post-only
    # REJECTED or filled instantly at the bid as a taker.
    assert p["price"] == {"value": "0.4400", "currency": "USD"}
    assert p["quantity"] == 2


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_ships_yes_space_with_no_complement():
    """The GTC ask carries the caller's yes-space price to the wire UNCHANGED.

    This test previously asserted the complement (0.55 → "0.4500") and guarded that subtraction's
    float exactness. There is no subtraction here any more — the complement was the bug — so what
    it guards now is that none reappears. The float-boundary concern moved to `place_limit_fok`,
    which is the one short path that still converts; see the test below."""
    client = _gtc_client_2s(best_bid="0.10", best_ask="0.60")
    await client.place_limit_gtc("slug-x", 0.55, 1, "[t]", side="sell")
    assert client._sdk.orders.calls[0]["price"] == {"value": "0.5500", "currency": "USD"}


@pytest.mark.asyncio
async def test_place_limit_fok_short_complement_is_exact_at_the_float_boundary():
    """`1.0 - 0.55` is 0.44999999999999996 in float, and this value goes straight to a 4dp wire
    format. `place_limit_fok` is now the only short path that complements, so the exactness guard
    lives here: it must use `bot.core.money.complement`, not a raw subtraction."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()
    client._sdk.orders = _FakeOrders({"order": {"status": "killed"}})
    await client.place_limit_fok("slug-x::short", 0.55, 1, "[t]")
    assert client._sdk.orders.last_params["price"] == {"value": "0.4500", "currency": "USD"}


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_refuses_at_or_below_the_best_bid():
    """The MIRROR of the buy guard: a yes-space ask at or under the yes bid crosses and executes as
    a TAKER. Refuse with nothing placed — at the touch and through it, both. (In short space the
    same order is a BUY at 1−p against a short ask of 1−bid, so the two conditions are one.)"""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    assert await client.place_limit_gtc("slug-x", 0.40, 1, "[t]", side="sell") is None
    assert await client.place_limit_gtc("slug-x", 0.39, 1, "[t]", side="sell") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_refuses_when_the_touch_is_UNREADABLE():
    """FAIL-CLOSED, and BOTH sides do it — a crossing check that cannot run must refuse, not wave
    the order through.

    ⚠️ This docstring used to say the buy path was "skipped entirely when get_best_ask returns None
    ... so the order goes out unchecked — pre-existing, and left alone because it is a live path".
    That was fixed by 7daa773 (the same commit that added this file's buy-side tests) and the claim
    outlived the defect by a day. The sell side is no longer the exception; it is the pattern."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")

    async def _boom(path, query=None):
        raise RuntimeError("venue 502")

    client._sdk.get = _boom
    assert await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="sell") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_reads_the_bid_not_the_ask():
    """An ask priced above the best ask is NOT crossing — it rests further out. If the sell path
    reused the buy guard's bestAsk read it would refuse this legal order (and, worse, would let a
    genuinely crossing ask through)."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    out = await client.place_limit_gtc("slug-x", 0.60, 1, "[t]", side="sell")
    assert out is not None
    assert client._sdk.orders.calls[0]["intent"] == "ORDER_INTENT_BUY_SHORT"


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_composes_with_post_only():
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="sell", post_only=True)
    p = client._sdk.orders.calls[0]
    assert p["participateDontInitiate"] is True
    assert p["intent"] == "ORDER_INTENT_BUY_SHORT"


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_refuses_a_short_token():
    """Yes-space quoting only. A `::short` token would make the ask side a SELL of the short leg
    (i.e. a buy of the long in disguise) and the bid/ask geometry stops mirroring — and the buy
    guard's own bbo read does not even handle the suffix. Refuse before any venue call."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    with pytest.raises(PreSendRefusal, match="short"):
        await client.place_limit_gtc("slug-x::short", 0.44, 1, "[t]", side="sell")
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_rejects_an_unknown_side_before_any_venue_call():
    """A typo'd side must never silently fall back to `buy` — that is a wrong-DIRECTION order."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    with pytest.raises(PreSendRefusal, match="side"):
        await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="Sell")
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_default_payload_is_unchanged_by_the_side_parameter():
    """The whole safety argument for extending this signature: the default call must send exactly
    what it always has. Asserted as a WHOLE-DICT equality, not a key set — a key-set assertion left
    every value free, and a mutation flipping manualOrderIndicator to MANUAL survived the suite."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    await client.place_limit_gtc("slug-x", 0.41, 1, "[t]")
    assert client._sdk.orders.calls[0] == {
        "marketSlug": "slug-x",
        "intent": "ORDER_INTENT_BUY_LONG",
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": "0.4100", "currency": "USD"},
        "quantity": 1,
        "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
    }


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_dry_run_still_runs_the_crossing_guard():
    """DRY exercises the same refusal as live — the guard sits before the dry short-circuit."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    client._dry_run = True
    assert await client.place_limit_gtc("slug-x", 0.40, 1, "[t]", side="sell") is None
    out = await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="sell")
    assert out["status"] == "dry_run"
    assert client._sdk.orders.calls == []


# ── place_limit_gtc BUY side — the three defects a money-path review turned up ────────────────


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_refuses_when_the_touch_is_UNREADABLE():
    """FAIL CLOSED, defect 1. The guard used to read `if ask is not None and price >= ask` — so an
    unreadable touch SKIPPED the check entirely and the order went out UNGUARDED, at exactly the
    moment the book is least knowable. A crossing check that cannot run must refuse."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")

    async def _boom(path, query=None):
        raise RuntimeError("venue 502")

    client._sdk.get = _boom
    assert await client.place_limit_gtc("slug-x", 0.41, 1, "[t]") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_refuses_when_THE_ASK_IS_ABSENT():
    """The other route to an unreadable touch: the read succeeds but the book carries no offers (an
    empty side, or a shape change). Same answer — a missing side is 'we could not tell', never
    'nothing to cross'."""
    client = _gtc_client(best_bid="0.40")          # bids only: no ask to guard against
    assert await client.place_limit_gtc("slug-x", 0.41, 1, "[t]") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_refuses_when_THE_BID_IS_ABSENT():
    """The sell-side mirror: offers but no bids. The sell guard needs the BID, so an empty bid side
    is unreadable for it — refuse rather than rest an ask into a book we cannot price against."""
    client = _gtc_client(best_ask="0.45")          # offers only: no bid to guard against
    assert await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="sell") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_guards_on_the_WIRE_price_not_the_raw_float():
    """Defect 2 — the guard and the wire must be the SAME number. The wire price is 4dp, so a buy at
    0.44996 into a 0.45 ask passed the raw-float guard (0.44996 < 0.45) and then went out as
    "0.4500": AT the touch the guard had just cleared it as behind. Quantize FIRST, guard on that."""
    client = _gtc_client(best_ask="0.45")
    assert await client.place_limit_gtc("slug-x", 0.44996, 1, "[t]") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_guards_on_the_WIRE_price_not_the_raw_float():
    """The sell side has the same gap and gets the same fix: a yes-space ask at 0.40004 over a 0.40
    bid cleared the raw compare, then reached the wire as short-space "0.6000" — i.e. a yes ask AT
    the bid, which crosses. One canonical rounding, applied before either guard runs."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    assert await client.place_limit_gtc("slug-x", 0.40004, 1, "[t]", side="sell") is None
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_refuses_a_short_token():
    """Defect 3. `get_best_ask(token)` passed "slug::short" straight to `bbo`, so the buy guard read
    the LONG side's ask and compared it to a short-space price — the wrong touch entirely. Refused
    outright rather than given short-space arithmetic: this method has zero production callers, and
    a short-space quote is already expressible as side="sell" on the LONG token (which sends exactly
    the BUY_SHORT this would have). ValueError before any venue call — a caller bug, not a market
    condition."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    with pytest.raises(ValueError, match="short"):
        await client.place_limit_gtc("slug-x::short", 0.41, 1, "[t]")
    assert client._sdk.orders.calls == []


@pytest.mark.asyncio
async def test_place_limit_gtc_wire_price_is_the_one_canonical_rounding():
    """The quantized value IS what ships — not a second `f"{price:.4f}"` beside it. Two roundings of
    the same number are two chances to disagree; here there is only one, and the payload it produces
    for an ordinary price is byte-identical to what every prior real order sent."""
    client = _gtc_client(best_ask="0.60")
    await client.place_limit_gtc("slug-x", 0.4356789, 1, "[t]")
    assert client._sdk.orders.calls[0]["price"] == {"value": "0.4357", "currency": "USD"}


@pytest.mark.asyncio
async def test_place_limit_gtc_buy_guard_reads_a_CACHE_BUSTED_book():
    """THE guard's read must reach ORIGIN. It used to route through `markets.bbo`, which is not
    cache-busted, and every Poly public GET is Cloudflare max-age=30 — so "the whole safety contract
    of this function" could clear a placement against a touch half a minute old, fail-OPEN (a
    stale-HIGH ask lets a crossing buy through). `_fetch_book(fresh=True)` appends the nonce that
    makes the read cf=MISS. The bbo fake raises, so a regression is red rather than silent."""
    client = _gtc_client(best_ask="0.60")
    assert await client.place_limit_gtc("slug-x", 0.50, 1, "[t]") is not None
    assert client._sdk.cached_reads == []                     # never the cacheable /book route
    assert len(client._sdk.reads) == 1
    path, query = client._sdk.reads[0]
    assert path == "/v1/markets/slug-x/book"
    assert list(query) == ["_"] and query["_"].isdigit()      # the CF cache-busting nonce


@pytest.mark.asyncio
async def test_place_limit_gtc_sell_guard_reads_a_CACHE_BUSTED_book():
    """Same for the sell side, off the SAME single fetch — both touches come out of one book read,
    so the two guards describe one instant and cost one request rather than two."""
    client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
    assert await client.place_limit_gtc("slug-x", 0.44, 1, "[t]", side="sell") is not None
    assert client._sdk.cached_reads == []
    assert len(client._sdk.reads) == 1
    path, query = client._sdk.reads[0]
    assert path == "/v1/markets/slug-x/book"
    assert list(query) == ["_"] and query["_"].isdigit()


@pytest.mark.asyncio
async def test_place_limit_gtc_guard_reads_the_BOOK_not_the_cached_bbo_touch():
    """Stated as its own assertion, not left as a side effect of the fake: neither side may call
    `markets.bbo`. `get_best_ask`/`get_best_bid` survive for reporting reads, where a 30s-old touch
    is acceptable — a placement decision is not one of those."""
    for side, price in (("buy", 0.41), ("sell", 0.44)):
        client = _gtc_client_2s(best_bid="0.40", best_ask="0.45")
        calls: list[str] = []
        client._sdk.markets.bbo = lambda slug: calls.append(slug)   # would be awaited if reached
        assert await client.place_limit_gtc("slug-x", price, 1, "[t]", side=side) is not None
        assert calls == []


def test_wire_price_takes_a_Decimal_EXACTLY_never_through_float():
    """[Decimal boundary] A Decimal caller price must quantize DIRECTLY — laundering it
    through `from_float` re-parses via `repr(float(x))`, which collapses the exact value onto
    the nearest shortest-repr double first. The witness value sits just below a half-centicent
    tie: exact quantize says 0.4443; the float round-trip lands ON the tie (0.44435) and
    HALF_EVEN then answers 0.4444."""
    assert _wire_price(Decimal("0.44434999999999999999")) == Decimal("0.4443")
    # floats keep the documented from_float path, byte-for-byte
    assert _wire_price(0.44435) == Decimal("0.4444")


@pytest.mark.asyncio
async def test_place_limit_gtc_accepts_a_Decimal_price_on_the_wire():
    """Callers holding exact Decimal prices pass them straight through — no float cast at the
    call site. The witness is >4dp just below a half-centicent tie so the float-laundered
    path answers differently (0.4444) — a 4dp witness would pass either way and pin nothing."""
    client = _gtc_client(best_ask="0.60")
    await client.place_limit_gtc("slug-x", Decimal("0.44434999999999999999"), 1, "[t]")
    assert client._sdk.orders.calls[0]["price"] == {"value": "0.4443", "currency": "USD"}


def test_wire_price_refuses_a_non_finite_price_with_the_TYPED_refusal():
    """No literal ever shipped (Infinity dies at the quantize, NaN at the crossing compare —
    both InvalidOperation); the guard's value is the TYPE. The asserts
    pin `PreSendRefusal`, not bare ValueError — `_place` clears its durable intent on the
    type alone, so a one-word revert to ValueError restores the phantom-intent bug with a
    green suite unless this pins it."""
    with pytest.raises(PreSendRefusal):
        _wire_price(Decimal("NaN"))
    with pytest.raises(PreSendRefusal):
        _wire_price(float("nan"))
    with pytest.raises(PreSendRefusal):
        _wire_price(float("inf"))
    with pytest.raises(PreSendRefusal):
        _wire_quantity(Decimal("0"))
    with pytest.raises(PreSendRefusal):
        _wire_quantity(float("nan"))


def test_wire_price_rounds_HALF_EVEN_at_a_TIE():
    """Pin the rounding MODE, not just the digit count. `from_float`'s shortest-repr recovery is
    what puts a caller's price ON an exact half-centicent tie (Decimal("0.12345") — a binary double
    essentially never lands on one), so the tie rule is reachable and must be nailed down.

    Two prices, because one mutant hides behind each: HALF_EVEN and HALF_DOWN agree on 0.12345
    (→0.1234) and only HALF_UP differs (→0.1235); HALF_EVEN and HALF_UP agree on 0.12355 (→0.1236,
    5 is odd so it rounds up to the even 6) and only HALF_DOWN differs (→0.1235). Either price alone
    leaves one substitution alive."""
    assert _wire_price(0.12345) == Decimal("0.1234")      # HALF_UP would give 0.1235
    assert _wire_price(0.12355) == Decimal("0.1236")      # HALF_DOWN would give 0.1235


@pytest.mark.asyncio
async def test_place_limit_gtc_ships_the_HALF_EVEN_tie_price():
    """And the mode reaches the WIRE, so the pin cannot be satisfied by a helper nobody's payload
    uses. Same two ties, asserted on the emitted price string."""
    for price, wire in ((0.12345, "0.1234"), (0.12355, "0.1236")):
        client = _gtc_client(best_ask="0.60")
        await client.place_limit_gtc("slug-x", price, 1, "[t]")
        assert client._sdk.orders.calls[0]["price"] == {"value": wire, "currency": "USD"}


# ── touch_from_md — THE one touch parser (guard + rebate probe read the same code) ─────────────


def _lvl(px: str, qty: str = "10") -> dict:
    return {"px": {"value": px, "currency": "USD"}, "qty": qty}


def test_touch_from_md_takes_max_bid_and_min_ask_NOT_level_zero():
    """The SDK's book is a bare list with no documented ordering, and every sibling reader
    (get_book_depth, sell_back, quote_from_md) already refuses to trust it. Levels are given here in
    the WRONG order on purpose: a `bids[0]`/`offers[0]` read returns 0.30/0.70 and would clear a
    crossing order at 0.55 as "behind the touch"."""
    md = {"bids": [_lvl("0.30"), _lvl("0.50"), _lvl("0.40")],
          "offers": [_lvl("0.70"), _lvl("0.52"), _lvl("0.60")]}
    bid, ask, _tob = touch_from_md(md)
    assert (bid, ask) == (Decimal("0.50"), Decimal("0.52"))


def test_touch_from_md_bid_tob_is_the_SUM_at_the_best_price():
    """Queue depth ahead of us is everything resting AT the best bid, not level-0's slice, and not
    the whole bid side."""
    md = {"bids": [_lvl("0.50", "3"), _lvl("0.40", "99"), _lvl("0.50", "4")],
          "offers": [_lvl("0.60")]}
    _bid, _ask, tob = touch_from_md(md)
    assert tob == Decimal("7")          # 3+4 — not 3 (level 0) and not 106 (every level)


def test_touch_from_md_tob_sums_quantities_EXACTLY():
    """Quantities are Decimal too, and summed exactly. `Decimal(float("0.1"))`
    is 0.1000000000000000055511151231257827, so a laundered sum is not equal to the number anyone
    would compare it against."""
    _bid, _ask, tob = touch_from_md({"bids": [_lvl("0.50", "0.1"), _lvl("0.50", "0.2")],
                                     "offers": [_lvl("0.60")]})
    assert tob == Decimal("0.3")


def test_touch_from_md_reads_prices_EXACTLY_as_decimals():
    """Prices are Decimal parsed from the venue's string form — a float hop would launder binary
    error into the number a crossing guard compares against."""
    md = {"bids": [_lvl("0.005")], "offers": [_lvl("0.995")]}
    bid, ask, _tob = touch_from_md(md)
    assert (bid, ask) == (Decimal("0.005"), Decimal("0.995"))
    assert isinstance(bid, Decimal) and isinstance(ask, Decimal)


def test_touch_from_md_accepts_a_BARE_px_as_well_as_an_amount():
    """The venue has been seen sending both shapes for a level price (the tick field is bare while
    its neighbouring price fields are {value,currency} Amounts). Read either."""
    bid, ask, _tob = touch_from_md({"bids": [{"px": "0.42", "qty": "1"}],
                                    "offers": [{"px": "0.44", "qty": "1"}]})
    assert (bid, ask) == (Decimal("0.42"), Decimal("0.44"))


def test_touch_from_md_bare_px_arriving_as_a_JSON_NUMBER_is_not_laundered():
    """⚠️ The case a string-only test misses, and mutation testing caught: an UNQUOTED px. JSON
    numbers arrive as floats, and `Decimal(0.005)` is 0.005000000000000000104083408558… — a value
    that is not equal to any tick and would make an exact `px == best` comparison (the TOB sum)
    silently drop levels. `parse_wire` recovers the shortest round-tripping decimal instead."""
    bid, ask, tob = touch_from_md({"bids": [{"px": 0.005, "qty": 2}, {"px": 0.005, "qty": 3}],
                                   "offers": [{"px": 0.995, "qty": 1}]})
    assert (bid, ask) == (Decimal("0.005"), Decimal("0.995"))
    assert tob == Decimal("5")          # both levels matched `px == best`, none laundered away


def test_touch_from_md_missing_side_is_None_not_a_number():
    """A side with no levels is None — 'we could not tell'. A placement guard reads that as a
    refusal; it must never be able to read it as 'nothing to cross'."""
    assert touch_from_md({"offers": [_lvl("0.60")]})[0] is None      # no bids → no bid
    assert touch_from_md({"bids": [_lvl("0.40")]})[1] is None        # no offers → no ask
    assert touch_from_md({}) == (None, None, None)


def test_touch_from_md_unparseable_book_is_all_None_and_never_raises():
    """It rides a money path, so a shape change must fail closed, not raise into the caller."""
    assert touch_from_md({"bids": [{"qty": "1"}], "offers": [_lvl("0.60")]}) == (None, None, None)
    assert touch_from_md({"bids": "nonsense", "offers": 7}) == (None, None, None)
    assert touch_from_md(None) == (None, None, None)


# ── get_market_tick — promoted from the rebate probe; fails to REFUSE, never to a default ──────

class _TickMarkets:
    def __init__(self, payload):
        self._payload = payload
        self.slugs: list[str] = []

    async def retrieve_by_slug(self, slug):
        self.slugs.append(slug)
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _tick_client(payload) -> PolyUSClient:
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = True
    client._sdk = type("S", (), {})()
    client._sdk.markets = _TickMarkets(payload)
    return client


@pytest.mark.asyncio
async def test_get_market_tick_parses_the_bare_number_shape():
    """Confirmed against the live venue: it wraps as {'market': {...}} and sends the tick as a BARE number,
    unlike its neighbouring {value,currency} price fields."""
    from decimal import Decimal
    client = _tick_client({"market": {"orderPriceMinTickSize": 0.001}})
    assert await client.get_market_tick("example-x") == Decimal("0.001")
    assert client._sdk.markets.slugs == ["example-x"]


@pytest.mark.asyncio
async def test_get_market_tick_parses_a_string_tick_exactly():
    """Decimal(str(...)) — a float-constructed Decimal would launder binary error into the value
    the whole quote grid is computed from."""
    from decimal import Decimal
    client = _tick_client({"market": {"orderPriceMinTickSize": "0.005"}})
    assert await client.get_market_tick("slug") == Decimal("0.005")


@pytest.mark.asyncio
async def test_get_market_tick_tolerates_a_flat_payload():
    from decimal import Decimal
    client = _tick_client({"orderPriceMinTickSize": 0.01})
    assert await client.get_market_tick("slug") == Decimal("0.01")


@pytest.mark.asyncio
async def test_get_market_tick_none_on_missing_unparseable_or_error():
    """Three ways to have no tick, one answer: None = REFUSE upstream. A defaulted tick is right
    often enough to look correct while silently mispricing the markets that differ
    (scanner._event_poly_tick)."""
    assert await _tick_client({"market": {}}).get_market_tick("s") is None
    assert await _tick_client({"market": {"orderPriceMinTickSize": "wat"}}).get_market_tick("s") is None
    assert await _tick_client({"market": {"orderPriceMinTickSize": None}}).get_market_tick("s") is None
    assert await _tick_client(RuntimeError("venue 502")).get_market_tick("s") is None
    assert await _tick_client("not-a-dict").get_market_tick("s") is None


@pytest.mark.asyncio
async def test_sell_back_short_retry_pays_MORE_not_less():
    """⛔ "2¢ through the touch" is a different DIRECTION on each side.

    Disposing of a LONG sells, so conceding means accepting LESS (−2¢). Disposing of a SHORT buys
    the yes side back, so conceding means paying MORE (+2¢). A single `best_price - 0.02` ladder
    applied the long direction to both, making the short retry strictly LESS likely to fill than
    the attempt it was meant to rescue — a rescue that moved away from the book.
    """
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_NEW"}}]}   # nothing fills
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"offers": [
        {"px": {"value": "0.40", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    await client.sell_back("exg-mlb-tor-bos::short", 5, "[t]")
    prices = [float(c["price"]["value"]) for c in client._sdk.orders.calls]
    assert len(prices) == 2, f"expected a retry, got {prices}"
    assert prices[0] == pytest.approx(0.40)
    assert prices[1] > prices[0], f"short retry must pay MORE, got {prices}"
    assert prices[1] == pytest.approx(0.42)


@pytest.mark.asyncio
async def test_sell_back_long_retry_still_accepts_LESS():
    """The long direction is unchanged — the fix must not flip both."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_NEW"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"bids": [
        {"px": {"value": "0.40", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    await client.sell_back("exg-mlb-tor-bos", 5, "[t]")
    prices = [float(c["price"]["value"]) for c in client._sdk.orders.calls]
    assert len(prices) == 2
    assert prices[1] < prices[0], f"long retry must accept LESS, got {prices}"
    assert prices[1] == pytest.approx(0.38)


@pytest.mark.asyncio
async def test_sell_back_short_retry_never_lands_behind_the_first_attempt():
    """The +2¢ clamp must not re-create the no-op it exists to remove.

    A flat `min(0.99, best+0.02)` makes the retry LESS marketable than attempt 1 whenever the yes
    ask already exceeds 0.99 — the deepest-adverse regime, i.e. precisely when a short flatten is
    needed. Reachable because `sell_back` re-reads a fresh book at unwind time, after the move that
    caused the unwind."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_NEW"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"offers": [
        {"px": {"value": "0.995", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    await client.sell_back("exg-mlb-tor-bos::short", 5, "[t]")
    prices = [float(c["price"]["value"]) for c in client._sdk.orders.calls]
    assert prices[1] >= prices[0], (
        f"short retry must never be LESS marketable than attempt 1; got {prices}")


@pytest.mark.asyncio
async def test_sell_back_long_retry_never_lands_behind_the_first_attempt():
    """Mirror of the above at the bottom of the grid: a yes bid under 0.01."""
    class _Orders:
        def __init__(self):
            self.calls = []
        async def create(self, params):
            self.calls.append(params)
            return {"executions": [{"order": {"state": "ORDER_STATE_NEW"}}]}
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = _FakeSDK({"marketData": {"bids": [
        {"px": {"value": "0.005", "currency": "USD"}, "qty": "10"},
    ]}})
    client._sdk.orders = _Orders()
    await client.sell_back("exg-mlb-tor-bos", 5, "[t]")
    prices = [float(c["price"]["value"]) for c in client._sdk.orders.calls]
    assert prices[1] <= prices[0], (
        f"long retry must never be LESS marketable than attempt 1; got {prices}")


@pytest.mark.asyncio
async def test_preview_order_short_validates_the_SAME_wire_price_as_place_limit_fok():
    """`preview_order`'s docstring promises it validates EXACTLY what the fire path would send. It
    must therefore apply the same short-space→yes-space conversion; otherwise the pre-flight check
    validates the MIRROR of the real order. This drifted the moment the conversion moved into
    place_limit_fok."""
    client = PolyUSClient.__new__(PolyUSClient)
    client._dry_run = False
    client._sdk = type("S", (), {})()

    class _O:
        def __init__(self):
            self.last = None
            self.last_params = None
        async def preview(self, params):
            self.last = params
            return {"order": {}}
        async def create(self, params):
            self.last_params = params
            return {"order": {"status": "killed"}}
    client._sdk.orders = _O()
    await client.preview_order("slug-x::short", 0.53, 5)
    await client.place_limit_fok("slug-x::short", 0.53, 5, "[t]")
    assert (client._sdk.orders.last["request"]["price"]
            == client._sdk.orders.last_params["price"]
            == {"value": "0.4700", "currency": "USD"})


@pytest.mark.asyncio
async def test_place_limit_gtc_ships_a_fractional_quantity_EXACTLY_as_a_string():
    """A live preview probe showed the venue echoes fractional quantities unrounded (0.8, 12.8,
    "0.80" all exact; integer control clean), so the int(round(size)) wire cast — which
    silently reshaped 12.80 into 13, the overshoot-through-flat class — is retired. A
    fractional size ships as the exact STRING form of its Decimal (the venue accepted the
    string shape; a float would re-launder the exactness the whole change exists for)."""
    from decimal import Decimal as D
    client = _gtc_client(best_bid="0.40")
    await client.place_limit_gtc("slug-x", 0.85, D("12.80"), "[t]", side="sell")
    p = client._sdk.orders.calls[0]
    assert p["quantity"] == "12.80", f"exact string, got {p['quantity']!r}"


@pytest.mark.asyncio
async def test_place_limit_gtc_integral_quantity_wire_is_byte_identical_int():
    """Every existing caller (the maker) sends whole sizes — their wire payload must not
    change shape under the fractional support (int, not '5' or 5.0)."""
    from decimal import Decimal as D
    client = _gtc_client(best_ask="0.60")
    await client.place_limit_gtc("slug-x", 0.50, D("5"), "[t]")
    p = client._sdk.orders.calls[0]
    assert p["quantity"] == 5 and isinstance(p["quantity"], int)
    client2 = _gtc_client(best_ask="0.60")
    await client2.place_limit_gtc("slug-x", 0.50, 5, "[t]")
    assert client2._sdk.orders.calls[0]["quantity"] == 5


class TestOrderExVerdicts:
    """get_order_ex/cancel_order_ex return TYPED verdicts —
    ok | not_found | error — because the SDK's NotFoundError fires on ANY 404 (a
    renamed route included), and its `message` degrades to the bare reason-phrase
    when the body is not JSON. Verdict `not_found` therefore requires a STRUCTURED
    venue body; a bare-reason-phrase 404 is `error`, never terminal — otherwise an
    SDK base-URL drift retires every resting order at once and re-places over live
    ones — the load-bearing failure mode this whole class exists to hold shut."""

    def _client(self):
        from bot.poly_us.client import PolyUSClient
        c = PolyUSClient.__new__(PolyUSClient)
        c._dry_run = False
        return c

    class _SDKRaise:
        def __init__(self, exc):
            self._exc = exc

            class _O:
                def __init__(s):
                    s.exc = exc

                async def retrieve(s, oid):
                    raise s.exc

                async def cancel(s, oid, params):
                    raise s.exc
            self.orders = _O()

    @staticmethod
    def _nf(body):
        import httpx
        from polymarket_us import NotFoundError
        resp = httpx.Response(404, request=httpx.Request("GET", "https://x/v1/order/1"))
        return NotFoundError("nf", response=resp, body=body)

    def test_structured_not_found_is_the_typed_verdict(self):
        import asyncio
        c = self._client()
        c._sdk = self._SDKRaise(self._nf({"message": "Order not found"}))
        body, verdict = asyncio.run(c.get_order_ex("X"))
        assert body is None and verdict == "not_found"
        ok, cv = asyncio.run(c.cancel_order_ex("X", "slug"))
        assert ok is False and cv == "not_found"

    def test_bare_reason_phrase_404_is_ERROR_never_terminal(self):
        import asyncio
        c = self._client()
        c._sdk = self._SDKRaise(self._nf(None))          # route-drift shape: no JSON body
        body, verdict = asyncio.run(c.get_order_ex("X"))
        assert body is None and verdict == "error"
        ok, cv = asyncio.run(c.cancel_order_ex("X", "slug"))
        assert ok is False and cv == "error"

    def test_transport_failure_is_error(self):
        import asyncio
        c = self._client()
        c._sdk = self._SDKRaise(RuntimeError("Request timed out."))
        body, verdict = asyncio.run(c.get_order_ex("X"))
        assert body is None and verdict == "error"

    def test_success_passes_the_body_through_and_get_order_wrapper_unchanged(self):
        import asyncio
        c = self._client()

        class _SDKOk:
            class orders:
                @staticmethod
                async def retrieve(oid):
                    return {"order": {"id": oid}}

                @staticmethod
                async def cancel(oid, params):
                    return {}
        c._sdk = _SDKOk()
        body, verdict = asyncio.run(c.get_order_ex("X"))
        assert verdict == "ok" and body == {"order": {"id": "X"}}
        assert asyncio.run(c.get_order("X")) == {"order": {"id": "X"}}
        ok, cv = asyncio.run(c.cancel_order_ex("X", "slug"))
        assert ok is True and cv == "ok"

    def test_sdk_absent_and_dry_run_branches_are_pinned(self, monkeypatch):
        """Both fallbacks point the SAFE way and must be held there:
        SDK-absent classifies error (never a fabricated terminal not_found), and a
        DRY cancel answers ok (the WAS-LIVE path) — a DRY not_found would let a dry
        preview retire orders it believes live, diverging DRY from live in exactly
        the branch under test."""
        import asyncio
        import builtins

        from bot.poly_us.client import PolyUSClient
        real_import = builtins.__import__

        def _no_sdk(name, *a, **k):
            if name == "polymarket_us":
                raise ImportError("absent")
            return real_import(name, *a, **k)
        monkeypatch.setattr(builtins, "__import__", _no_sdk)
        assert PolyUSClient._order_verdict(RuntimeError("Order not found")) == "error"
        monkeypatch.setattr(builtins, "__import__", real_import)
        c = self._client()
        c._dry_run = True
        ok, verdict = asyncio.run(c.cancel_order_ex("X", "slug"))
        assert (ok, verdict) == (True, "ok")
