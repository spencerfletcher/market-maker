"""Tests for KalshiOrderBookCache WebSocket price cache."""
import asyncio
import json
import pytest
from unittest.mock import MagicMock


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def cache():
    from bot.kalshi.feed import KalshiOrderBookCache
    client_mock = MagicMock()
    client_mock.ws_url = "wss://fake"
    client_mock.ws_headers.return_value = {}
    return KalshiOrderBookCache(client_mock, series_prefixes=["KXNBA"])


def _ticker_msg(market_ticker, yes_bid="0.4500", yes_ask="0.5500"):
    return json.dumps({
        "type": "ticker",
        "msg": {
            "market_ticker": market_ticker,
            "yes_bid_dollars": yes_bid,
            "yes_ask_dollars": yes_ask,
        }
    })


def test_parse_ticker_updates_yes_prices(cache):
    _run(cache._handle_message(_ticker_msg("KXNBAGAME-LAKCEL-JUN14")))
    assert cache.get_best_bid("KXNBAGAME-LAKCEL-JUN14", "yes") == pytest.approx(0.45)
    assert cache.get_best_ask("KXNBAGAME-LAKCEL-JUN14", "yes") == pytest.approx(0.55)


def test_parse_ticker_derives_no_prices(cache):
    _run(cache._handle_message(_ticker_msg("KXNBAGAME-LAKCEL-JUN14", "0.4000", "0.6000")))
    # no_bid = 1 - yes_ask = 1 - 0.60 = 0.40
    assert cache.get_best_bid("KXNBAGAME-LAKCEL-JUN14", "no") == pytest.approx(0.40)
    # no_ask = 1 - yes_bid = 1 - 0.40 = 0.60
    assert cache.get_best_ask("KXNBAGAME-LAKCEL-JUN14", "no") == pytest.approx(0.60)


def test_ticker_prices_are_exact_decimals_internally(cache):
    """Phase D: the ticker channel quotes decimal STRINGS ("0.4500"), so `float(raw)` threw exactness
    away at the one place it was still free. The cache now holds the wire value exactly; the getters
    stay the float boundary for the 38 external call sites."""
    from decimal import Decimal
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))
    pd = cache._prices[_TKR]
    assert isinstance(pd.yes_bid, Decimal) and isinstance(pd.yes_ask, Decimal)
    assert pd.yes_bid == Decimal("0.45") and pd.yes_ask == Decimal("0.55")
    # 1 - 0.55 is exactly 0.45 (0.44999999999999996 in float)
    assert pd.no_bid == Decimal("0.45") and pd.no_ask == Decimal("0.55")
    assert isinstance(cache.get_best_bid(_TKR, "yes"), float)
    # SCALE is the pin that distinguishes parse_wire from float()-then-convert: both give a value
    # equal to 0.45, but only parsing the venue's own string keeps "0.4500". A value-only assertion
    # here is satisfied by the float round-trip this migration removes.
    assert str(pd.yes_bid) == "0.4500" and str(pd.yes_ask) == "0.5500"


@pytest.mark.parametrize("bad", ["abc", "", "1,2", "0.45.6"])
def test_garbled_ticker_price_cannot_escape_into_the_receive_loop(cache, bad):
    """`float("abc")` raised ValueError OUTSIDE any try, so one garbled ticker field would propagate
    out of `_handle_message` into `run_forever`'s receive loop and cycle the whole Kalshi WS
    connection mid-game — the same failure mode the orderbook path's guards exist to prevent.
    Unparseable ⇒ no price ⇒ leave the cache alone, quietly."""
    _run(cache._handle_message(_ticker_msg(_TKR, bad, "0.5500")))
    assert cache.get_best_bid(_TKR, "yes") is None   # nothing cached, and nothing raised
    # the actual safety property [review nit]: a PRIOR good quote survives a garbled update
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))
    _run(cache._handle_message(_ticker_msg(_TKR, bad, "0.5500")))
    assert cache.get_best_bid(_TKR, "yes") == pytest.approx(0.45)


@pytest.mark.parametrize("bad", ["5.0000", "-0.5000", "1.0001"])
def test_out_of_domain_ticker_price_is_dropped_not_cached(cache, bad):
    """Finite-but-absurd is as wrong as NaN on a [0,1] venue: "5.0000" passing `is_finite()` was
    still cached as a price [review nit]. Drop ⇒ prior quote stands and ages into the freshness
    gates."""
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))
    _run(cache._handle_message(_ticker_msg(_TKR, bad, "0.5500")))
    assert cache.get_best_bid(_TKR, "yes") == pytest.approx(0.45)


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_ticker_price_is_dropped_not_cached(cache, bad):
    """`float("NaN")` parsed happily and wrote a NaN bid/ask into the cache the arb path reads —
    silent, and every comparison against it is False. Decimal is worse still (`Decimal("NaN") > 0`
    RAISES), so the guard has to be explicit: non-finite is not a price."""
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))   # a good quote first
    _run(cache._handle_message(_ticker_msg(_TKR, bad, "0.5500")))
    assert cache.get_best_bid(_TKR, "yes") == pytest.approx(0.45)        # prior quote intact


def test_rest_seeded_prices_cross_the_float_boundary_exactly(cache):
    """`prime`/`refresh` take floats because their caller does (`KalshiMarket.yes_bid` is float-typed
    in scanner.py) — but the cache behind them is Decimal, so the conversion has to happen at a NAMED
    boundary (`from_float`) rather than by letting a float sit in a Decimal-typed field. The
    complement is the tell: `1.0 - 0.55` is 0.44999999999999996 in float, exactly 0.45 here."""
    from decimal import Decimal
    cache._orderbook_mode = False
    cache.prime(_TKR, 0.45, 0.55)
    pd = cache._prices[_TKR]
    assert isinstance(pd.yes_bid, Decimal) and isinstance(pd.no_bid, Decimal)
    assert pd.no_bid == Decimal("0.45") and pd.no_ask == Decimal("0.55")

    other = "KXNBAGAME-REST-JUN20"
    cache.refresh(other, 0.30, 0.70)
    pd2 = cache._prices[other]
    assert isinstance(pd2.yes_ask, Decimal) and isinstance(pd2.no_ask, Decimal)
    assert pd2.no_bid == Decimal("0.30") and pd2.no_ask == Decimal("0.70")


def test_unknown_ticker_returns_none(cache):
    assert cache.get_best_ask("KXNBAGAME-UNKNOWN", "yes") is None
    assert cache.get_best_bid("KXNBAGAME-UNKNOWN", "no") is None


def test_series_prefix_filter_blocks_unrelated(cache):
    """Ticker not starting with a tracked series prefix must be ignored."""
    msg = _ticker_msg("KXFEDDECISION-DEC25-T5.00")  # not in ["KXNBA"]
    _run(cache._handle_message(msg))
    assert cache.get_best_ask("KXFEDDECISION-DEC25-T5.00", "yes") is None


def test_series_prefix_filter_passes_matching(cache):
    msg = _ticker_msg("KXNBAGAME-BOS-JUN20")
    _run(cache._handle_message(msg))
    assert cache.get_best_ask("KXNBAGAME-BOS-JUN20", "yes") is not None


def test_callback_triggered_on_update(cache):
    called = []
    cache.set_callback(lambda: called.append(1))
    _run(cache._handle_message(_ticker_msg("KXNBAGAME-LAKCEL-JUN14")))
    assert len(called) == 1


def test_malformed_message_ignored(cache):
    _run(cache._handle_message("not-json"))
    _run(cache._handle_message('{"type":"unknown"}'))
    assert cache.get_best_ask("anything", "yes") is None


# ── live liquidity from the ticker stream (logging-only; isolated from the price path) ─────────

_TKR = "KXNBAGAME-LAKCEL-JUN14"


def _ticker_with_liq(market_ticker=_TKR, *, bid="0.4500", ask="0.5500",
                     oi="68.00", vol="94.00", last="0.5400", with_bidask=True):
    m = {"market_ticker": market_ticker, "open_interest_fp": oi,
         "volume_fp": vol, "price_dollars": last}
    if with_bidask:
        m["yes_bid_dollars"] = bid
        m["yes_ask_dollars"] = ask
    return json.dumps({"type": "ticker", "msg": m})


def test_ticker_records_live_liquidity(cache):
    _run(cache._handle_message(_ticker_with_liq()))
    liq = cache.get_liquidity(_TKR)
    assert liq is not None
    oi, vol, last, age = liq
    assert oi == pytest.approx(68.0) and vol == pytest.approx(94.0) and last == pytest.approx(0.54)
    assert 0.0 <= age < 5.0                       # fresh: receive-time anchored


def test_liquidity_parse_error_does_not_eat_price_write(cache):
    # FAIL-DIRECTION: a malformed liquidity field must NOT skip the bid/ask price update.
    _run(cache._handle_message(_ticker_with_liq(oi="abc")))   # open_interest_fp unparseable
    assert cache.get_best_ask(_TKR, "yes") == pytest.approx(0.55)   # price STILL written
    assert cache.get_liquidity(_TKR) is None                        # liquidity blank, not faked


def test_liquidity_recorded_even_when_bid_ask_missing(cache):
    # Independence (other direction): a tick with liquidity but no bid/ask still records liquidity;
    # the price is (correctly) not updated.
    _run(cache._handle_message(_ticker_with_liq(with_bidask=False)))
    assert cache.get_liquidity(_TKR) is not None
    assert cache.get_best_ask(_TKR, "yes") is None


def test_liquidity_blank_on_resubscribe(cache):
    # BLANK-NOT-STALE: a (re)subscribe (e.g. a mode switch) clears the cache → blank, not frozen.
    _run(cache._handle_message(_ticker_with_liq()))
    assert cache.get_liquidity(_TKR) is not None

    class _FakeWS:
        async def send(self, _msg):
            pass
    _run(cache._send_subscribe(_FakeWS()))
    assert cache.get_liquidity(_TKR) is None


def test_unseen_ticker_liquidity_none(cache):
    assert cache.get_liquidity("KXNBAGAME-NEVER-SEEN") is None


# ── Frozen-book detection foundation: _note_price_change / _last_book_change_ts ─────────────────
# These three pins are load-bearing — the freshness watchdog (_stale_book_reconnect) keys entirely
# on _last_book_change_ts, which must advance ONLY on a real tradeable-price move. Mirrors the Poly
# feed's _note_book pins. The key is (yes_bid, yes_ask) (no_* are exact complements — 2 DOF).

def test_note_price_change_resend_is_not_a_change(cache):
    """Identical re-send (the frozen-feed signature): the socket looks alive but the book never
    moved → must NOT advance _last_book_change_ts."""
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))
    assert cache._book_changes == 1
    t1 = cache._last_book_change_ts
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))   # identical re-send
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))   # still frozen
    assert cache._book_changes == 1                                      # no advance
    assert cache._last_book_change_ts == t1                              # stamp untouched


def test_note_price_change_liquidity_churn_is_not_a_change(cache):
    """The ticker-mode analog of Poly's 'deep churn ≠ change': a tick with the SAME yes_bid/yes_ask
    but DIFFERENT open_interest/volume keeps _last_msg_ts fresh (socket alive) but the tradeable
    price is frozen → must NOT advance _last_book_change_ts (else OI/volume churn masks a zombie)."""
    _run(cache._handle_message(_ticker_with_liq(bid="0.4500", ask="0.5500", oi="10", vol="20")))
    assert cache._book_changes == 1
    t1 = cache._last_book_change_ts
    # same price, churning liquidity:
    _run(cache._handle_message(_ticker_with_liq(bid="0.4500", ask="0.5500", oi="99", vol="180")))
    assert cache.get_liquidity(_TKR)[0] == pytest.approx(99.0)   # liquidity DID update (logging path)
    assert cache._book_changes == 1                              # but book-change did NOT
    assert cache._last_book_change_ts == t1


def test_note_price_change_real_move_advances(cache):
    """A real best-bid/ask move IS a change → advances _book_changes and the freshness stamp."""
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5500")))
    assert cache._book_changes == 1
    t1 = cache._last_book_change_ts
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4500", "0.5600")))   # yes_ask moved
    assert cache._book_changes == 2
    assert cache._last_book_change_ts >= t1
    _run(cache._handle_message(_ticker_msg(_TKR, "0.4400", "0.5600")))   # yes_bid moved
    assert cache._book_changes == 3
