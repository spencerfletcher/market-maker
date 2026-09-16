"""Tests for KalshiScanner market discovery."""
import pytest
from unittest.mock import MagicMock


def _make_event(ticker, title, markets):
    return {
        "event_ticker": ticker,
        "title": title,
        "markets": markets,
    }


def _make_market(ticker, yes_label, no_label, close_time, status="open", subtitle="Jun 14"):
    return {
        "ticker": ticker,
        "yes_sub_title": yes_label,
        "no_sub_title": no_label,
        "close_time": close_time,
        "status": status,
        "subtitle": subtitle,
    }


@pytest.fixture
def scanner():
    from bot.kalshi.scanner import KalshiScanner
    client = MagicMock()
    return KalshiScanner(client)


def test_parse_open_market():
    event = _make_event(
        "KXNBAGAME-LAKCEL",
        "Lakers vs Celtics",
        [_make_market("KXNBAGAME-LAKCEL-JUN14", "Lakers", "Celtics", "2026-06-14T23:00:00Z")],
    )
    from bot.kalshi.scanner import _parse_event
    markets = _parse_event(event)
    assert len(markets) == 1
    m = markets[0]
    assert m.ticker == "KXNBAGAME-LAKCEL-JUN14"
    assert m.yes_side_label == "Lakers"
    assert m.no_side_label == "Celtics"
    assert m.status == "open"
    assert m.price_tick == pytest.approx(0.01)   # no price_ranges in fixture → fallback <n>


# ── price tick read live from price_ranges[].step, with a fail-safe fallback ──────────────────

def test_price_tick_read_live_from_price_ranges():
    from bot.kalshi.scanner import _parse_event
    m = _make_market("KX-1", "A", "B", "2026-06-14T23:00:00Z")
    m["price_ranges"] = [{"start": "0.00", "end": "1.00", "step": "0.05"}]   # hypothetical nickel tick
    out = _parse_event(_make_event("EV", "A vs B", [m]))
    assert out[0].price_tick == pytest.approx(0.05)


def test_parse_price_tick_fallback_never_zero():
    from bot.kalshi.scanner import _parse_price_tick
    assert _parse_price_tick({"price_ranges": [{"step": "0.0100"}]}) == pytest.approx(0.01)
    # absent / empty / null / zero / garbage → 0.01, NEVER 0 (a 0 tick divides-by-zero in the floor)
    for bad in ({}, {"price_ranges": []}, {"price_ranges": [{}]},
                {"price_ranges": [{"step": "0"}]}, {"price_ranges": [{"step": "x"}]},
                {"price_ranges": None}):
        assert _parse_price_tick(bad) == pytest.approx(0.01)


def test_parse_volume_24h_never_raises():
    """This parser runs on the LIVE cross-arb scan path and `fetch_markets` catches per-SERIES, so a
    raise here would drop every market in that series from arb detection every ~3s — blind, with only
    an ERROR log. Kalshi has changed a field's shape under us twice (orderbook->orderbook_fp,
    positions->market_positions), and this field's only consumer is an offline ranking heuristic."""
    from bot.kalshi.scanner import _parse_volume_24h
    assert _parse_volume_24h({"volume_24h_fp": "34.00"}) == pytest.approx(34.0)
    assert _parse_volume_24h({"volume_24h": 12}) == pytest.approx(12.0)          # legacy key
    assert _parse_volume_24h({"volume_24h_fp": "0.00"}) == pytest.approx(0.0)    # zero is a real value
    # every shape Kalshi could plausibly send instead → 0.0, never an exception
    for bad in ({}, {"volume_24h_fp": None}, {"volume_24h_fp": "x"}, {"volume_24h_fp": {"v": 1}},
                {"volume_24h_fp": ["34.00"]}, {"volume_24h_fp": "-5"}):
        assert _parse_volume_24h(bad) == pytest.approx(0.0), bad


def test_parse_skips_closed_market():
    event = _make_event(
        "KXNBAGAME-LAKCEL",
        "Lakers vs Celtics",
        [_make_market("KXNBAGAME-LAKCEL-JUN14", "Lakers", "Celtics", "2026-06-14T23:00:00Z", status="closed")],
    )
    from bot.kalshi.scanner import _parse_event
    markets = _parse_event(event)
    assert markets == []


def test_parse_event_preserves_event_ticker():
    event = _make_event(
        "KXNBAGAME-LAKCEL",
        "Lakers vs Celtics",
        [_make_market("KXNBAGAME-LAKCEL-JUN14", "Lakers", "Celtics", "2026-06-14T23:00:00Z")],
    )
    from bot.kalshi.scanner import _parse_event
    markets = _parse_event(event)
    assert markets[0].event_ticker == "KXNBAGAME-LAKCEL"
    assert markets[0].title == "Lakers vs Celtics"


def test_parse_multiple_markets_in_event_filters_closed():
    event = _make_event(
        "KXNBAGAME-BOS",
        "Celtics Game",
        [
            _make_market("KXNBAGAME-BOS-JUN15", "Celtics", "Heat", "2026-06-15T01:00:00Z"),
            _make_market("KXNBAGAME-BOS-JUN16", "Celtics", "Pacers", "2026-06-16T01:00:00Z", status="closed"),
        ],
    )
    from bot.kalshi.scanner import _parse_event
    markets = _parse_event(event)
    assert len(markets) == 1
    assert markets[0].ticker == "KXNBAGAME-BOS-JUN15"
