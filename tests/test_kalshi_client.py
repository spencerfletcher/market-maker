"""Tests for KalshiClient auth, signing, and REST calls."""
import base64
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from bot.core import config


@pytest.fixture
def key_path(tmp_path):
    """Generate a real Ed25519 key and write to a temp PEM file."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, NoEncryption,
    )
    key = Ed25519PrivateKey.generate()
    p = tmp_path / "test_key.pem"
    p.write_bytes(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
    return str(p)


@pytest.fixture
def client(key_path, monkeypatch):
    monkeypatch.setattr(config, "KALSHI_API_KEY", "test-key-id")
    monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", key_path)
    monkeypatch.setattr(config, "KALSHI_ENV", "demo")
    monkeypatch.setattr(config, "DRY_RUN", False)
    from bot.kalshi.client import KalshiClient
    return KalshiClient()


def test_sign_returns_base64_string(client):
    sig = client._sign(1_700_000_000_000, "GET", "/trade-api/v2/portfolio/balance")
    decoded = base64.b64decode(sig)
    assert len(decoded) == 64  # Ed25519 signature is always 64 bytes


def test_headers_contain_required_fields(client):
    headers = client._headers("GET", "/trade-api/v2/portfolio/balance")
    assert "KALSHI-ACCESS-KEY" in headers
    assert "KALSHI-ACCESS-SIGNATURE" in headers
    assert "KALSHI-ACCESS-TIMESTAMP" in headers
    assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"].isdigit()


def test_ws_url_demo(client):
    assert "demo" in client.ws_url


@pytest.mark.asyncio
async def test_get_balance(client):
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = MagicMock()
    # Kalshi returns balance in cents; 123450 cents == <n>
    mock_resp.json = AsyncMock(return_value={"balance": "123450"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("bot.kalshi.client.aiohttp.ClientSession", return_value=mock_session):
        balance = await client.get_balance()

    assert balance == pytest.approx(1234.50)


@pytest.mark.asyncio
async def test_create_order_sends_correct_body(client):
    captured = {}

    mock_resp = AsyncMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.status = 200  # _post now checks status before parsing
    mock_resp.json = AsyncMock(return_value={
        "order": {"status": "filled", "fill_count_fp": "10", "taker_fees_dollars": "0.20"}
    })
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    def capture_post(url, headers=None, json=None, **kwargs):
        captured["url"] = url
        captured["json"] = json
        return mock_resp

    mock_session.post = MagicMock(side_effect=capture_post)

    with patch("bot.kalshi.client.aiohttp.ClientSession", return_value=mock_session):
        result = await client.create_order(
            ticker="KXNBAGAME-LAKCEL-JUN14",
            side="yes",
            action="buy",
            count=10,
            price_dollars="0.5500",
        )

    # V2 wire format (migrated 2026-07-14; V1 /portfolio/orders is 410 Gone).
    assert captured["url"].endswith("/portfolio/events/orders"), "must POST the V2 endpoint"
    assert captured["json"]["ticker"] == "KXNBAGAME-LAKCEL-JUN14"
    assert captured["json"]["side"] == "bid"                  # buy YES → bid (V2 is YES-only)
    assert captured["json"]["price"] == "0.5500"              # price is ALWAYS the YES price
    assert captured["json"]["count"] == "10.00"               # V2 wants a fixed-point STRING
    assert captured["json"]["time_in_force"] == "fill_or_kill"
    assert captured["json"]["self_trade_prevention_type"] == "taker_at_cross"  # REQUIRED in V2
    # V1 fields must be GONE — sending them is what the 410 migration removes.
    for dead in ("action", "yes_price_dollars", "no_price_dollars"):
        assert dead not in captured["json"], f"V1 field {dead!r} must not be sent to V2"


def test_kalshi_order_filled_recognizes_executed():
    """A filled Kalshi FOK is status 'executed' (not 'filled') — must read True,
    else a real fill looks like a miss and strands the leg."""
    from bot.kalshi.client import kalshi_order_filled
    assert kalshi_order_filled(
        {"order": {"status": "executed", "fill_count_fp": "13.00",
                   "remaining_count_fp": "0.00"}}) is True
    assert kalshi_order_filled(
        {"order": {"status": "canceled", "fill_count_fp": "0.00",
                   "remaining_count_fp": "13.00"}}) is False
    assert kalshi_order_filled({"order": {"status": "filled"}}) is True
    assert kalshi_order_filled(None) is False
    assert kalshi_order_filled({"order": {}}) is False
    # A partial (remaining > 0) must NOT read as filled even if status=executed.
    assert kalshi_order_filled(
        {"order": {"status": "executed", "fill_count_fp": "3.00",
                   "remaining_count_fp": "2.00"}}) is False


def test_kalshi_filled_qty_reads_partial():
    """IOC can partial-fill — size the Poly leg to the real fill, not the request."""
    from bot.kalshi.client import kalshi_filled_qty
    assert kalshi_filled_qty(
        {"order": {"status": "executed", "fill_count_fp": "8.00",
                   "remaining_count_fp": "4.00"}}) == 8.0
    assert kalshi_filled_qty(
        {"order": {"status": "canceled", "fill_count_fp": "0.00"}}) == 0.0
    assert kalshi_filled_qty(None) == 0.0


@pytest.mark.asyncio
async def test_create_order_no_side_maps_to_ask_at_the_complement(client):
    """A NO order must become `ask` at the COMPLEMENT (1-n) — V2 is YES-only and has no
    per-side price field.

    Same intent as the V1 test this replaces ("a NO order must be priced correctly — the bug
    that mis-priced every NO leg into a 409"), re-expressed for V2's YES-only model. Under V1
    the failure was sending yes_price on a no order; under V2 it is failing to complement.
    Both buy the wrong thing silently. CONFIRMED against the exchange 2026-07-14: ask @ 0.30
    holding zero YES booked side:"no" @ no_price 0.70."""
    captured = {}
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"order": {"status": "filled"}})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    def capture_post(url, headers=None, json=None, **kwargs):
        captured["json"] = json
        return mock_resp
    mock_session.post = MagicMock(side_effect=capture_post)

    with patch("bot.kalshi.client.aiohttp.ClientSession", return_value=mock_session):
        await client.create_order(
            ticker="KXWCGAME-X-JOR", side="no", action="buy",
            count=11, price_dollars="0.9000",
        )

    # buy NO @ 0.90  ==  sell YES @ 0.10
    assert captured["json"]["side"] == "ask", "buy NO must become ask (sell YES)"
    assert captured["json"]["price"] == "0.1000", "price must be the COMPLEMENT (1 - 0.90)"
    assert "no_price_dollars" not in captured["json"]
    assert "yes_price_dollars" not in captured["json"]
    assert "action" not in captured["json"]


@pytest.mark.asyncio
async def test_dry_run_create_order_skips_network(key_path, monkeypatch):
    monkeypatch.setattr(config, "KALSHI_API_KEY", "test-key-id")
    monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", key_path)
    monkeypatch.setattr(config, "KALSHI_ENV", "demo")
    monkeypatch.setattr(config, "DRY_RUN", True)

    from bot.kalshi.client import KalshiClient
    c = KalshiClient()

    with patch("bot.kalshi.client.aiohttp.ClientSession") as mock_cls:
        result = await c.create_order("TICKER", "yes", "buy", 5, "0.50")
        mock_cls.assert_not_called()

    assert result == {"order": {"status": "dry_run"}}


# ── get_positions: the reconciler's Kalshi eyes ───────────────────────────────────────────────
# It returned `data.get("positions", [])` for a response with no `positions` key, so it returned
# [] forever and the reconciler reported "Kalshi confirmed flat" on every poll regardless of what
# we held. These pin the two things that matter: it reads the RIGHT array, and it never answers
# "flat" when it doesn't actually know.

# CAPTURED from the live endpoint 2026-07-16 — the real envelope, not a model of it.
# {'cursor': '', 'event_positions': [], 'market_positions': []}
def _positions_client(key_path, monkeypatch, pages):
    """A client whose _get returns each page of `pages` in turn."""
    monkeypatch.setattr(config, "KALSHI_API_KEY", "id")
    monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", key_path)
    monkeypatch.setattr(config, "KALSHI_ENV", "demo")
    from bot.kalshi.client import KalshiClient
    c = KalshiClient()
    seq = list(pages)
    async def _get(path, params=None):
        assert path == "/portfolio/positions"
        return seq.pop(0)
    c._get = _get
    return c


@pytest.mark.asyncio
async def test_get_positions_reads_market_positions_not_a_nonexistent_key(key_path, monkeypatch):
    """The real envelope. `positions` does not exist; `market_positions` is where they live."""
    c = _positions_client(key_path, monkeypatch, [{
        "cursor": "",
        "event_positions": [{"event_ticker": "KXMLBGAME-26JUL18TEXATL"}],   # NOT this array
        "market_positions": [
            {"ticker": "KXMLBGAME-26JUL181610TEXATL-TEX", "position_fp": "-8.00"},
            {"ticker": "KXWCGAME-26JUL18FRAENG-FRA", "position_fp": "3.00"},
        ],
    }])
    got = await c.get_positions()
    assert [p["ticker"] for p in got] == [
        "KXMLBGAME-26JUL181610TEXATL-TEX", "KXWCGAME-26JUL18FRAENG-FRA"]


@pytest.mark.asyncio
async def test_get_positions_follows_the_cursor(key_path, monkeypatch):
    """Page 1 alone under-reads. `cursor` is '' at the end (captured 2026-07-16)."""
    c = _positions_client(key_path, monkeypatch, [
        {"cursor": "abc", "market_positions": [{"ticker": "A", "position_fp": "1.00"}]},
        {"cursor": "",    "market_positions": [{"ticker": "B", "position_fp": "2.00"}]},
    ])
    assert [p["ticker"] for p in await c.get_positions()] == ["A", "B"]


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [
    {"cursor": "", "positions": []},                       # the OLD assumed shape — must not pass
    {"cursor": "", "event_positions": []},                 # right family, wrong array
    {"cursor": "", "market_positions": None},              # key present, null
    {"cursor": "", "market_positions": "nope"},            # key present, wrong type
    {},                                                    # empty object
    [],                                                    # not an object at all
])
async def test_get_positions_RAISES_rather_than_reporting_flat_on_an_unknown_shape(
        key_path, monkeypatch, body):
    """The load-bearing asymmetry.

    _fetch_kalshi_positions maps an exception to CANNOT-VERIFY and [] to "confirmed flat". A shape
    we cannot read is not evidence of no position, so it must take the first path. Note the first
    case is the exact body the old code assumed — if the venue ever really sent it, we would now
    find out loudly instead of trusting it.
    """
    c = _positions_client(key_path, monkeypatch, [body])
    with pytest.raises(RuntimeError):
        await c.get_positions()


@pytest.mark.asyncio
async def test_get_positions_RAISES_rather_than_truncating(key_path, monkeypatch):
    """A cursor still set at the page stop means we did NOT read everything. Returning what we
    managed to read would report a partial holding as the whole of it."""
    c = _positions_client(key_path, monkeypatch,
                          [{"cursor": "more", "market_positions": [{"ticker": "A"}]}] * 25)
    with pytest.raises(RuntimeError, match="truncated"):
        await c.get_positions()


@pytest.mark.asyncio
async def test_get_positions_empty_means_genuinely_flat(key_path, monkeypatch):
    """The one case where [] IS the right answer: a well-formed response with no positions.
    This is what the live account returns today (captured 2026-07-16)."""
    c = _positions_client(key_path, monkeypatch,
                          [{"cursor": "", "event_positions": [], "market_positions": []}])
    assert await c.get_positions() == []
