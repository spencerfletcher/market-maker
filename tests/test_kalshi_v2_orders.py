"""Kalshi V2 order migration — the four side/action mappings and the flat-response parsers.

WHY THIS IS DANGEROUS (read before touching):
  V1 took (side="yes"/"no", action="buy"/"sell") with the price in THAT side's own space.
  V2 is YES-ONLY: side is "bid"/"ask", and `price` is ALWAYS the YES price. There is no "buy NO".
      buy NO @ n  ==  ask (sell YES) @ (1 - n)
  That re-introduces the 1-x complement, which is the exact footgun that once "stranded/blocked
  every NO leg" under V1 (sending yes_price on a no order -> FOK never crossed -> 409). A sign
  error here does not fail loudly — it buys THE WRONG SIDE.

GROUND TRUTH (measured 2026-07-14, not inferred):
  - demo: we sent {side:"ask", price:"0.3000"} holding zero YES. Kalshi's OWN fill record came
    back {"side":"no","outcome_side":"no","no_price_dollars":"0.7000","yes_price_dollars":"0.3000"}
    and the position went to position_fp=-1.00 (short YES == long NO), balance -0.70-fee.
    => buy NO @ 0.70  ==  ask @ 0.30.  CONFIRMED BY THE EXCHANGE.
  - prod: {side:"bid", price:"0.4500"} filled 1 YES @ 0.4270.  => buy YES == bid.
  - The CREATE response is FLAT (no `order` envelope, no `status`); the GET response is nested
    WITH `status`. The V1 parsers read the nested shape, so against a V2 create they silently
    returned 0.0 / False — which would make the bot flatten Poly while holding Kalshi (a naked
    position it does not know it has). Hence the parser tests below.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.core import config
from bot.kalshi.client import (
    _v2_order_params,
    kalshi_filled_qty,
    kalshi_order_filled,
)

# The REAL V2 create response we measured on prod (1 YES @ 0.4270). Fixture, not invented.
REAL_V2_CREATE = {
    "order_id": "94cb58b9-003f-481e-f4c6-f0699a394bd7",
    "fill_count": "1.00",
    "remaining_count": "0.00",
    "average_fill_price": "0.4270",
    "average_fee_paid": "0.0172",
    "ts_ms": 1784063018859,
}
# The nested shape the GET endpoint returns (and what V1 used to return).
NESTED_GET = {"order": {"fill_count_fp": "1.00", "remaining_count_fp": "0.00",
                        "status": "executed", "side": "yes", "outcome_side": "no"}}
DRY_RESP = {"order": {"status": "dry_run"}}


# ── the four mappings: buy/sell x yes/no -> (v2_side, v2_yes_price) ──────────────────────

@pytest.mark.parametrize("side,action,price,exp_side,exp_price", [
    # buy YES @ 0.45 -> bid @ 0.45   (PROVEN on prod)
    ("yes", "buy", "0.4500", "bid", 0.45),
    # buy NO  @ 0.70 -> ask @ 0.30   (PROVEN on demo — Kalshi booked side:"no" @ no_price 0.70)
    ("no", "buy", "0.7000", "ask", 0.30),
    # sell YES @ 0.45 -> ask @ 0.45  (selling the YES you hold)
    ("yes", "sell", "0.4500", "ask", 0.45),
    # sell NO @ 0.70 -> bid @ 0.30   (closing a NO == buying YES at the complement)
    ("no", "sell", "0.7000", "bid", 0.30),
])
def test_the_four_mappings(side, action, price, exp_side, exp_price):
    got_side, got_price = _v2_order_params(side, action, price)
    assert got_side == exp_side, f"{action} {side} must map to {exp_side}"
    assert got_price == pytest.approx(exp_price), f"{action} {side} @ {price} -> yes price {exp_price}"


def test_no_side_flips_BOTH_side_and_price():
    """The NO side flips the book side AND complements the price. Flipping only one is the bug."""
    yes_side, yes_px = _v2_order_params("yes", "buy", "0.3000")
    no_side, no_px = _v2_order_params("no", "buy", "0.3000")
    assert (yes_side, yes_px) == ("bid", pytest.approx(0.30))
    assert (no_side, no_px) == ("ask", pytest.approx(0.70))   # both flipped


def test_buy_and_sell_of_the_same_side_are_opposite_book_sides_at_the_SAME_price():
    for side, px in (("yes", "0.4500"), ("no", "0.7000")):
        b_side, b_px = _v2_order_params(side, "buy", px)
        s_side, s_px = _v2_order_params(side, "sell", px)
        assert {b_side, s_side} == {"bid", "ask"}, "buy/sell must be opposite book sides"
        assert b_px == pytest.approx(s_px), "same side-space price -> same wire price"


def test_complement_is_EXACT_for_tick_aligned_prices():
    """The tick-rounding footgun dissolves iff we complement AFTER the caller's tick_floor:
    1 - (multiple of 0.01) is still a multiple of 0.01. Pin it so nobody reorders the steps."""
    for cents in range(1, 100):
        p = cents / 100
        _, yes_px = _v2_order_params("no", "buy", f"{p:.4f}")
        assert yes_px == pytest.approx(round(1.0 - p, 4), abs=1e-9)
        assert abs(round(yes_px * 100) - yes_px * 100) < 1e-6, f"{yes_px} is off-tick"


def test_unknown_side_or_action_raises_rather_than_guessing():
    for bad in (("maybe", "buy"), ("yes", "hodl"), ("", "")):
        with pytest.raises(ValueError):
            _v2_order_params(bad[0], bad[1], "0.5000")


# ── parsers: the V2 CREATE response is FLAT ─────────────────────────────────────────────

def test_parsers_read_the_real_v2_create_response():
    """The regression that matters: against the real flat create response the OLD parsers
    returned 0.0/False silently — which flattens Poly while holding Kalshi (naked position)."""
    assert kalshi_filled_qty(REAL_V2_CREATE) == pytest.approx(1.0)
    assert kalshi_order_filled(REAL_V2_CREATE) is True


def test_partial_fill_is_not_reported_as_filled():
    partial = dict(REAL_V2_CREATE, fill_count="3.00", remaining_count="7.00")
    assert kalshi_filled_qty(partial) == pytest.approx(3.0)
    assert kalshi_order_filled(partial) is False, "partial must NOT read as a full fill"


def test_killed_fok_reads_as_zero():
    killed = dict(REAL_V2_CREATE, fill_count="0.00", remaining_count="10.00")
    assert kalshi_filled_qty(killed) == 0.0
    assert kalshi_order_filled(killed) is False


def test_parsers_still_handle_the_nested_GET_shape():
    """GET /portfolio/orders/{id} is nested WITH status — keep reading it (fixtures/diagnostics)."""
    assert kalshi_filled_qty(NESTED_GET) == pytest.approx(1.0)
    assert kalshi_order_filled(NESTED_GET) is True


def test_dry_run_shape_is_unchanged():
    """DRY short-circuits before the network and returns the nested dry_run stub. Behaviour must
    not drift: qty 0, not filled (call sites special-case DRY explicitly)."""
    assert kalshi_filled_qty(DRY_RESP) == 0.0
    assert kalshi_order_filled(DRY_RESP) is False


@pytest.mark.parametrize("junk", [None, "", 42, [], {}, {"order": None}, {"order": "x"},
                                 {"fill_count": "abc", "remaining_count": "0"}])
def test_parsers_never_raise_on_junk(junk):
    assert kalshi_filled_qty(junk) == 0.0
    assert kalshi_order_filled(junk) is False


# ── V2 FOK kill: a 409 is an OUTCOME, not an error ────────────────────────────────────────────
# V2 changed how a kill LOOKS: V1 returned a body with fill_count=0; V2 raises
# 409 fill_or_kill_insufficient_resting_volume. The V2 migration propagated that to
# scripts/order_rtt_probe.py but NOT to the fire path, where _exec_poly_first has no try/except:
#   P = await self._place_poly(...)        # Poly leg FILLS
#   kalshi_resp = await create_order(...)  # raises 409 on a kill
#   ...                                    # NEVER REACHED
#   await self._unwind_poly_excess(...)    # THE FLATTEN NEVER RUNS
# → a naked, UNRECORDED Poly leg: no hedge, no strand, no global pause, no alert. On the DEFAULT
# exec order (poly_first) and the MODAL outcome (64.0% kalshi_moved on economic events).

def _resp_409(detail):
    r = AsyncMock()
    r.status = 409
    r.text = AsyncMock(return_value=detail)
    r.raise_for_status = MagicMock()
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


def _session_for(resp):
    s = AsyncMock()
    s.post = MagicMock(return_value=resp)
    s.__aenter__ = AsyncMock(return_value=s)
    s.__aexit__ = AsyncMock(return_value=False)
    return s


@pytest.fixture
def _client(tmp_path, monkeypatch):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, NoEncryption)
    key = Ed25519PrivateKey.generate()
    p = tmp_path / "k.pem"
    p.write_bytes(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
    monkeypatch.setattr(config, "KALSHI_API_KEY", "id")
    monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", str(p))
    monkeypatch.setattr(config, "KALSHI_ENV", "demo")
    monkeypatch.setattr(config, "DRY_RUN", False)
    from bot.kalshi.client import KalshiClient
    return KalshiClient()


# ── post_only + cancel_order (resting-maker additions; the FOK path must stay byte-unchanged) ──
def _resp_ok(payload):
    r = AsyncMock()
    r.status = 200
    r.json = AsyncMock(return_value=payload)
    r.raise_for_status = MagicMock()
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


@pytest.mark.asyncio
async def test_post_only_flag_is_added_ONLY_when_set(_client, monkeypatch):
    posted = {}
    sess = AsyncMock()
    sess.post = MagicMock(side_effect=lambda url, headers=None, json=None, timeout=None:
                          (posted.update(body=json), _resp_ok({"order_id": "x", "fill_count": "0.00"}))[1])
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    await _client.create_order("TKR", "yes", "buy", 1, "0.4000",
                               time_in_force="good_till_canceled", post_only=True)
    assert posted["body"].get("post_only") is True
    posted.clear()
    await _client.create_order("TKR", "yes", "buy", 1, "0.4000")  # default: FOK path
    assert "post_only" not in posted["body"], "FOK/IOC body must be byte-unchanged"


@pytest.mark.asyncio
async def test_cancel_order_deletes_the_order_path(_client, monkeypatch):
    called = {}
    sess = AsyncMock()
    sess.delete = MagicMock(side_effect=lambda url, headers=None, timeout=None:
                            (called.update(url=url), _resp_ok({"order_id": "abc"}))[1])
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    await _client.cancel_order("abc")
    assert called["url"].endswith("/portfolio/events/orders/abc")  # V2 base (V1 /portfolio/orders is 410-dead)


@pytest.mark.asyncio
async def test_cancel_order_dry_run_hits_no_network(_client, monkeypatch):
    monkeypatch.setattr(config, "DRY_RUN", True)
    sess = AsyncMock()
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    r = await _client.cancel_order("abc")
    assert r["status"] == "dry_run_canceled"
    sess.delete.assert_not_called()


def _resp(status, detail="{}"):
    r = AsyncMock()
    r.status = status
    r.text = AsyncMock(return_value=detail)
    r.json = AsyncMock(return_value={})
    r.raise_for_status = MagicMock()
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


@pytest.mark.asyncio
async def test_cancel_order_404_is_benign_already_gone(_client, monkeypatch):
    # a 404 = the order already filled/gone → cancel is idempotent success, NOT an error → no raise
    sess = AsyncMock()
    sess.delete = MagicMock(return_value=_resp(404, '{"error":{"code":"not_found"}}'))
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    r = await _client.cancel_order("gone-id")
    assert r["status"] == "already_gone"


@pytest.mark.asyncio
async def test_cancel_order_real_failure_still_raises(_client, monkeypatch):
    # a 5xx (or auth) is a REAL failure — must stay loud and raise, not be swallowed as "gone"
    sess = AsyncMock()
    sess.delete = MagicMock(return_value=_resp(500, '{"error":"server"}'))
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    with pytest.raises(RuntimeError):
        await _client.cancel_order("x")


@pytest.mark.asyncio
async def test_post_only_cross_still_raises_so_caller_skips(_client, monkeypatch):
    # a post_only-cross rejection is benign (logged WARNING not ERROR) but MUST still raise so the
    # caller knows the quote wasn't placed and doesn't track a non-existent order.
    sess = AsyncMock()
    sess.post = MagicMock(return_value=_resp(400, '{"error":{"code":"invalid_order","details":"post only cross"}}'))
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    with pytest.raises(RuntimeError):
        await _client.create_order("T", "yes", "buy", 1, "0.5000",
                                   time_in_force="good_till_canceled", post_only=True)


@pytest.mark.asyncio
async def test_fok_kill_409_returns_a_zero_fill_not_an_exception(_client):
    """A killed FOK is a NORMAL outcome — the order reached the engine and didn't fill. It must
    read as zero-fill so the caller's existing kfill==0 reconciliation (flatten the Poly leg) runs.
    Raising here skips the flatten entirely and leaves a naked leg."""
    from bot.kalshi.client import kalshi_filled_qty, kalshi_order_filled
    resp = _resp_409('{"error":{"code":"fill_or_kill_insufficient_resting_volume"}}')
    with patch("bot.kalshi.client.aiohttp.ClientSession", return_value=_session_for(resp)):
        out = await _client.create_order("KX-A", "yes", "buy", 10, "0.4500",
                                         time_in_force="fill_or_kill")
    assert kalshi_filled_qty(out) == 0.0, "a kill must read as ZERO fill"
    assert kalshi_order_filled(out) is False
    assert out is not None, "must not be None — callers reconcile off the response"


@pytest.mark.asyncio
async def test_a_real_error_still_raises(_client):
    """ONLY the FOK-kill is an outcome. Every other 4xx (bad tick, insufficient funds, auth) is a
    genuine error and must stay loud — swallowing those would hide real rejections."""
    for detail in ('{"error":{"code":"invalid_price_tick"}}',
                   '{"error":{"code":"insufficient_balance"}}'):
        resp = _resp_409(detail)
        with patch("bot.kalshi.client.aiohttp.ClientSession", return_value=_session_for(resp)):
            with pytest.raises(RuntimeError):
                await _client.create_order("KX-A", "yes", "buy", 10, "0.4500",
                                           time_in_force="fill_or_kill")


# ── get_resting_orders: the stray-cancel sweep. Envelope VERIFIED 2026-07-19 vs demo:
#    GET /portfolio/orders → {cursor, orders[]}; items carry order_id/ticker/status. It mirrors
#    get_positions' fail-closed contract — a shape it can't read RAISES, never reports "nothing
#    resting" (that would silently skip the very strays the sweep exists to cancel). ──
def _get_session_seq(payloads):
    """A session whose .get returns each payload in turn (drives cursor pagination)."""
    s = AsyncMock()
    s.get = MagicMock(side_effect=[_resp_ok(p) for p in payloads])
    return s


@pytest.mark.asyncio
async def test_get_resting_orders_parses_and_filters_by_ticker(_client, monkeypatch):
    page = {"cursor": "", "orders": [
        {"order_id": "a", "ticker": "KX-A", "status": "resting"},
        {"order_id": "b", "ticker": "KX-B", "status": "resting"}]}
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=_get_session_seq([page])))
    assert [o["order_id"] for o in await _client.get_resting_orders()] == ["a", "b"]
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=_get_session_seq([page])))
    assert [o["order_id"] for o in await _client.get_resting_orders(tickers=["KX-A"])] == ["a"]


@pytest.mark.asyncio
async def test_get_resting_orders_follows_the_cursor_to_the_end(_client, monkeypatch):
    sess = _get_session_seq([
        {"cursor": "p2", "orders": [{"order_id": "a", "ticker": "T"}]},
        {"cursor": "",   "orders": [{"order_id": "b", "ticker": "T"}]}])
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    out = await _client.get_resting_orders()
    assert [o["order_id"] for o in out] == ["a", "b"] and sess.get.call_count == 2


@pytest.mark.parametrize("bad", [{"cursor": ""}, {"orders": "not-a-list"}, ["not", "a", "dict"]])
@pytest.mark.asyncio
async def test_get_resting_orders_raises_on_unreadable_shape_never_reports_empty(_client, monkeypatch, bad):
    # the whole point: an unrecognised shape is NOT evidence of "nothing resting" — it must fail
    # loud (like get_positions), or the sweep would silently leave strays un-cancelled.
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=_get_session_seq([bad])))
    with pytest.raises(RuntimeError):
        await _client.get_resting_orders()


# ── get_fills: the real economics record (actual fill price + fee + is_taker). Envelope VERIFIED
#    2026-07-19 vs demo: GET /portfolio/fills → {cursor, fills[]}. Empty is LEGIT (no fills); an
#    unreadable shape RAISES (must not read as 'no fills' and understate realized cost). ──
@pytest.mark.asyncio
async def test_get_fills_parses_paginates_and_filters(_client, monkeypatch):
    sess = _get_session_seq([
        {"cursor": "p2", "fills": [{"fill_id": "a", "ticker": "T1", "fee_cost": "0.01"}]},
        {"cursor": "",   "fills": [{"fill_id": "b", "market_ticker": "T2"},
                                   {"fill_id": "c", "ticker": "T3"}]}])
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=sess))
    out = await _client.get_fills(min_ts=123)
    assert [f["fill_id"] for f in out] == ["a", "b", "c"] and sess.get.call_count == 2
    # filter matches either `ticker` or `market_ticker`
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=_get_session_seq([
        {"cursor": "", "fills": [{"fill_id": "a", "ticker": "T1"},
                                 {"fill_id": "b", "market_ticker": "T2"}]}])))
    assert [f["fill_id"] for f in await _client.get_fills(tickers=["T2"])] == ["b"]


@pytest.mark.asyncio
async def test_get_fills_empty_is_legitimate_not_an_error(_client, monkeypatch):
    monkeypatch.setattr(_client, "_get_session",
                        AsyncMock(return_value=_get_session_seq([{"cursor": "", "fills": []}])))
    assert await _client.get_fills() == []   # genuinely no fills — NOT a raise


@pytest.mark.parametrize("bad", [{"cursor": ""}, {"fills": "nope"}, ["not", "dict"]])
@pytest.mark.asyncio
async def test_get_fills_raises_on_unreadable_shape_never_understates(_client, monkeypatch, bad):
    monkeypatch.setattr(_client, "_get_session", AsyncMock(return_value=_get_session_seq([bad])))
    with pytest.raises(RuntimeError):
        await _client.get_fills()
