"""Tests for the raw Polymarket US markets WebSocket transport (bot/poly_us/feed.py).

Covers the pure pieces that don't need a live socket: Ed25519 auth header
construction (signature must verify against the documented message), the subscribe
payload shape, and message dispatch by top-level key. The connect/recv loop itself
is validated live via scripts/poly_us_ws_capture.py.
"""
import base64
import time

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (Encoding, NoEncryption,
                                                          PrivateFormat, PublicFormat)

import bot.poly_us.feed as feed_mod
from bot.poly_us.feed import PolyUSOrderBookCache, _WS_PATH


def _cache():
    return PolyUSOrderBookCache(sdk=None)


# ── Ed25519 auth headers ────────────────────────────────────────────────────

def _seed_and_pub(sk: Ed25519PrivateKey) -> tuple[bytes, bytes]:
    """Raw 32-byte seed and 32-byte public key (the wire forms Poly US uses)."""
    return (sk.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()),
            sk.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))


def test_auth_headers_signature_verifies(monkeypatch):
    sk = Ed25519PrivateKey.generate()
    seed, _ = _seed_and_pub(sk)
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_KEY_ID", "kid-123")
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_SECRET_KEY", base64.b64encode(seed).decode())

    h = _cache()._auth_headers()
    assert h["X-PM-Access-Key"] == "kid-123"
    # Message the server reconstructs: timestamp + "GET" + path
    message = f'{h["X-PM-Timestamp"]}GET{_WS_PATH}'
    # Raises InvalidSignature if the signature doesn't match → test fails.
    sk.public_key().verify(base64.b64decode(h["X-PM-Signature"]), message.encode())


_RFC8032_SEED = bytes.fromhex(
    "9d61b19d effd5a60 ba844af4 92ec2cc4 4449c569 7b326919 703bac03 1cae7f60")  # RFC 8032 §7.1 vector 1 (public)
_RFC8032_SIG_EMPTY = (
    "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e"
    "39701cf9b46bd25bf5f0595bbe24655141438e7a100b")


def test_ed25519_primitive_matches_rfc8032_vector():
    """The signing primitive `_auth_headers` calls is Ed25519 proper, not a look-alike.

    RFC 8032 §7.1 TEST 1: seed 9d61b1…7f60, empty message → signature e55643…100b. Pinned on the
    exact import `_auth_headers` uses, so a swap of the signing backend (pynacl → cryptography,
    2026-09-04) cannot silently change what the venue verifies.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    sig = Ed25519PrivateKey.from_private_bytes(_RFC8032_SEED).sign(b"")
    assert sig.hex() == _RFC8032_SIG_EMPTY


def test_auth_frame_is_byte_identical_for_fixed_seed_and_timestamp(monkeypatch):
    """The whole handshake frame — not just a verifying signature — is byte-for-byte fixed.

    Key = the RFC 8032 TEST 1 seed, clock frozen at 1750000000.0 s. The signed message is
    `{ts}GET/v1/ws/markets`; the expected base64 was cross-checked to be identical under both
    pynacl and cryptography before the dependency was dropped.
    """
    monkeypatch.setattr(feed_mod.time, "time", lambda: 1750000000.0)
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_KEY_ID", "kid-fixed")
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_SECRET_KEY",
                        base64.b64encode(_RFC8032_SEED).decode())

    assert _cache()._auth_headers() == {
        "X-PM-Access-Key": "kid-fixed",
        "X-PM-Timestamp": "1750000000000",
        "X-PM-Signature": ("LmJK2XSBd5UQ0QrriFZatQRxrMTc7qQIvDmtjHNT3O1oXCIZYidwoyINk6fqH9nd"
                           "HDz3KL3qQ5rydsYN5AAQCA=="),
    }


def test_auth_headers_accepts_64_byte_key(monkeypatch):
    sk = Ed25519PrivateKey.generate()
    seed, pub = _seed_and_pub(sk)
    full = seed + pub  # 64-byte seed||pubkey form
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_KEY_ID", "kid")
    monkeypatch.setattr(feed_mod.config, "POLYMARKET_US_SECRET_KEY", base64.b64encode(full).decode())

    h = _cache()._auth_headers()
    message = f'{h["X-PM-Timestamp"]}GET{_WS_PATH}'
    sk.public_key().verify(base64.b64decode(h["X-PM-Signature"]), message.encode())


# ── Subscribe payload ───────────────────────────────────────────────────────

def test_subscribe_payload_shape():
    p = _cache()._subscribe_payload(["slug-a", "slug-b"])
    sub = p["subscribe"]
    assert sub["subscriptionType"] == "SUBSCRIPTION_TYPE_MARKET_DATA"
    assert sub["marketSlugs"] == ["slug-a", "slug-b"]
    assert isinstance(sub["requestId"], str) and sub["requestId"]


# ── Dispatch by top-level key ───────────────────────────────────────────────

def test_dispatch_market_data_sets_ask_and_depth():
    c = _cache()
    c._dispatch({"marketData": {"marketSlug": "S", "offers": [
        {"px": {"value": "0.41"}, "qty": "50"},
        {"px": {"value": "0.40"}, "qty": "100"},   # best ask = lowest
        {"px": {"value": "0.40"}, "qty": "25"},    # same level adds to depth
    ]}})
    assert c.get_best_ask("S") == 0.40
    assert c.get_depth("S") == 125.0


def test_dispatch_market_data_lite_sets_ask_no_depth():
    c = _cache()
    c._dispatch({"marketDataLite": {"marketSlug": "S", "bestAsk": {"value": "0.55"}}})
    assert c.get_best_ask("S") == 0.55
    assert c.get_depth("S") == 0.0


def test_dispatch_heartbeat_bumps_liveness_without_price():
    c = _cache()
    c._last_msg_ts = 0.0
    c._dispatch({"heartbeat": {}})
    assert c._last_msg_ts > 0.0
    assert c.get_best_ask("S") is None


def test_dispatch_error_and_unknown_are_safe():
    c = _cache()
    # Neither should raise.
    c._dispatch({"error": "bad subscription", "requestId": "r1"})
    c._dispatch({"somethingNew": {"x": 1}})
    assert c.get_best_ask("S") is None


_TRADE_FRAME = {
    "subscriptionType": "SUBSCRIPTION_TYPE_TRADE",
    "trade": {"marketSlug": "S", "price": "0.42", "quantity": "7",
              "tradeTime": "2026-08-12T21:00:00Z",
              "taker": {"side": "BUY", "intent": "OPEN"},
              "maker": {"side": "SELL", "intent": "OPEN"}},
}


def test_dispatch_trade_calls_on_trade_sink():
    c = _cache()
    got = []
    c.on_trade = got.append
    c._dispatch(dict(_TRADE_FRAME))
    assert len(got) == 1 and got[0]["trade"]["marketSlug"] == "S"
    # A trade frame must NOT touch the book path.
    assert c.get_best_ask("S") is None


def test_dispatch_trade_without_sink_is_safe():
    # The maker never sets on_trade — a trade frame must be a silent no-op, not a crash.
    c = _cache()
    assert c.on_trade is None
    c._dispatch(dict(_TRADE_FRAME))   # must not raise
    assert c.get_best_ask("S") is None


def test_dispatch_market_data_does_not_call_on_trade():
    # Book path is byte-identical whether or not a trade sink is registered.
    c = _cache()
    got = []
    c.on_trade = got.append
    c._dispatch({"marketData": {"marketSlug": "S", "offers": [
        {"price": {"value": "0.55"}, "size": {"value": "10"}}]}})
    assert got == []


# ── Ask-empty frames are retained, not dropped [N1 2026-08-13] ─────────────

def _md(slug, offers=None, bids=None, omit_offers=False, **extra):
    payload = {"marketSlug": slug, **extra}
    if not omit_offers:
        payload["offers"] = offers if offers is not None else []
    if bids is not None:
        payload["bids"] = bids
    return {"marketData": payload}


def test_ask_empty_frame_overwrites_stale_two_sided_touch():
    """A full ask sweep produces an offers=[] frame; before the fix it was dropped and
    get_book_md kept serving the PRE-sweep two-sided touch — the worst adverse event,
    invisible. Mutation pin: restore the old `if not slug or not offers: return` and
    this goes RED."""
    c = _cache()
    c._on_market_data(_md("s", offers=[{"px": {"value": "0.60"}, "qty": "5"}],
                          bids=[{"px": {"value": "0.55"}, "qty": "7"}]))
    assert "offers" in c.get_book_md("s")
    c._on_market_data(_md("s", offers=[],
                          bids=[{"px": {"value": "0.55"}, "qty": "7"}],
                          transactTime="2026-08-13T05:00:00.000000000Z"))
    md = c.get_book_md("s")
    assert md is not None
    assert "offers" not in md          # ask side reads EMPTY now, not the stale 0.60
    assert md["bids"][0]["px"]["value"] == "0.55"
    assert md["transactTime"] == "2026-08-13T05:00:00.000000000Z"


def test_ask_empty_frame_keeps_pricing_state_last_known():
    """PRICING state deliberately keeps last-known (age-gated by its readers): an
    ask=0.0 sentinel would poison every mid built on _prices."""
    c = _cache()
    c._on_market_data(_md("s", offers=[{"px": {"value": "0.60"}, "qty": "5"}]))
    c._on_market_data(_md("s", offers=[]))
    assert c.get_best_ask("s") == 0.60


def test_ask_empty_marks_offers_key_absent_distinctly():
    """offers:[] (venue says empty) vs key missing entirely — a measurement consumer
    must be able to split them until the wire semantics of key-absence are confirmed."""
    c = _cache()
    c._on_market_data(_md("s", offers=[]))
    assert c._touch_raw["s"]["offers_key_absent"] is False
    c._on_market_data(_md("s2", omit_offers=True))
    assert c._touch_raw["s2"]["offers_key_absent"] is True


# ── on_book raw-frame tap [prereg r3 B1/B2/C4] ─────────────────────────────

def test_on_book_receives_verbatim_frame_including_ask_empty():
    """The sampler's ONE named feed read: the verbatim marketData frame, so
    offers present vs [] vs key-absent reach the consumer undamaged — the cache
    getters cannot serve that distinction (get_book_md omits an empty side;
    _prices keeps last-known through a sweep). Mutation pin: remove the
    on_book dispatch and this goes RED."""
    c = _cache()
    seen = []
    c.on_book = seen.append
    normal = _md("s", offers=[{"px": {"value": "0.60"}, "qty": "5"}])
    c._dispatch(normal)
    swept = _md("s", offers=[])
    c._dispatch(swept)
    absent = _md("s", omit_offers=True)
    c._dispatch(absent)
    assert seen == [normal, swept, absent]
    assert seen[1]["marketData"]["offers"] == []          # empty survives
    assert "offers" not in seen[2]["marketData"]          # key-absence survives


def test_on_book_not_fired_for_trades_and_none_is_safe():
    c = _cache()
    seen = []
    c.on_book = seen.append
    c._dispatch({"trade": {"marketSlug": "s"}})
    assert seen == []
    c2 = _cache()
    c2._dispatch(_md("s", offers=[]))     # no sink registered — must not raise


# ── on_frame pre-routing tap + tap exception safety [prereg r4 C4/C5/N2] ───

def test_on_frame_fires_before_cache_processing_for_all_kinds():
    """Pre-routing tap: the sampler's stamps must carry no parse-cost skew between
    trade and book frames — on_frame fires BEFORE _on_market_data touches the cache.
    Order pin: at on_frame time for the first book frame, the cache is still empty."""
    c = _cache()
    state_at_tap = []
    c.on_frame = lambda m: state_at_tap.append((next(iter(m)), c.get_best_ask("s")))
    c._dispatch(_md("s", offers=[{"px": {"value": "0.60"}, "qty": "5"}]))
    c._dispatch({"trade": {"marketSlug": "s"}})
    c._dispatch({"heartbeat": {}})
    assert state_at_tap[0] == ("marketData", None)   # tap ran BEFORE the cache write
    assert [k for k, _ in state_at_tap] == ["marketData", "trade", "heartbeat"]
    assert c.get_best_ask("s") == 0.60               # cache still processed after


def test_raising_tap_does_not_escape_dispatch():
    """An escaped tap exception would force a reconnect and a frame gap — the exact
    data loss the sampler's gap rule exists to avoid manufacturing. Wrapped + counted."""
    c = _cache()
    def bomb(_m):
        raise RuntimeError("consumer bug")
    c.on_frame = bomb
    c.on_trade = bomb
    c.on_book = bomb
    c._dispatch(_md("s", offers=[{"px": {"value": "0.60"}, "qty": "5"}]))
    c._dispatch({"trade": {"marketSlug": "s"}})
    assert c.get_best_ask("s") == 0.60               # cache work still happened
    assert c.tap_errors == {"on_frame": 2, "on_trade": 1, "on_book": 1}


# ── gap_log opt-in reconnect records [prereg r5 C1] ────────────────────────

def test_gap_log_default_none_and_tap_warning_rate_limited(caplog):
    """gap_log default None = record nothing (arb/maker paths unchanged). Tap warnings
    rate-limit: first 5 then every 1000th — the COUNTER is the health gate, the log is
    a hint; per-frame logging on the receive path would inflate the sampler's floor."""
    c = _cache()
    assert c.gap_log is None and c._gap_started_mono is None
    def bomb(_m):
        raise RuntimeError("x")
    c.on_frame = bomb
    import logging
    with caplog.at_level(logging.WARNING):
        for _ in range(50):
            c._dispatch({"heartbeat": {}})
    assert c.tap_errors["on_frame"] == 50
    assert sum("on_frame tap raised" in r.message for r in caplog.records) == 5
