"""Tests for PolyUSOrderBookCache price access (WS loop tested manually)."""
import time
import pytest
from bot.poly_us.feed import PolyUSOrderBookCache
from bot.poly_us.sides import parse_token


# ── retain_slugs: closed-market pruning (discovery-membership + 2-cycle debounce) ──────────────

def test_retain_prunes_after_two_absent_cycles():
    f = PolyUSOrderBookCache(sdk=None)
    f.prime("a", 0.40); f.prime("b", 0.55)
    f._subscribed.update({"a", "b"})
    f.retain_slugs({"a"})                       # cycle 1: 'b' absent (1 < 2) → KEPT
    assert set(f._prices) == {"a", "b"} and "b" in f._subscribed
    f.retain_slugs({"a"})                       # cycle 2: 'b' absent (== 2) → PRUNED from both maps
    assert set(f._prices) == {"a"} and "b" not in f._subscribed and "b" not in f._absent_cycles


def test_retain_keeps_still_discovered_even_if_quiet():
    # Prune on membership, NOT staleness: a discovered-but-never-updating slug is KEPT, byte-identical.
    f = PolyUSOrderBookCache(sdk=None)
    f.prime("a", 0.40)
    old = f._prices["a"]
    for _ in range(5):
        f.retain_slugs({"a"})
    assert set(f._prices) == {"a"} and f._prices["a"] == old


def test_retain_flicker_resets_counter():
    # The flagged catch: absent cycle 1, PRESENT cycle 2 → counter RESETS, doesn't accumulate to a prune.
    f = PolyUSOrderBookCache(sdk=None)
    f.prime("b", 0.55)
    f.retain_slugs({"a"})                       # 'b' absent → count 1
    assert "b" in f._prices and f._absent_cycles.get("b") == 1
    f.retain_slugs({"a", "b"})                  # 'b' back → reset
    assert f._absent_cycles.get("b") is None
    f.retain_slugs({"a"})                       # absent again → count 1 (NOT 2) → still kept
    assert "b" in f._prices and f._absent_cycles.get("b") == 1


def test_retain_keying_matches_prices_on_a_real_pair():
    # CATASTROPHIC-FAILURE GUARD: `live`'s keys MUST equal `_prices`'s keys, or every slug looks
    # absent → 2 cycles later prune EVERYTHING (silent total detection loss until restart). Build the
    # tokens the way the scanner's _event_to_pair does — token_yes_a = bare slug, token_yes_b =
    # short_token(slug) (the REAL function, not a hardcoded "::short") — and run the discovery loop's
    # EXACT prime + live-build, both through the real parse_token. Pins that the two keyings can't drift.
    from bot.poly_us.sides import short_token
    slug = "aec-mlb-tor-bos"
    pair_tokens = (slug, short_token(slug))             # = MarketPair.token_yes_a / token_yes_b (scanner)
    f = PolyUSOrderBookCache(sdk=None)
    live: set[str] = set()
    for token in pair_tokens:                           # the discovery loop's exact iteration
        f.prime(token, 1.0)
        live.add(parse_token(token)[0])
    assert live == set(f._prices)                       # identical keys → retain never spuriously prunes
    for _ in range(3):
        f.retain_slugs(live)
    assert set(f._prices) == {slug}                     # the live pair is KEPT across cycles (no prune-all)


def test_retain_watchdog_still_trips_with_live_slate():
    # Pruning must NOT weaken freeze detection for the slugs that REMAIN: a live slate + a frozen
    # book still trips the watchdog (last_book_change_ts logic untouched).
    from bot.core.feed_health import _stale_book_reconnect
    f = PolyUSOrderBookCache(sdk=None)
    f.prime("a", 0.40)
    f.retain_slugs({"a"})                                # 'a' kept (discovered)
    now = 10_000.0
    assert _stale_book_reconnect(now, now - 200.0, len(f._prices), 0.0, 180.0, 60.0) is True


def test_market_data_lite_update_fires_callback():
    cache = PolyUSOrderBookCache(sdk=None)
    fired = []
    cache.set_callback(lambda: fired.append(True))
    cache._on_market_data_lite(
        {"marketDataLite": {"marketSlug": "slug-x",
                            "bestAsk": {"value": "0.24", "currency": "USD"}}}
    )
    assert cache.get_best_ask("slug-x") == pytest.approx(0.24)
    assert fired == [True]


def test_no_callback_when_no_ask():
    cache = PolyUSOrderBookCache(sdk=None)
    fired = []
    cache.set_callback(lambda: fired.append(True))
    cache._on_market_data_lite({"marketDataLite": {"marketSlug": "slug-x", "bestAsk": None}})
    assert fired == []


@pytest.mark.asyncio
async def test_resubscribe_noop_when_not_connected():
    cache = PolyUSOrderBookCache(sdk=None)
    cache.prime("slug-x", 0.24)
    # _ws is None (never connected) → resubscribe must be a safe no-op, not raise.
    await cache.resubscribe()


def test_market_data_captures_best_ask_and_depth():
    """Full-book market_data: best ask = lowest offer; depth = total qty at it."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "slug-x",
        "offers": [
            {"px": {"value": "0.24", "currency": "USD"}, "qty": "100"},
            {"px": {"value": "0.24", "currency": "USD"}, "qty": "50"},   # same level
            {"px": {"value": "0.25", "currency": "USD"}, "qty": "999"},  # deeper, ignored
        ],
    }})
    assert cache.get_best_ask("slug-x") == pytest.approx(0.24)
    assert cache.get_depth("slug-x") == pytest.approx(150)  # 100 + 50 at best ask


def test_market_data_skips_a_malformed_level_not_the_whole_frame():
    """A variant/garbage level (a currency string where the price belongs — the venue really did
    send one) is SKIPPED per-level; the good levels still update the book."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "slug-x",
        "offers": [
            {"px": {"value": "USD", "currency": "USD"}, "qty": "100"},   # garbage: value is the currency
            {"px": {"value": "0.30", "currency": "USD"}, "qty": "40"},   # good → best ask
            {"px": {"value": "0.31", "currency": "USD"}, "qty": "999"},  # good, deeper
        ],
    }})
    assert cache.get_best_ask("slug-x") == pytest.approx(0.30)  # bad level dropped, not the frame
    assert cache.get_depth("slug-x") == pytest.approx(40)


def test_market_data_all_levels_malformed_keeps_last_known_book():
    """If NO level parses, the frame is dropped and the last-known book is retained — never
    torn/cleared (and the socket-alive stamp is still set before the parse)."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "slug-y",
        "offers": [{"px": {"value": "0.42", "currency": "USD"}, "qty": "7"}],
    }})
    assert cache.get_best_ask("slug-y") == pytest.approx(0.42)
    cache._on_market_data({"marketData": {   # all-garbage follow-up must not wipe it
        "marketSlug": "slug-y",
        "offers": [{"px": {"value": "USD", "currency": "USD"}, "qty": "USD"}],
    }})
    assert cache.get_best_ask("slug-y") == pytest.approx(0.42)  # unchanged, not cleared


def test_short_token_prices_as_one_minus_bid():
    """Moneyline short side: ask = 1 − bestBid, depth = bid depth. One slug, two sides."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "aec-mlb-tor-bos",
        "offers": [{"px": {"value": "0.48", "currency": "USD"}, "qty": "11"}],
        "bids":   [{"px": {"value": "0.47", "currency": "USD"}, "qty": "22"},
                   {"px": {"value": "0.46", "currency": "USD"}, "qty": "99"}],  # deeper, ignored
    }})
    # Long side unchanged.
    assert cache.get_best_ask("aec-mlb-tor-bos") == pytest.approx(0.48)
    assert cache.get_depth("aec-mlb-tor-bos") == pytest.approx(11)
    # Short side = 1 − best bid; depth = best-bid qty.
    assert cache.get_best_ask("aec-mlb-tor-bos::short") == pytest.approx(0.53)
    assert cache.get_depth("aec-mlb-tor-bos::short") == pytest.approx(22)


def test_get_best_bid_long_and_short():
    """Flatten price: long sells into bestBid; short sells into 1−bestAsk."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "aec-mlb-tor-bos",
        "offers": [{"px": {"value": "0.48", "currency": "USD"}, "qty": "11"}],
        "bids":   [{"px": {"value": "0.47", "currency": "USD"}, "qty": "22"}],
    }})
    assert cache.get_best_bid("aec-mlb-tor-bos") == pytest.approx(0.47)          # long → bid
    assert cache.get_best_bid("aec-mlb-tor-bos::short") == pytest.approx(0.52)   # short → 1−ask


def test_get_best_bid_none_when_no_quote():
    cache = PolyUSOrderBookCache(sdk=None)
    assert cache.get_best_bid("nope") is None


def test_short_token_none_when_no_bids():
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "aec-mlb-tor-bos",
        "offers": [{"px": {"value": "0.48", "currency": "USD"}, "qty": "11"}],
    }})
    assert cache.get_best_ask("aec-mlb-tor-bos::short") is None


def test_lite_update_captures_bid_for_short():
    """marketDataLite carries bestBid too → short side priceable from lite."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data_lite({"marketDataLite": {
        "marketSlug": "aec-mlb-tor-bos",
        "bestAsk": {"value": "0.48", "currency": "USD"},
        "bestBid": {"value": "0.47", "currency": "USD"},
    }})
    assert cache.get_best_ask("aec-mlb-tor-bos::short") == pytest.approx(0.53)


def test_prime_and_get_best_ask():
    cache = PolyUSOrderBookCache(sdk=None)
    cache.prime("slug-x", 0.24)
    assert cache.get_best_ask("slug-x") == pytest.approx(0.24)


def test_get_best_ask_unknown_returns_none():
    cache = PolyUSOrderBookCache(sdk=None)
    assert cache.get_best_ask("nope") is None


def test_get_age_returns_seconds():
    cache = PolyUSOrderBookCache(sdk=None)
    cache.prime("slug-x", 0.24)
    age = cache.get_age("slug-x")
    assert age is not None and 0 <= age < 5


def _md(slug, ask, qty):
    return {"marketData": {"marketSlug": slug,
                           "offers": [{"px": {"value": ask, "currency": "USD"}, "qty": qty}]}}


def test_book_change_counted_only_on_real_change():
    """The freeze disguise: identical re-sends keep the socket 'alive' but the book never
    moves. _note_book must count a CHANGE only when ask/depth/bid actually differ."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data(_md("slug-x", "0.24", "100"))   # first quote → change
    assert cache._book_changes == 1
    cache._on_market_data(_md("slug-x", "0.24", "100"))   # identical re-send → NOT a change
    cache._on_market_data(_md("slug-x", "0.24", "100"))   # still frozen → NOT a change
    assert cache._book_changes == 1
    cache._on_market_data(_md("slug-x", "0.25", "100"))   # price moved → change
    cache._on_market_data(_md("slug-x", "0.25", "80"))    # depth moved → change
    assert cache._book_changes == 3


class _FakeWS:
    def __init__(self):
        self.sent = []
    async def send(self, payload):
        self.sent.append(payload)


@pytest.mark.asyncio
async def test_raw_subscribe_sends_only_delta():
    import json
    cache = PolyUSOrderBookCache(sdk=None)
    cache._raw_ws = _FakeWS()
    await cache._send_subscribe_raw(["a", "b"])
    assert len(cache._raw_ws.sent) == 1
    await cache._send_subscribe_raw(["a", "b"])          # all already subscribed → no send
    assert len(cache._raw_ws.sent) == 1
    await cache._send_subscribe_raw(["a", "b", "c"])     # only "c" is new
    assert len(cache._raw_ws.sent) == 2
    assert json.loads(cache._raw_ws.sent[-1])["subscribe"]["marketSlugs"] == ["c"]


@pytest.mark.asyncio
async def test_raw_subscribe_clear_restores_full_resubscribe():
    # The reconnect invariant: clearing _subscribed (as run_forever does on every connect)
    # must cause the FULL set to be re-sent — else a reconnect leaves the feed dark.
    cache = PolyUSOrderBookCache(sdk=None)
    cache._raw_ws = _FakeWS()
    await cache._send_subscribe_raw(["a", "b"])
    cache._subscribed.clear()                            # simulate reconnect
    await cache._send_subscribe_raw(["a", "b"])
    assert len(cache._raw_ws.sent) == 2                  # re-sent, not skipped


@pytest.mark.asyncio
async def test_raw_subscribe_noop_without_socket():
    cache = PolyUSOrderBookCache(sdk=None)
    await cache._send_subscribe_raw(["a"])               # _raw_ws is None → no raise, no state
    assert cache._subscribed == set()


def test_health_log_resets_counter_and_is_throttled():
    """_maybe_log_health flushes the change count once per window; identical re-sends in a
    quiet/frozen window flush as 0 (the visible freeze signature)."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data(_md("slug-x", "0.24", "100"))
    assert cache._book_changes == 1
    cache._maybe_log_health()                 # first call always logs → resets counter
    assert cache._book_changes == 0
    cache._on_market_data(_md("slug-x", "0.24", "100"))   # frozen re-send → no change
    cache._maybe_log_health()                 # throttled (<60s) → counter untouched
    assert cache._book_changes == 0


def test_health_log_silent_within_first_window_after_connect(caplog):
    """Connect resets the health clock (_last_health_log = now), so the first 60s must
    NOT emit a frozen-feed warning — the startup warm-up false alarm this fixed. State
    must be untouched (early return), proving no log fired in-window."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._last_health_log = time.time()                 # what a fresh connect now sets
    cache._on_market_data(_md("slug-x", "0.24", "100"))  # one real change pending
    with caplog.at_level("WARNING"):
        cache._maybe_log_health()                        # ~0s into window → must stay quiet
    assert cache._book_changes == 1                      # window NOT rolled
    assert "frozen feed" not in caplog.text


def test_health_log_warns_on_zero_changes_after_full_window(caplog):
    """A genuine freeze still surfaces: a full 60s window with zero changes warns.
    Default _game_windows is None → _books_should_move True (armed) → in-window semantics."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._last_health_log = time.time() - 61            # a real window has elapsed
    with caplog.at_level("WARNING"):
        cache._maybe_log_health()
    assert cache._book_changes == 0                      # window rolled (log fired)
    assert "no book changes" in caplog.text


def test_health_log_no_warn_off_hours_zero_changes(caplog):
    """Off-hours (no game in-window) a quiet book is EXPECTED, not a freeze: 0 changes must NOT
    warn — same _books_should_move gate the freshness watchdog rests on. This kills the per-minute
    cry-wolf spam during quiet periods. Window still rolls (log fired at DEBUG)."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._last_health_log = time.time() - 61            # a real window has elapsed
    cache.set_game_windows([], unknown=False)            # no games, timing known → not armed
    with caplog.at_level("WARNING"):
        cache._maybe_log_health()
    assert cache._book_changes == 0                      # window rolled (log fired, just at DEBUG)
    assert "no book changes" not in caplog.text          # suppressed off-hours
    assert "frozen feed" not in caplog.text


def test_health_log_warns_in_window_zero_changes(caplog):
    """The dangerous direction stays loud: 0 changes WHILE a game is in-window is a real freeze
    and must warn even though windows are now set."""
    cache = PolyUSOrderBookCache(sdk=None)
    now = time.time()
    cache._last_health_log = now - 61
    cache.set_game_windows([(now - 100, now + 100)], unknown=False)   # a game is in-window
    with caplog.at_level("WARNING"):
        cache._maybe_log_health()
    assert "no book changes" in caplog.text


# ── two-timer split: connection-liveness (ping/pong) vs DATA-freshness/zombie watchdog ──────────

from bot.core.feed_health import _stale_book_reconnect


def test_stale_book_reconnect_branches():
    """The data-freshness/zombie decision (pure): reconnect iff a subscribed book has gone stale
    beyond the threshold AND we're past the anti-storm cooldown."""
    NOW = 1000.0
    # stale beyond threshold, slugs>0, past cooldown → reconnect
    assert _stale_book_reconnect(NOW, NOW - 200, 3, NOW - 100, 180, 60) is True
    # book changed within threshold → no reconnect (quiet ≠ zombie if it's still moving)
    assert _stale_book_reconnect(NOW, NOW - 100, 3, NOW - 100, 180, 60) is False
    # no subscribed slugs → never (nothing to expect — e.g. between-slate)
    assert _stale_book_reconnect(NOW, NOW - 9999, 0, 0.0, 180, 60) is False
    # within cooldown of the last forced reconnect → suppressed (anti-storm, no 45s-style loop)
    assert _stale_book_reconnect(NOW, NOW - 200, 3, NOW - 30, 180, 60) is False


def _md_book(*pairs):
    """marketData with MULTI-level offers from (price, qty) pairs (the existing _md is single-level)."""
    offers = [{"px": {"value": str(p), "currency": "USD"}, "qty": str(q)} for p, q in pairs]
    return {"marketData": {"marketSlug": "s", "offers": offers}}


def test_note_book_deep_churn_is_not_a_change():
    """The frozen-resend zombie catch rests on _note_book keying on the TRADEABLE top-of-book.
    Existing tests cover identical-resend / price-move / depth-move; this pins the subtle case the
    watchdog depends on: a frame with the SAME best ask + touch depth but a DIFFERENT deep level
    must NOT count as a change (deep churn can't mask a frozen tradeable price), while a real
    best-ask move must."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data(_md_book((0.24, 100), (0.25, 999)))    # best ask 0.24 @100
    assert cache._book_changes == 1
    cache._on_market_data(_md_book((0.24, 100), (0.26, 500)))    # same top, DIFFERENT deep level
    assert cache._book_changes == 1                              # deep churn ≠ change
    cache._on_market_data(_md_book((0.23, 100)))                 # real best-ask move
    assert cache._book_changes == 2


def test_note_book_freshness_ts_advances_only_on_change():
    """The freshness stamp the watchdog reads must NOT advance on an identical re-send (the zombie
    signature: frames arriving, book frozen)."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data(_md_book((0.24, 100)))
    t1 = cache._last_book_change_ts
    assert t1 > 0
    cache._on_market_data(_md_book((0.24, 100)))     # identical re-send
    assert cache._last_book_change_ts == t1          # NOT advanced
    cache._on_market_data(_md_book((0.23, 100)))     # real change
    assert cache._last_book_change_ts >= t1          # advanced


def test_full_market_data_retains_stats_and_state_and_prunes_them():
    """The full market_data frame carries `stats` (the flow-tape columns downstream analysis
    consumes) and market `state` — the arb path dropped both. They must be retained RAW
    (parse_book_stats computes SetTime ages at read-time) and pruned with _prices, or a WS-fed
    maker's flow tape blanks."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "slug-x",
        "offers": [{"px": {"value": "0.85"}, "qty": "100"}],
        "bids": [{"px": {"value": "0.84"}, "qty": "50"}],
        "state": "MARKET_STATE_OPEN",
        "transactTime": "2026-08-12T18:37:46.4Z",
        "stats": {"sharesTraded": "1234", "lastTradePx": {"value": "0.845"}},
    }})
    assert cache._stats["slug-x"]["sharesTraded"] == "1234"
    assert cache._states["slug-x"] == "MARKET_STATE_OPEN"
    assert cache._transact_times["slug-x"] == "2026-08-12T18:37:46.4Z"
    # prune: absent for the debounce window drops stats/state alongside the price
    for _ in range(cache._PRUNE_AFTER_ABSENT_CYCLES):
        cache.retain_slugs(set())
    assert "slug-x" not in cache._stats and "slug-x" not in cache._states


def test_full_market_data_without_stats_is_harmless():
    """A frame missing stats/state must not raise and must not fabricate an entry."""
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "y", "offers": [{"px": {"value": "0.3"}, "qty": "5"}]}})
    assert "y" not in cache._stats and "y" not in cache._states
    assert cache.get_best_ask("y") == pytest.approx(0.3)


def test_get_book_md_reads_exact_via_the_makers_own_parsers():
    """[WS-maker book feed] the getter feeds the maker's REAL parsers (touch_from_md,
    qty_at_price, parse_book_stats) and must read IDENTICALLY to a REST book. The load-bearing
    property is EXACT wire strings, not floats: two raw touch levels at 0.1 + 0.2 qty — the raw
    pass-through lets qty_at_price Decimal-sum to exactly 0.3, while a float-reconstruct getter
    (sum-then-str) would produce 0.30000000000000004. A float-reconstruct mutant of get_book_md
    goes RED here; str(float) of the on-grid PRICE round-trips, so the qty sum is what proves it."""
    from decimal import Decimal
    from bot.poly_us.client import touch_from_md, parse_book_stats
    from bot.poly_us.maker import qty_at_price
    cache = PolyUSOrderBookCache(sdk=None)
    cache._on_market_data({"marketData": {
        "marketSlug": "s",
        "offers": [{"px": {"value": "0.848", "currency": "USD"}, "qty": "40"}],
        "bids": [{"px": {"value": "0.847", "currency": "USD"}, "qty": "0.1"},
                 {"px": {"value": "0.847", "currency": "USD"}, "qty": "0.2"}],   # same touch price
        "state": "MARKET_STATE_OPEN",
        "transactTime": "2026-08-12T18:00:00.0Z",
        "stats": {"sharesTraded": "999"},
    }})
    md = cache.get_book_md("s")
    bid, ask, tob = touch_from_md(md)
    assert bid == Decimal("0.847") and ask == Decimal("0.848"), "EXACT price"
    assert tob == Decimal("0.3"), "Decimal-summed touch depth, not a float 0.30000…4"
    assert qty_at_price(md, "bid", Decimal("0.847")) == Decimal("0.3")
    assert qty_at_price(md, "bid", Decimal("0.848")) == Decimal("0")   # improved rests alone
    assert parse_book_stats(md, now=1.0)["shares_traded"] == 999.0


def test_get_book_md_is_None_before_a_book_arrives():
    cache = PolyUSOrderBookCache(sdk=None)
    assert cache.get_book_md("never-seen") is None
