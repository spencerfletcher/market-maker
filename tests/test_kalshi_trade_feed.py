"""The Kalshi trade-tape feed's SILENT-FAILURE surfaces.

This module exists so an adverse-selection question can be answered from real prints. Its dangerous
failure is not a crash — it is producing nothing while looking healthy. Kalshi's tape is genuinely
sparse (~1 print/40s across 60 tickers), so "no trades" is a plausible real outcome and is therefore
indistinguishable from a rejected subscription unless the ack is captured. That exact confusion has
already cost this repo two real-money runs (`quotes=0 fills=0`, WS logging "subscribed" throughout).
"""
import asyncio
import json
from decimal import Decimal as D

import pytest

from bot.kalshi.trade_feed import KalshiTradeFeed, _dec, _epoch_s, aggressor_from_mid


def _feed(sink=None):
    return KalshiTradeFeed(client=object(), tickers=["KXA", "KXB"],
                           on_trade=(sink if sink is not None else (lambda e: None)))


def _run(coro):
    return asyncio.run(coro)


# ── subscription state: the blocker ──────────────────────────────────────────────────────────────

def test_a_feed_does_not_claim_to_be_subscribed_until_the_venue_acks():
    f = _feed()
    assert f.subscribed is False


def test_the_ack_sets_subscribed():
    f = _feed()
    _run(f._handle(json.dumps({"type": "subscribed"})))
    assert f.subscribed is True


def test_a_venue_error_is_recorded_LOUDLY_and_does_not_set_subscribed():
    """A rejected subscribe leaves the socket open forever. Without this the only symptom is a tape
    that never prints — identical to a quiet market."""
    f = _feed()
    _run(f._handle(json.dumps({"type": "error", "msg": {"code": 6, "msg": "invalid ticker"}})))
    assert f.subscribed is False
    assert f.last_error and "invalid ticker" in f.last_error
    assert sum(f.errors.values()) == 1


def test_zero_trades_with_no_ack_is_distinguishable_from_a_quiet_market():
    """The whole point: two states that used to look identical must now be tellable apart."""
    quiet = _feed()
    _run(quiet._handle(json.dumps({"type": "subscribed"})))
    assert (quiet.subscribed, quiet.n_trades) == (True, 0)      # genuinely quiet

    broken = _feed()
    _run(broken._handle(json.dumps({"type": "error", "msg": "nope"})))
    assert (broken.subscribed, broken.n_trades) == (False, 0)   # never subscribed


# ── trade parsing ────────────────────────────────────────────────────────────────────────────────

def test_a_trade_is_normalised_and_counted():
    got = []
    f = _feed(got.append)
    _run(f._handle(json.dumps({"type": "trade", "msg": {
        "market_ticker": "KXA", "yes_price_dollars": "0.4300",
        "count_fp": "2.00", "ts": 1784502017}})))
    assert f.n_trades == 1
    e = got[0]
    assert e["ticker"] == "KXA"
    assert e["yes_price"] == D("0.4300")     # exact, from the string form
    assert e["qty"] == D("2.00")
    assert e["ts"] == 1784502017.0
    assert e["raw"]["market_ticker"] == "KXA"


def test_a_renamed_price_field_is_COUNTED_not_silently_ignored():
    """If the venue renames yes_price_dollars, `n_trades=0 n_dropped=large` reads as 'the schema
    moved'. Silently returning would read as 'the market was quiet'."""
    f = _feed()
    _run(f._handle(json.dumps({"type": "trade", "msg": {
        "market_ticker": "KXA", "price_in_cents": 43, "count_fp": "1.00"}})))
    assert f.n_trades == 0
    assert f.n_dropped == 1
    # The NARROW counter too: this frame WAS a trade and could not be used, which is a lost print.
    # It is the one the maker's queue attribution reads, and `n_dropped` is unusable for that
    # because it also counts unrecognised control frames.
    assert f.n_bad_trades == 1


def test_unparseable_size_is_None_and_not_a_zero_print():
    """Collapsing a parse failure into 0 makes any volume-weighted statistic silently read no size."""
    got = []
    f = _feed(got.append)
    _run(f._handle(json.dumps({"type": "trade", "msg": {
        "market_ticker": "KXA", "yes_price_dollars": "0.43", "count_fp": "??"}})))
    assert got[0]["qty"] is None
    assert _dec("0") == D(0)                 # a genuine zero still parses to zero
    # ⚠️ AND IT COUNTS AS A LOST PRINT. Every consumer drops a qty-less trade, so uncounted this
    # gives `subscribed=True, n_reconnects=0, n_bad_trades=0` and a climbing `n_trades` while the
    # queue attribution accrues nothing — all health flags clean, every fill scoring as a queue that
    # never traded. A total rename is backstopped by the own-fill completeness guard; an
    # INTERMITTENT parse failure is not.
    assert f.n_bad_trades == 1


def test_a_raising_callback_is_counted_and_does_not_kill_the_tape():
    def boom(_e):
        raise ValueError("callback exploded")
    f = _feed(boom)
    msg = json.dumps({"type": "trade", "msg": {
        "market_ticker": "KXA", "yes_price_dollars": "0.43", "count_fp": "1"}})
    _run(f._handle(msg))
    _run(f._handle(msg))
    assert f.n_trades == 2                   # the feed kept going
    assert sum(f.errors.values()) == 2       # and said so
    # ⚠️ AND SEPARATELY as a LOST PRINT. `errors` also accrues reconnect exceptions and venue
    # `error` frames, so a consumer that treats it as print-loss would mark every order on every
    # poll during an error storm. `n_callback_errors` is the narrow signal the maker's queue
    # attribution can safely act on: the tape delivered these and the consumer dropped them.
    assert f.n_callback_errors == 2


# ── timestamps: same hardening as the MM tool's _fill_ts ─────────────────────────────────────────

def test_millisecond_epochs_are_rescaled_not_taken_at_face_value():
    """A ms epoch read literally lands ~55,000 years out, making every markout horizon instantly
    'due' and emitting a full set of mk=0 rows that look like measurements."""
    assert _epoch_s(1784502017000) == 1784502017.0


def test_implausible_or_junk_timestamps_become_None():
    for bad in ("nope", None, 1, -1, 5e9):
        assert _epoch_s(bad) is None


# ── aggressor inference ──────────────────────────────────────────────────────────────────────────

def test_a_print_exactly_at_the_mid_is_UNKNOWN_not_a_guess():
    """1,646 of 30,894 rows (5.3%) on the real Kalshi tape sit exactly at the mid. `>=` assigned
    every one to offer_hit, which does not add noise — it flips the SIGN of that observation's
    markout, one way, across 5% of the population deciding maker viability."""
    assert aggressor_from_mid(D("0.50"), D("0.50")) == "unknown"


def test_prints_away_from_the_mid_are_classified():
    assert aggressor_from_mid(D("0.51"), D("0.50")) == "offer_hit"
    assert aggressor_from_mid(D("0.49"), D("0.50")) == "bid_hit"


def test_non_trade_messages_are_counted_as_dropped():
    f = _feed()
    _run(f._handle(json.dumps({"type": "orderbook_delta", "msg": {}})))
    assert f.n_dropped == 1
    assert f.n_trades == 0


def test_malformed_json_is_survivable():
    f = _feed()
    _run(f._handle("{not json"))
    assert f.n_trades == 0


# ── the guard must survive a reconnect, and must work for a float-holding caller ─────────────────

def test_subscribed_is_reset_before_every_resubscribe():
    """Carrying True across a reconnect defeats the whole guard: acked at t=0, socket drops, the
    RE-subscribe is rejected, the tape goes silent — and the flag still says healthy. Reconnects are
    the normal case on a long run, not an edge case."""
    src = (__import__("inspect").getsource(KalshiTradeFeed.run_forever))
    assert "self.subscribed = False" in src, "run_forever must reset the flag before subscribing"
    i_reset = src.index("self.subscribed = False")
    i_send = src.index("ws.send(")
    assert i_reset < i_send, "the reset must happen BEFORE the subscribe is sent"


def test_a_float_mid_still_gets_the_unknown_tie_branch():
    """`maker.MakerSession.mid` returns a float, and Decimal('0.43') == 0.43 is False — so without
    coercion the tie branch never fires for the only realistic caller, silently restoring the 5.3%
    sign flip while the Decimal-only tests stay green."""
    assert aggressor_from_mid(D("0.50"), 0.50) == "unknown"
    assert aggressor_from_mid(D("0.51"), 0.50) == "offer_hit"
    assert aggressor_from_mid(D("0.49"), 0.50) == "bid_hit"


def test_an_unusable_mid_is_unknown_rather_than_a_coin_flip():
    assert aggressor_from_mid(D("0.50"), None) == "unknown"
    assert aggressor_from_mid(D("0.50"), float("nan")) == "unknown"
