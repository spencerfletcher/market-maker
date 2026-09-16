"""Pins Decimal Phase 3b-lite: the loss-cap accumulators are EXACT.

These two functions are the only place in the codebase where money ACCUMULATES over an unbounded
number of rows and the running total is then compared against a cap. That is the one shape where
float drift can actually flip a safety decision: summing 1000 rows of <n> in float yields
9.999999999999831, so `total > 10.00` is False when the true total is exactly the limit.

Everything else Phases 0-3a touched was amplification-at-a-threshold (ceil/floor/compare-to-zero);
this is the accumulation case, and it is why 3b-lite was worth doing even though the remaining
plain-arithmetic sites were not.
"""
import datetime as _dt
from decimal import Decimal

from bot.core import safety

# ⚠️ These fixtures were hard-dated "2026-07-19". `is_daily_loss_cap_hit()` counts only TODAY's rows,
# so the two cap tests passed all of 2026-07-19 and went red the moment UTC rolled to 07-20 — with
# nothing having changed. A safety-net test that fails every midnight is worse than no test: it
# trains you to wave through a red suite on precisely the check you least want ignored. The date is
# now computed at test time, so the fixture always describes "today" wherever and whenever it runs.
TODAY = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
TS = f"{TODAY}T00:00:00Z"


def _write(tmp_path, monkeypatch, rows):
    p = tmp_path / "execution_pnl.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        f.write("timestamp,bucket,kind,event,ticker,qty,amount,note\n")
        for ts, bucket, amount in rows:
            f.write(f"{ts},{bucket},k,E,T,8,{amount},n\n")
    monkeypatch.setattr(safety, "_EXEC_PNL_PATH", str(p))
    return p


def test_float_accumulation_of_this_shape_really_does_drift():
    """The premise, demonstrated — not assumed."""
    f = 0.0
    for _ in range(1000):
        f += 0.01
    assert f != 10.0 and f == 9.999999999999831
    assert sum([Decimal("0.01")] * 1000) == Decimal("10.00")


def test_thousand_one_cent_losses_sum_to_exactly_ten(tmp_path, monkeypatch):
    """1000 x <n> of execution_cost is EXACTLY <n> — in float it was 9.999999999999831, which
    reads as under a <n> cap when it is precisely at it."""
    _write(tmp_path, monkeypatch,
           [(TS, "execution_cost", "0.01") for _ in range(1000)])
    assert safety.cumulative_realized_loss() == 10.0
    assert safety.daily_realized_loss(TODAY) == 10.0


def test_cap_fires_at_the_exact_boundary(tmp_path, monkeypatch):
    """The decision this protects: at exactly the limit the cap must NOT fire (strictly greater),
    and one more cent must trip it. Float drift made the boundary itself unreliable."""
    rows = [(TS, "execution_cost", "0.01") for _ in range(1000)]
    _write(tmp_path, monkeypatch, rows)
    monkeypatch.setattr(safety.config, "DAILY_LOSS_LIMIT", 10.0)
    assert safety.is_daily_loss_cap_hit() is False        # exactly at the limit → not hit

    _write(tmp_path, monkeypatch, rows + [(TS, "execution_cost", "0.01")])
    assert safety.is_daily_loss_cap_hit() is True         # one cent over → hit


def test_signed_realized_settled_counts_only_the_loss_side(tmp_path, monkeypatch):
    """Bucket semantics must survive the conversion: execution_cost is loss-positive, while
    realized_settled is signed P&L and only its negative side is a loss."""
    _write(tmp_path, monkeypatch, [
        (TS, "execution_cost", "1.50"),      # loss 1.50
        (TS, "realized_settled", "-2.25"),   # loss 2.25
        (TS, "realized_settled", "9.99"),    # a WIN -> contributes 0
    ])
    assert safety.cumulative_realized_loss() == 3.75


def test_nonfinite_amount_FIRES_the_cap_rather_than_being_skipped(tmp_path, monkeypatch):
    """Fail-CLOSED, and it fixes both float-era defects.

    Infinity: `inf > budget` used to halt trading. Silently skipping the row (an earlier version of
    this guard) would let a real unrecorded loss through — a direction regression.
    NaN: `nan > budget` is False, so one NaN row SILENTLY DISABLED the cap; under Decimal,
    max(0, NaN) RAISES out of the 5s halt check. Both are unusable, so assume the worst."""
    for bad in ("Infinity", "NaN", "-Infinity"):
        _write(tmp_path, monkeypatch, [
            (TS, "execution_cost", bad),
            (TS, "execution_cost", "2.00"),
        ])
        assert safety.cumulative_realized_loss() == float("inf"), bad
        monkeypatch.setattr(safety.config, "DAILY_LOSS_LIMIT", 5.0)
        assert safety.is_daily_loss_cap_hit() is True, bad      # halts, does not sail past


def test_a_clean_file_still_does_not_trip(tmp_path, monkeypatch):
    """Control for the above — the fail-closed path must not fire on ordinary data."""
    _write(tmp_path, monkeypatch, [(TS, "execution_cost", "2.00")])
    monkeypatch.setattr(safety.config, "DAILY_LOSS_LIMIT", 5.0)
    assert safety.is_daily_loss_cap_hit() is False


def test_unparseable_amount_is_skipped_not_fatal(tmp_path, monkeypatch):
    _write(tmp_path, monkeypatch, [
        (TS, "execution_cost", "not-a-number"),
        (TS, "execution_cost", "3.00"),
    ])
    assert safety.cumulative_realized_loss() == 3.0
