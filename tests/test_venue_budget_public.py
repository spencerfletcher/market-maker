"""The shared venue request budget: bucket depth, priority order and the ban — on a tmp state file."""
import pytest

from bot.core import venue_budget as vb


@pytest.fixture
def state(tmp_path) -> str:
    return str(tmp_path / "venue_budget_default.json")


def test_burst_is_the_bucket_depth_then_the_refill_rate_governs(state):
    now = 1_000_000.0
    taken = 0
    while vb.try_take(vb.PRIO_OPERATOR, path=state, now=now, pid=1):
        taken += 1
        assert taken <= vb.VENUE_BUDGET_BURST + 1
    assert taken == vb.VENUE_BUDGET_BURST
    one_token_s = 1.0 / vb.VENUE_BUDGET_REQ_PER_S
    assert vb.try_take(vb.PRIO_OPERATOR, path=state, now=now + one_token_s * 0.9, pid=1) is False
    assert vb.try_take(vb.PRIO_OPERATOR, path=state, now=now + one_token_s * 1.1, pid=1) is True


def test_a_waiting_maker_outranks_a_collector_that_asked_first(state):
    now = 1_000_000.0
    for _ in range(vb.VENUE_BUDGET_BURST):
        assert vb.try_take(vb.PRIO_OPERATOR, path=state, now=now, pid=1)
    assert vb.try_take(vb.PRIO_COLLECTOR, path=state, now=now, pid=2) is False   # registers as waiter
    assert vb.try_take(vb.PRIO_MAKER, path=state, now=now, pid=3) is False       # registers, outranks
    later = now + 1.0 / vb.VENUE_BUDGET_REQ_PER_S * 1.1                          # one token refilled
    assert vb.try_take(vb.PRIO_COLLECTOR, path=state, now=later, pid=2) is False  # maker is waiting
    assert vb.try_take(vb.PRIO_MAKER, path=state, now=later, pid=3) is True


def test_a_ban_refuses_every_class_until_it_lifts(state):
    now = 1_000_000.0
    vb.record_ban("429", path=state, now=now)
    until = vb.ban_active(path=state, now=now)
    assert until is not None and until > now
    with pytest.raises(vb.VenueBanned):
        vb.try_take(vb.PRIO_MAKER, path=state, now=now + 1.0, pid=1)
    assert vb.ban_active(path=state, now=until + 1.0) is None
