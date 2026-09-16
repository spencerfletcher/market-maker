"""Book P&L off the ACTUAL fill, not the detection price (S3).

_record_hedge books `opp.poly_ask`/`opp.kalshi_ask` — the DETECTION-time effective costs — and
writes that into trades.log's `cost`. settlement_scorer.py:129 reads that same `cost` to compute
realized_settled, which CLAUDE.md calls "the INTENDED sizing authority" and which the cumulative
loss cap reads. So a fill worse than detection doesn't merely flatter marked_unsettled; it corrupts
the number that will authorize scaling up.

Both venues report the real cost exactly — price AND fee — so no modelling is needed:
  Kalshi V2 create: average_fill_price + average_fee_paid   [real prod fixture]
  Poly create:      avgPx + commissionNotionalTotalCollected [real prod fixture]
"""
import pytest

from bot.kalshi.client import kalshi_avg_fill_cost, kalshi_avg_sell_proceeds, kalshi_fee_paid
from bot.kalshi.fees import _kalshi_taker_fee
from bot.poly_us.client import poly_avg_fill_cost


# The REAL prod V2 create response (1 YES @ 0.4270, fee <n>) — same fixture as
# tests/test_kalshi_v2_orders.py. Not invented.
REAL_V2_CREATE = {
    "order_id": "94cb58b9-003f-481e-f4c6-f0699a394bd7", "fill_count": "1.00",
    "remaining_count": "0.00", "average_fill_price": "0.4270",
    "average_fee_paid": "0.0172", "ts_ms": 1784063018859,
}


def test_kalshi_actual_cost_is_price_plus_the_REAL_fee():
    """0.4270 + 0.0172/1 = 0.4442 — the exchange's own numbers, no fee model involved."""
    assert kalshi_avg_fill_cost(REAL_V2_CREATE, "yes") == pytest.approx(0.4442)


def test_kalshi_fee_basis_is_DETECTED_not_assumed():
    """average_fee_paid's basis was unresolvable read-only, so we DON'T guess — we DETECT. [RESOLVED
    2026-07-17: a demo fill_count=2 fill measured it PER-CONTRACT; detection kept for robustness.]

    It cannot be measured read-only — the field exists on the create response and nowhere else
    (/portfolio/fills has fee_cost, /portfolio/orders has taker_fees_dollars; neither is this).
    Our only real fixture is fill_count=1, where the two bases are arithmetically IDENTICAL, so
    the measurement that "verified" this could never have discriminated. The predecessor of this
    test asserted per-ORDER using `4 @ 0.50 → fee 0.0400` — a value the venue cannot produce (the
    real order total is 0.0700, the real per-contract 0.0175). It pinned the code's own arithmetic
    against an impossible world.

    The fee FORMULA is verified to the centicent against 7 real fills, and the two candidates
    differ by a factor of fill_count, so the response identifies itself. The point of this test:
    **fed a REAL fee, both bases yield the same cost**, so the answer no longer depends on the
    guess. Getting it wrong is not cheap either way — reading per-contract as a total understates
    by ~1.5c/share at the 8-share ramp (77% of the minimum edge, flattering realized_settled),
    and the mirror overstates by ~12c/share, which would trip the cumulative loss cap.
    """
    per_contract_fee = _kalshi_taker_fee(0.50, 10)          # 0.0175
    order_total_fee = per_contract_fee * 10                 # 0.1750

    as_per_contract = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                           average_fee_paid=f"{per_contract_fee:.6f}")
    as_order_total = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                          average_fee_paid=f"{order_total_fee:.6f}")

    assert kalshi_avg_fill_cost(as_per_contract, "yes") == pytest.approx(0.5175)
    assert kalshi_avg_fill_cost(as_order_total, "yes") == pytest.approx(0.5175)


def test_kalshi_fee_matching_neither_basis_refuses_to_price_the_leg():
    """A fee that is neither candidate means the schedule moved or the field changed. Returning a
    number would price a real position off arithmetic we no longer understand; None makes the
    caller fall back explicitly. This is what caught the two fabricated fixtures in this suite."""
    impossible = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                      average_fee_paid="0.1000")   # neither 0.0175 nor 0.1750
    assert kalshi_avg_fill_cost(impossible, "yes") is None


def test_kalshi_unreadable_returns_None_never_a_price():
    """None = "use the caller's fallback". A 0.0 here would book a FREE fill — the most
    flattering possible lie about a leg we actually paid for."""
    assert kalshi_avg_fill_cost({"fill_count": "0.00"}, "yes") is None
    assert kalshi_avg_fill_cost({"average_fill_price": "0.4"}, "yes") is None      # no fill_count
    assert kalshi_avg_fill_cost({"fill_count": "1.00"}, "yes") is None             # no price
    assert kalshi_avg_fill_cost(None, "yes") is None
    assert kalshi_avg_fill_cost({"fill_count": "1.00", "average_fill_price": "x"}, "yes") is None


def test_kalshi_missing_fee_is_unreadable_not_free():
    """A price with no fee must NOT book as fee-free — that silently under-costs the leg, the
    exact failure the Poly fee fallback exists to prevent ("NEVER 0")."""
    assert kalshi_avg_fill_cost({"fill_count": "1.00", "average_fill_price": "0.4270"}, "yes") is None


def test_kalshi_fee_paid_is_the_DISCRETE_total_dollar_fee():
    """The ledger/tax companion to kalshi_avg_fill_cost (which folds the fee into a per-contract
    COST). Real prod fill (1 @ 0.4270, fee 0.0172) → total dollar fee 0.0172."""
    assert kalshi_fee_paid(REAL_V2_CREATE) == pytest.approx(0.0172)


def test_kalshi_fee_paid_resolves_the_basis_both_ways_to_the_same_total():
    """Same unknown-basis resolution as the cost reader (_kalshi_px_and_fee): whether the venue
    reports average_fee_paid per-contract or per-order, the TOTAL is per_contract × fill."""
    per_contract_fee = _kalshi_taker_fee(0.50, 10)          # 0.0175
    total = per_contract_fee * 10                           # 0.1750
    as_per_contract = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                           average_fee_paid=f"{per_contract_fee:.6f}")
    as_order_total = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                          average_fee_paid=f"{total:.6f}")
    assert kalshi_fee_paid(as_per_contract) == pytest.approx(total)
    assert kalshi_fee_paid(as_order_total) == pytest.approx(total)


def test_kalshi_fee_paid_none_when_unreadable_or_basis_unknown():
    """Never 0.0 on a real fill (that books a free fee); None → the ledger records a blank, never
    a fabricated zero."""
    assert kalshi_fee_paid(None) is None
    assert kalshi_fee_paid({"fill_count": "0.00"}) is None
    assert kalshi_fee_paid({"fill_count": "1.00", "average_fill_price": "0.4270"}) is None  # no fee
    impossible = dict(REAL_V2_CREATE, fill_count="10.00", average_fill_price="0.5000",
                      average_fee_paid="0.1000")   # neither 0.0175 nor 0.1750
    assert kalshi_fee_paid(impossible) is None


def _poly_resp(cum, avg_px, commission):
    return {"executions": [{"order": {
        "cumQuantity": cum, "avgPx": {"value": avg_px, "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": commission, "currency": "USD"}}}]}


def test_poly_actual_cost_is_avgpx_plus_the_REAL_commission():
    """255 @ 0.0090 with <n> commission — the real 2026-07-15 probe fill.
    0.0090 + 0.1400/255 = 0.009549."""
    assert poly_avg_fill_cost(_poly_resp(255, "0.0090", "0.1400"), is_short=False) == pytest.approx(0.009549, abs=1e-6)


def test_poly_reads_the_execution_that_actually_FILLED():
    """executions[0] LIES (verified: cumQuantity 0 on an order that filled 255). Take the max-fill
    execution, same rule as order_filled_qty."""
    r = {"executions": [
        {"order": {"cumQuantity": 0, "avgPx": {"value": "0"},
                   "commissionNotionalTotalCollected": {"value": "0"}}},
        {"order": {"cumQuantity": 255, "avgPx": {"value": "0.0090"},
                   "commissionNotionalTotalCollected": {"value": "0.1400"}}},
    ]}
    assert poly_avg_fill_cost(r, is_short=False) == pytest.approx(0.009549, abs=1e-6)


def test_poly_unreadable_returns_None_never_a_price():
    assert poly_avg_fill_cost({"executions": []}, is_short=False) is None
    assert poly_avg_fill_cost(None, is_short=False) is None
    assert poly_avg_fill_cost(_poly_resp(0, "0.5", "0.1"), is_short=False) is None            # nothing filled
    assert poly_avg_fill_cost({"executions": [{"order": {"cumQuantity": 5}}]}, is_short=False) is None  # no px


# ── S3's other half: the UNWIND paths ─────────────────────────────────────────────────────────
# S3 fixed _record_hedge to book the venues' own fill reports and stopped there. The unwinds kept
# booking detection prices into execution_pnl.csv, which safety.is_exec_cost_cap_hit reads — so
# the lifetime loss ratchet was fed an optimistic number.

def test_kalshi_sell_proceeds_NET_the_fee_they_do_not_add_it():
    """A buy PAYS the fee, a sell NETS it. Reading a sell with the buy helper would report
    proceeds two fees too high and make every unwind look cheaper than it was."""
    from bot.kalshi.client import kalshi_avg_sell_proceeds
    fee_pc = _kalshi_taker_fee(0.40, 10)
    resp = {"fill_count": "10.00", "average_fill_price": "0.4000",
            "average_fee_paid": f"{fee_pc * 10:.6f}"}          # reported as an order total
    assert kalshi_avg_sell_proceeds(resp, "yes") == pytest.approx(0.40 - fee_pc)
    assert kalshi_avg_fill_cost(resp, "yes") == pytest.approx(0.40 + fee_pc)
    # ...and the gap between them is exactly two fees — what the mistake would have cost.
    assert kalshi_avg_fill_cost(resp, "yes") - kalshi_avg_sell_proceeds(resp, "yes") == pytest.approx(2 * fee_pc)


def test_kalshi_sell_proceeds_resolve_the_fee_basis_like_the_buy_side():
    """Same unknown basis, same resolver — answering it twice would let the two drift."""
    from bot.kalshi.client import kalshi_avg_sell_proceeds
    fee_pc = _kalshi_taker_fee(0.40, 10)
    as_pc = {"fill_count": "10.00", "average_fill_price": "0.4000",
             "average_fee_paid": f"{fee_pc:.6f}"}
    as_total = {"fill_count": "10.00", "average_fill_price": "0.4000",
                "average_fee_paid": f"{fee_pc * 10:.6f}"}
    assert kalshi_avg_sell_proceeds(as_pc, "yes") == pytest.approx(kalshi_avg_sell_proceeds(as_total, "yes"))
    # neither basis → refuse, same as the buy side
    assert kalshi_avg_sell_proceeds(
        {"fill_count": "10.00", "average_fill_price": "0.4000",
         "average_fee_paid": "0.1000"}, "yes") is None


# ── NO-leg fills come back in YES-space on the wire and MUST be complemented (demo-caught 2026-07-17)
# Real fixtures captured by scripts/kalshi_demo_probe, NOT invented: a NO buy at no-ask 0.99 came back
# average_fill_price 0.0100 (the YES complement); the NO flatten at no-bid 0.02 came back 0.9800.
# Before the side-aware fix, kalshi_avg_fill_cost booked 0.0107 for a contract that cost ~0.99 —
# hiding real NO-leg losses from the loss cap.
_DEMO_NO_BUY = {"order_id": "ede6ccd9", "fill_count": "2.00", "remaining_count": "0.00",
                "average_fill_price": "0.0100", "average_fee_paid": "0.0007"}
_DEMO_NO_SELL = {"order_id": "bfdb0505", "fill_count": "2.00", "remaining_count": "0.00",
                 "average_fill_price": "0.9800", "average_fee_paid": "0.0014"}


def test_kalshi_NO_leg_cost_is_complemented_to_no_space():
    """side='no': the real per-contract cost is (1 − 0.01) + fee ≈ 0.9907, NOT the wire's 0.0107 —
    the bug that would have booked a ~0.99 NO leg as ~0.01 and hidden the loss from the cap."""
    assert kalshi_avg_fill_cost(_DEMO_NO_BUY, "no") == pytest.approx(0.9907, abs=1e-4)
    # side='yes' is the raw wire interpretation (what the old, side-blind reader always returned)
    assert kalshi_avg_fill_cost(_DEMO_NO_BUY, "yes") == pytest.approx(0.0107, abs=1e-4)


def test_kalshi_NO_leg_sell_proceeds_complemented():
    """Selling NO at 0.02 nets ~0.02 − fee ≈ 0.0186, NOT the wire's 0.98 − fee ≈ 0.9786."""
    assert kalshi_avg_sell_proceeds(_DEMO_NO_SELL, "no") == pytest.approx(0.0186, abs=1e-4)
    assert kalshi_avg_sell_proceeds(_DEMO_NO_SELL, "yes") == pytest.approx(0.9786, abs=1e-4)


# ⛔ REMOVED 2026-09-04 — test_kalshi_effective_is_NOT_opp_kalshi_ask pinned
# `bot.runner.kalshi_arb._kalshi_effective`, deleted with the cross-arb bot
# (archive branch archive/arb-bot-2026-09-04).


# ── SHORT intents: avgPx is in YES space, so the cost is NOT avgPx + fee ──────────────────────

def _poly_short_resp(cum, avg_px, commission):
    """The REAL 2026-07-28 order-5 shape: a filled BUY_SHORT, with the venue's own `intent`."""
    return {"executions": [{"order": {
        "cumQuantity": cum, "intent": "ORDER_INTENT_BUY_SHORT",
        "avgPx": {"value": avg_px, "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": commission, "currency": "USD"}}}]}


def test_poly_short_cost_is_the_COMPLEMENT_of_avgpx_plus_commission():
    """⛔ THE CAPTURED ORDER-5 RESPONSE, not a literal.

    Real fill: BUY_SHORT, 5 shares, `avgPx` 0.8490, commission <n> — and the USDC balance moved
    <n>, i.e. <n>/share. `avgPx` is in YES space even for a short intent
    (the private design notes), so the short's cost is the cost of the NO
    side, `(1 - 0.8490) + 0.0400/5 = 0.159`. Returning `avgPx + fee` books 0.857 — a 5x
    overstatement that drives `guaranteed_profit` negative on every short leg.
    """
    got = poly_avg_fill_cost(_poly_short_resp(5, "0.8490", "0.0400"), is_short=True)
    assert got == pytest.approx(0.159, abs=1e-6)
    # and it must be nowhere near the long-formula answer
    assert got != pytest.approx(0.8490 + 0.0400 / 5, abs=1e-3)
    # CORROBORATION, not an identity: the USDC balance moved <n> over this fill, i.e.
    # <n>/share against the formula's <n>. The 0.2c/share residual is unexplained — the
    # balance delta is not isolated to this order and may carry other activity — so this asserts
    # only that the formula lands in the right NEIGHBOURHOOD, which is enough to separate 0.159
    # from the long formula's 0.857. Do not tighten it into a claim the evidence cannot carry.
    assert got == pytest.approx(0.785 / 5, abs=1e-2)


def test_poly_long_cost_is_unchanged_by_the_short_awareness():
    """The short branch must leave every long fill alone — including one carrying an explicit long
    intent, where the venue agrees with the caller and the guard has to stay quiet.

    ⛔ Said "must key off the venue's own `intent`", which is now the opposite of the rule: round 3
    keyed off `intent` and that reopened the invisible-loss bug through a missing field. The side
    comes from the CALLER's token; `intent` only ever vetoes. [round 5]"""
    assert poly_avg_fill_cost(_poly_resp(255, "0.0090", "0.1400"), is_short=False) == pytest.approx(0.009549, abs=1e-6)
    long_resp = {"executions": [{"order": {
        "cumQuantity": 255, "intent": "ORDER_INTENT_BUY_LONG",
        "avgPx": {"value": "0.0090", "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": "0.1400", "currency": "USD"}}}]}
    assert poly_avg_fill_cost(long_resp, is_short=False) == pytest.approx(0.009549, abs=1e-6)


def test_poly_REFUSES_a_disposal_rather_than_pricing_it():
    """A disposal has no defined value under this function's contract ("cost per share of the side
    we BOUGHT"), and every caller reaches it from a BUY-only path, so a SELL_* intent IS a
    contradiction.

    ⛔ Two earlier versions got this wrong in opposite directions. The first returned `+fee` and
    pinned it with commission "0.0000" — the ONE value at which the sign is invisible. The second
    flipped the sign to `-fee`, which returns a PROCEEDS quantity under a cost-shaped name with no
    signal to the caller, on a fee direction that was modelled and never measured. Refusing is the
    only answer the evidence supports."""
    r = {"executions": [{"order": {
        "cumQuantity": 2, "intent": "ORDER_INTENT_SELL_SHORT",
        "avgPx": {"value": "0.8530", "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": "0.0200", "currency": "USD"}}}]}
    assert poly_avg_fill_cost(r, is_short=True) is None


def test_poly_an_UNRECOGNISED_intent_is_no_information_not_a_contradiction():
    """A suffix test calls anything unrecognised "long", so a proto3 zero value or a renamed enum
    would make EVERY short fill refuse and fall back to the detection price — optimistic, on 60% of
    live directions, with only a log line. The whitelist reserves refusal for a RECOGNISED
    disagreement."""
    r = {"executions": [{"order": {
        "cumQuantity": 5, "intent": "ORDER_INTENT_UNSPECIFIED",
        "avgPx": {"value": "0.8490", "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": "0.0400", "currency": "USD"}}}]}
    got = poly_avg_fill_cost(r, is_short=True)
    assert got == pytest.approx((1.0 - 0.8490) + 0.008, abs=1e-6), (
        "an unrecognised intent must be treated as no information and the caller trusted")


# ── is_short comes from the CALLER's token, never the venue's optional `intent` ───────────────

def test_poly_short_cost_is_correct_when_the_venue_OMITS_intent():
    """⛔ THE ROUND-2 BUG, ONE MISSING FIELD AWAY FROM RETURNING.

    Keying off `order["intent"]` meant an ABSENT intent fell back to the long formula. Below yes
    0.5 that flips the booked flatten loss NEGATIVE, and `bot/core/safety.py` clamps negatives to
    $0.00 — a real realized loss, invisible to both caps.

    `intent` genuinely can be absent: the repo's only captured real create-response carries it on
    `executions[0]` and NOT on `executions[1]`, which is the max-cum execution this function reads.
    The caller knows the side for free from its own token, so it must supply it."""
    no_intent = {"executions": [{"order": {
        "cumQuantity": 5,
        "avgPx": {"value": "0.2000", "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": "0.0400", "currency": "USD"}}}]}
    got = poly_avg_fill_cost(no_intent, is_short=True)
    assert got == pytest.approx((1.0 - 0.20) + 0.008, abs=1e-6)
    # and emphatically NOT the long reading, which is what the intent-keyed version returned
    assert got != pytest.approx(0.20 + 0.008, abs=1e-3)


def test_poly_refuses_when_the_venue_intent_CONTRADICTS_the_caller():
    """A disagreement means one of us is wrong about which side this fill is. Guessing either way
    books a real position at a mirrored price, so refuse: None = CANNOT READ, and the caller's
    explicit short-space fallback is already correct."""
    contradiction = {"executions": [{"order": {
        "cumQuantity": 5, "intent": "ORDER_INTENT_BUY_LONG",
        "avgPx": {"value": "0.2000", "currency": "USD"},
        "commissionNotionalTotalCollected": {"value": "0.0400", "currency": "USD"}}}]}
    assert poly_avg_fill_cost(contradiction, is_short=True) is None
    # and the mirror case
    short_resp = _poly_short_resp(5, "0.8490", "0.0400")
    assert poly_avg_fill_cost(short_resp, is_short=False) is None


# ── the CALLERS must supply is_short, and nothing pinned that until round 3 ───────────────────

def test_is_short_is_REQUIRED_so_an_omission_cannot_be_silent():
    """⛔ Replaces a source-string count that passed for the wrong reason.

    The old pin asserted `wired == calls - 1`, with a comment claiming the -1 was the import line.
    It was not — the import reads `poly_avg_fill_cost,` with a comma, never matching `(`. The count
    was 5 because of an ordinary COMMENT mentioning the function, so rewording a comment turned the
    pin red for no behavioural reason.

    Worse, it was the ONLY thing that caught the regression: hardcoding `is_short=False` at all
    four call sites — reinstating the round-3 bug on 60% of live directions — tripped that pin and
    no behavioural test, because no test drives the fire path with a `::short` token.

    Making the parameter REQUIRED is the stronger, non-brittle instrument: an omission is now a
    TypeError at the call site, which the existing suite surfaces 29 ways. [round 4]"""
    import inspect
    from bot.poly_us.client import poly_avg_fill_cost
    sig = inspect.signature(poly_avg_fill_cost)
    p = sig.parameters["is_short"]
    assert p.kind is inspect.Parameter.KEYWORD_ONLY, "is_short must be keyword-only"
    assert p.default is inspect.Parameter.empty, (
        "is_short must have NO default — a default of False makes an omission at a short call site "
        "silent, which is exactly the failure this parameter exists to prevent."
    )



def test_intent_map_records_the_POSITION_not_the_resulting_side():
    """Pins the constant itself, because behaviour cannot distinguish it.

    With disposals refused, a mismapped SELL_SHORT returns None either way — via the contradiction
    guard instead of the disposal guard — so a mutation of this table SURVIVES the behavioural
    suite. The table is still documentation, and a wrong-but-unobservable value is a trap for the
    next reader: Poly's names are {verb}_{POSITION} with the verb acting ON the position, so
    SELL_SHORT DISPOSES of a short — it ends up LONG while still concerning the SHORT position.
    The caller's `is_short` means "this is the ::short token", i.e. the position. [round 4]"""
    from bot.poly_us.client import _INTENT_IS_SHORT
    assert _INTENT_IS_SHORT == {
        "ORDER_INTENT_BUY_LONG": False,
        "ORDER_INTENT_SELL_LONG": False,
        "ORDER_INTENT_BUY_SHORT": True,
        "ORDER_INTENT_SELL_SHORT": True,
    }, "the map keys on the POSITION named by the intent, not the side it ends up on"
