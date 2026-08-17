"""The live-MM run guards — the ones with real-money consequences.

These had NO coverage, which is how the fixes they encode could silently regress. Each pins a
failure that has actually happened or was one config edit away.
"""
import ast
import inspect
import re
import signal

import pytest

from bot.kalshi import maker as mm
from bot.kalshi.maker import _RATE_LIMIT_BUDGET_U_PER_S, _rate_limit_units_per_s

SRC = inspect.getsource(mm.main)


# ── rate limit: the gate that stops a run recreating the 429 that left orders resting ────────────
# Measured cost model (venue-reference): list reads 10 units, cancel 2, create 10.
#   units/cycle = 20 (positions + fills) + markets · (2·10 + 2·2)
# The run that 429'd within minutes and tripped its fail-closed halt was markets=3 @ 6s ≈ 15.3 u/s.

# Bind to the REAL function. Re-implementing the formula here is how the first version of this
# file passed while the gate was mutated to hardcode markets=3.
_units_per_s = _rate_limit_units_per_s


def test_the_incident_profile_is_over_the_budget():
    """markets=3 @ 6s is the configuration that actually 429'd. It must not pass."""
    assert _units_per_s(3, 6.0) > 8.0


def test_the_documented_safe_run_passes():
    assert _units_per_s(3, 30.0) <= 8.0


def test_markets_can_recreate_the_incident_rate_at_a_long_requote():
    """THE HOLE THE FIRST GATE HAD. It tested --requote-s alone, so `--markets 18 --requote-s 30`
    reproduced the incident's units/s exactly while passing. Spend is dominated by markets."""
    assert _units_per_s(18, 30.0) > 8.0
    assert abs(_units_per_s(18, 30.0) - _units_per_s(3, 6.0)) < 0.5   # the SAME rate


def test_the_gate_is_actually_wired_into_the_real_money_path():
    """A budget nobody evaluates is not a guard."""
    assert "_rate_limit_units_per_s(args.markets, args.requote_s)" in SRC, (
        "the gate must call the shared budget function with the ACTUAL markets/requote args — "
        "hardcoding either one re-opens the hole this closed")
    assert "REFUSING" in SRC


def test_the_gate_REFUSES_the_over_budget_side_and_not_the_under():
    """THE DIRECTION OF THE GUARD, which string-matching cannot test — flipping `>` to `<` passed
    every other test in this file. Extract the comparison from the source and execute it."""
    m = re.search(r"if units_per_s\s*(>|<|>=|<=)\s*_RATE_LIMIT_BUDGET_U_PER_S\s*:", SRC)
    assert m, "the budget comparison must be present in a recognisable form"
    op = m.group(1)
    over = _rate_limit_units_per_s(3, 6.0)     # the profile that actually 429'd
    under = _rate_limit_units_per_s(3, 30.0)   # the documented safe run
    fires = {">": lambda v: v > _RATE_LIMIT_BUDGET_U_PER_S,
             ">=": lambda v: v >= _RATE_LIMIT_BUDGET_U_PER_S,
             "<": lambda v: v < _RATE_LIMIT_BUDGET_U_PER_S,
             "<=": lambda v: v <= _RATE_LIMIT_BUDGET_U_PER_S}[op]
    assert fires(over), "the gate must REFUSE the configuration that 429'd"
    assert not fires(under), "the gate must ALLOW --markets 3 --requote-s 30"


def test_the_budget_actually_scales_with_markets():
    """Directly kills the mutant that hardcoded markets=3: if the function ignores its first
    argument, these are equal."""
    assert _rate_limit_units_per_s(18, 30.0) > _rate_limit_units_per_s(3, 30.0)
    assert _rate_limit_units_per_s(3, 30.0) > _rate_limit_units_per_s(3, 60.0)
    assert _RATE_LIMIT_BUDGET_U_PER_S == 8.0


def test_the_gate_asserts_per_cycle_fill_polling_rather_than_offering_it_as_an_escape():
    """The ordering fix REQUIRES _FILLS_EVERY == 1: cancels run every cycle, so with N>1 the fills
    between polls are logged as cancelled_unfilled again. An earlier version of the refusal message
    literally suggested 'or raise _FILLS_EVERY' — which satisfies the rate gate and silently reverts
    the ordering fix in one edit."""
    assert "_FILLS_EVERY != 1" in SRC
    assert "raise _FILLS_EVERY" not in SRC, "must not suggest thinning the poll as a remedy"


def test_fill_polling_is_per_cycle_as_shipped():
    assert mm._FILLS_EVERY == 1


# ── ordering: fills must be polled BEFORE orders are cancelled ───────────────────────────────────

def test_fills_are_polled_before_quoting_in_the_main_loop():
    """`_quote` starts by cancelling, and `_log_unfilled` reads a set only the fill poll fills in.
    Polling after the cancel mislabels ~95% of fills as `cancelled_unfilled` at a 30s requote —
    inverting the one number this instrumentation exists to produce."""
    i_poll = SRC.index("await _record_fills_and_markout()")
    i_quote = SRC.index("await _quote(t)")
    assert i_poll < i_quote, "the fill poll must precede the quote/cancel loop"


def test_teardown_also_polls_fills_before_the_final_cancel():
    """The final cycle's orders were still labelled by the old ordering — 5% of a normal run, and
    the MOST interesting cycle on a run that exits early via the loss cap."""
    i_first_teardown_poll = SRC.index("await _record_fills_and_markout()",
                                      SRC.index("    finally:"))
    assert i_first_teardown_poll < SRC.index("await _cancel_everything()")


def test_the_final_poll_happens_before_the_book_task_is_cancelled():
    """Spread capture IS the maker edge; marking it against a frozen book is worse than not
    recording it."""
    # Match the STATEMENT, not the several comments that mention it by name — an earlier version of
    # this test matched a comment and reported a failure the code did not have.
    i_cancel_book = SRC.index("\n        book_task.cancel()")
    assert SRC.rindex("await _record_fills_and_markout()") < i_cancel_book, (
        "no fill poll may run after the book feed is cancelled — its markout/spread_capture rows "
        "would be marked against a frozen mid")


# ── markout horizons must not be tied to the requote interval ────────────────────────────────────

def test_markout_resolution_runs_on_its_own_ticker():
    """Resolving only inside the cycle loop meant a 5s horizon could not be valued until age>=30 at
    a 30s requote, so h=5 and h=30 fired on the same pass with IDENTICAL mk — one number written
    twice under two labels. `_mid` is a local cache read, so a 1s ticker costs zero rate-limit."""
    assert hasattr(mm, "main")
    # The ticker must be STARTED, not merely defined — an earlier version asserted only that the
    # name appeared, and passed happily when the task was replaced with `mk_task = None`.
    assert "asyncio.create_task(_markout_ticker())" in SRC
    # Bind to the REAL objects rather than main()'s source text. This test broke when the lifecycle
    # moved into MakerSession — not because behaviour changed, but because it was reading source,
    # which is the whole reason that habit is being refactored away.
    assert callable(mm.MakerSession.markout_ticker)
    assert callable(mm.MakerSession.resolve_markouts)
    ticker_src = inspect.getsource(mm.MakerSession.markout_ticker)
    assert "asyncio.sleep(1.0)" in ticker_src, "the ticker must run on its own 1s cadence"


def test_the_markout_ticker_is_cancelled_on_exit():
    """A stray task outliving the run holds the CSV writer open."""
    assert "mk_task.cancel()" in SRC


# ── the real-money gate: it had ZERO coverage, which is how a false safety claim survived ────────
# `real` does NOT gate order placement. `MakerSession.quote` calls create_order unconditionally and
# the only DRY short-circuit lives in KalshiClient. So `config.DRY_RUN` alone decides whether money
# moves, and `real` decides only whether the run is instrumented.

def test_order_placement_is_ungated_by_real_at_the_CALL_SITE():
    """Pins the FACT the refusal exists because of. An earlier version of this test stripped
    comments from `quote`'s BODY and asserted the substring 'real' was absent — fragile, and it
    inspected the wrong place: whether orders are gated is decided where `quote` is CALLED. A future
    `if real: await _quote(t)` would leave that version green while its claim became false."""
    assert "await _quote(t)" in SRC
    i = SRC.index("await _quote(t)")
    line_start = SRC.rindex("\n", 0, i) + 1
    assert SRC[line_start:i].strip() == "", (
        "`await _quote(t)` is unconditional today, which is WHY _real_money_refusal must exist. If "
        "you gate it on `real`, update this test and the comments that say orders are DRY_RUN-only.")


def test_a_DRY_RUN_false_env_without_the_flag_is_REFUSED_not_run_silently():
    """THE HOLE, tested by EXECUTING the guard rather than reading source. Orders are gated only by
    DRY_RUN, so this combination places real quotes while printing a DRY preview, stamping rows
    `dry-`, logging no fills and skipping the stray sweep. It must be a hard stop."""
    reason = mm._real_money_refusal(dry_run=False, flagged=False)
    assert reason and "REAL quotes" in reason


def test_the_three_coherent_modes_are_allowed():
    assert mm._real_money_refusal(dry_run=True, flagged=False) is None    # ordinary DRY preview
    assert mm._real_money_refusal(dry_run=False, flagged=True) is None    # intended real run
    # flag passed but config still DRY: cannot move money, and main() prints a DRY banner. Not a
    # refusal — that is the ordinary "forgot to configure it" case.
    assert mm._real_money_refusal(dry_run=True, flagged=True) is None


def test_the_refusal_is_RAISED_not_merely_computed():
    """`assert "REFUSING" in SRC` was vacuous — main() contains six of them, so a mutant that
    computes the refusal and discards it passed. Bind to the statement that actually stops the run:
    the guard's result must be tested and raised."""
    i = SRC.index("_real_money_refusal(config.DRY_RUN, flagged)")
    window = SRC[i:i + 250]
    assert "if refusal:" in window, "the guard's result must be TESTED, not just computed"
    assert "raise SystemExit" in window, "...and raised — a computed-and-discarded guard is inert"


def test_the_flag_is_read_as_a_plain_attribute_so_a_renamed_dest_raises():
    """`getattr(args, ..., False)` would silently degrade to permanent DRY — the safe money
    direction but the unsafe diagnosis one, and reachable only by an edit three lines away."""
    assert "args.i_understand_real_money" in SRC
    assert 'getattr(args, "i_understand_real_money"' not in SRC


# ── SIGTERM must run the teardown, not strand orders ─────────────────────────────────────────────

def test_sigterm_handler_raises_keyboardinterrupt_so_the_teardown_finally_runs():
    """A plain `kill` (SIGTERM) terminates without unwinding, so the cancel-all + stray-sweep
    finally never runs and real orders are left resting with the loss cap dead. The handler makes
    SIGTERM raise KeyboardInterrupt — the exact exception SIGINT (Ctrl-C) already relies on to unwind
    that finally — so `kill` and Ctrl-C take the identical teardown path. (SIGKILL is uncatchable and
    stays the operator's one forbidden signal.)"""
    # ⚠️ Restore all THREE. This installs SIGHUP as well as SIGTERM, and invoking the handler
    # disarms SIGINT too — so a SIGTERM-only `finally` leaked both into the rest of the suite (and
    # into the pytest process), where a stray SIGHUP would then raise KeyboardInterrupt.
    prev = {s: signal.getsignal(s) for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}
    try:
        mm._install_sigterm_handler()
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        assert handler not in (signal.SIG_DFL, signal.SIG_IGN)  # default terminate-without-unwind gone
        with pytest.raises(KeyboardInterrupt):
            handler(signal.SIGTERM, None)
    finally:
        for _s, _h in prev.items():
            signal.signal(_s, _h)


def test_SIGHUP_also_unwinds__an_ssh_drop_is_the_likeliest_strand():
    """⛔ SIGHUP WAS THE HOLE, and it is the signal an attended run actually meets. The kernel
    sends it to every process in the foreground group when the controlling terminal goes away: an
    SSH drop, a closed lid, a killed terminal. Its default disposition is terminate-WITHOUT-unwinding,
    so before this the teardown's cancel-all + venue stray-sweep never ran and live post-only quotes
    were left resting with the loss cap dead — the exact strand the SIGTERM handler exists to
    prevent, reached by the likeliest route rather than the one anyone tests.

    tmux/screen remains the primary defence; this is the backstop."""
    prev = {s: signal.getsignal(s) for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}
    try:
        # ⛔ SIGINT IS IN THIS LOOP DELIBERATELY. Leaving it to asyncio's own handler let the sweep
        # survive but truncated everything after it — the fills catch-up, `client.close()`, and the
        # OPEN-POSITION warning — so a second Ctrl-C exited a real run SILENTLY HOLDING INVENTORY.
        # Measured over all 9 first×second signal pairs: 6/9 reached the end of teardown with SIGINT
        # left alone, 9/9 with it installed. ⚠️ The disarm assertion below CANNOT catch this: the
        # handler sets all three to SIG_IGN once invoked, so `getsignal(SIGINT) is SIG_IGN` holds
        # whether or not SIGINT was ever installed. Dropping SIGINT from the install tuple passed
        # this file until this line existed.
        for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            mm._install_sigterm_handler()          # re-install: the handler DISARMS itself, below
            handler = signal.getsignal(sig)
            assert callable(handler), f"{sig.name} must be handled"
            assert handler not in (signal.SIG_DFL, signal.SIG_IGN), \
                f"{sig.name} still has its terminate-without-unwind default"
            with pytest.raises(KeyboardInterrupt) as ex:
                handler(sig, None)
            # The signal NAME must reach the message, or a post-mortem cannot tell which one fired.
            assert sig.name in str(ex.value)

            # ⛔ AND IT MUST DISARM ITSELF ON THE WAY OUT. The KeyboardInterrupt it just raised starts
            # a teardown that AWAITS (the flatten waits up to --flatten-wait-s, default 60s). A
            # SECOND signal in that window raises into the unwinding frame, asyncio tears the loop
            # down, and the venue sweep dies on `RuntimeError: no running event loop` — swallowed by
            # its own `except Exception`, leaving the flatten's own resting post-only order live with
            # the loss cap dead. Measured: SIGINT-first survives (asyncio cancels the task), but
            # SIGHUP/SIGTERM-first + ANY second signal killed the sweep until this disarm landed.
            # SIGINT is included because as a SECOND signal it kills the sweep too.
            for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
                assert signal.getsignal(s) is signal.SIG_IGN, (
                    f"{s.name} must be ignored once teardown has begun, or a second signal strands "
                    f"the flatten's own resting order")
    finally:
        for s, h in prev.items():
            signal.signal(s, h)


def test_the_flatten_is_NOT_gated_on_a_clean_exit():
    """⛔ THE BRANCH THAT FIRED WAS THE BRANCH THAT LEFT INVENTORY. The teardown used to gate
    the passive flatten on `not halted`, reasoning that a loss-cap halt means the market moved
    against us and is the worst moment to place orders. That inverts the risk: the process is
    EXITING, so the loss cap dies with it — declining to flatten does not avoid the adverse move, it
    walks away from an adverse position and leaves it unattended with no cap at all.

    Safe to drop because `flatten_on_exit` is PASSIVE, post-only, at the touch and REDUCING-ONLY (it
    cannot add exposure or cross), and it reads the VENUE position with its own fail-closed — so
    `positions_cannot_verify`, the one halt where flattening blind would be wrong, self-refuses
    inside rather than needing this gate.

    ⛔ THE FIRST VERSION OF THIS TEST ASSERTED ON SOURCE TEXT and was defeated by a one-liner:
    nesting `if not halted:` inside the outer branch restores the defect while keeping the old string
    absent, and it passed. So the decision is now a PREDICATE and this walks its whole truth table —
    `halted` is a parameter that must not change the answer."""
    for halted in (False, True):
        assert mm._should_flatten(True, True, halted) is True, \
            f"a real run with --flatten-on-exit must flatten (halted={halted})"
    # ...and the two things that DO decide it still decide it, or the predicate is vacuous.
    assert mm._should_flatten(False, True, False) is False, "DRY places nothing"
    assert mm._should_flatten(True, False, False) is False, "no --flatten-on-exit, no flatten"

    # And the teardown must actually route through the predicate rather than re-inlining the test.
    body = ast.unparse(ast.parse(inspect.getsource(mm.main)))
    assert "_should_flatten(real, args.flatten_on_exit, halted)" in body
    assert "not halted" not in body.split("_should_flatten")[-1][:400], \
        "no re-gating on `halted` around the flatten call"


def test_the_shim_installs_the_sigterm_handler_before_running():
    """The handler lives in maker but is wired at the process entry point (the shim), so a real CLI
    run gets it while tests that call main() directly do not pollute global signal state."""
    import scripts.kalshi_live_mm as shim
    shim_src = inspect.getsource(shim)
    assert "_install_sigterm_handler()" in shim_src


def test_own_size_at_a_price_matches_with_the_same_tolerance_queue_ahead_uses():
    """`_own_at` exists to give the price comparison ONE spelling. It nets our own resting size out
    of the depth that becomes `queue_ahead`, and an exact-equality miss fails SILENTLY in the
    direction of not subtracting — inflating `ahead` and, downstream, the adverse verdict.

    Pinned at the helper because the live path currently cannot produce a divergence (`_tick`
    string-normalises and `feed._derive` does the complement in exact Decimal before the float
    boundary), so this is a defensive guard whose contract only a direct test can hold."""
    own = {("buy", 0.40): 1.0, ("sell", 0.45): 2.0}
    assert mm._own_at(own, "buy", 0.40) == 1.0
    assert mm._own_at(own, "buy", 0.40 + 1e-12) == 1.0, "a representation wobble must still match"
    assert mm._own_at(own, "buy", 0.45) == 0.0, "a different price is not ours"
    assert mm._own_at(own, "sell", 0.40) == 0.0, "the other side's resting size is not ours"
    assert mm._own_at({}, "buy", 0.40) == 0.0


def test_the_reported_fill_count_EXCLUDES_flatten_fills():
    """⛔ THE SOURCE ASSERTION THIS REPLACES WAS GREEN ON THE ACTUAL DEFECT. It counted occurrences of
    `len(seen_fills) - len(sess.flatten_fill_ids)` in `main()`'s AST and required 2. Review showed
    a mutant walk straight through it — keep a decoy `_dead = len(seen_fills) - len(sess.flatten_
    fill_ids)` beside a `print` of the INFLATED number and the count still reads 2. It was also RED on
    a correct refactor (hoisting the subexpression into a local). A source assertion tests how the
    code is WRITTEN.

    `fill_counts`/`summary_note` are extracted so the reported numbers can be CALLED rather than
    spelled, which kills the decoy-and-discard shape. ⚠️ It does NOT make the mutants impossible — an
    earlier version of this docstring said it did, which is the same false-safety claim this whole
    change exists to retract. An INSERT still passes this test: a rebind of `note` after the pinned
    call, or a second `print` beside it, both leave it green while `main()` reports the inflated
    number. Verified by running both. The pin with teeth for those is
    `test_maker_main_e2e.py::test_the_end_row_and_closing_line_report_MAKER_fills_only`, which reads
    the numbers actually reported.

    Why it matters: `seen_fills` doubles as the dedup set and the advertised count, so the teardown
    flatten raised the numerator while `stats['placed']` (quotes only) held the denominator. The run
    plan compares this count against a FIFO model's prediction — an inflated count argues the model
    UNDER-predicts, the reading that would justify committing more capital."""
    assert mm.fill_counts({"a", "b", "c"}, {"c"}) == (2, 1)
    assert mm.fill_counts({"a", "b"}, set()) == (2, 0), "no flatten ⇒ every seen fill is maker edge"
    assert mm.fill_counts({"a"}, {"a"}) == (0, 1), "a run whose ONLY fill was the forced exit"

    stats = {"placed": 7, "rejected": 2}
    note = mm.summary_note(stats, {"a", "b", "c"}, {"c"})
    assert "fills=2" in note, f"maker fills only; got {note!r}"
    assert "flatten_fills=1" in note, "and the flatten count is REPORTED, not hidden"
    # ...and no flatten ⇒ no suffix, so the `end` row of an ordinary run is unchanged.
    assert mm.summary_note(stats, {"a", "b"}, set()) == "quotes=7 rejects=2 fills=2"

    # ⛔ BOTH REPORTING SITES. Pinning only the CSV row left the stdout line free: a mutant that
    # printed the INFLATED `len(seen_fills)` there passed the entire suite. ⚠️ Not because nothing
    # EXECUTED it — every e2e test drives `main()` and so runs that print; nothing READ it. (An
    # earlier version of this comment said "executed", a weakened restatement of the same false
    # no-coverage belief.) The stdout line is what an operator reads during an attended run.
    line = mm.summary_line(stats, {"a", "b", "c"}, {"c"})
    assert "real fills=2" in line, f"maker fills only; got {line!r}"
    assert "+1 flatten fills, NOT maker edge" in line
    assert "real fills=2" in mm.summary_line(stats, {"a", "b"}, set())
    assert "flatten fills" not in mm.summary_line(stats, {"a", "b"}, set()), \
        "no flatten ⇒ no suffix, so an ordinary run's line is unchanged"

    # And main() must ROUTE through them rather than re-inlining the arithmetic.
    # ⚠️ PIN THE COMPOSITION, not merely the call. Asserting the call APPEARS is satisfied by a
    # decoy that invokes it and discards the result (`_d = summary_line(...)` beside a `print` of the
    # inflated number) — verified: that mutant passed, and is RED against the assertions below.
    # Require the returned value to be the one BOUND/PRINTED. This still cannot see an INSERT after
    # the pinned statement — that is what the e2e test is for — and it stays RED on a benign rename,
    # the same brittleness criticised above. It is a backstop, not the primary pin.
    body = ast.unparse(ast.parse(inspect.getsource(mm.main)))
    assert "note = summary_note(stats, seen_fills, sess.flatten_fill_ids)" in body, \
        "the `end` row's note must BE the returned value"
    assert "print(summary_line(stats, seen_fills, sess.flatten_fill_ids))" in body, \
        "the closing line must BE the returned value, not a separately-built string"
