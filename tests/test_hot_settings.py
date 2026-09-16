"""Pins for the hot-settings PURE half (M0.8). Every refusal here is a rail: the review's
verdict was that the whole risk of this feature is a config that applies successfully and
shouldn't have — so the parse is tested as a refusal machine first, an applier second."""
from __future__ import annotations

import json

from bot.core.hot_settings import parse_hot_settings

SLATE = {"example-a": 85, "example-b": 44, "example-c": 5}
CF = {"example-a": 2, "example-b": 2, "example-c": 2}
#: PER-BOOK launch reaches [verification (a): the slate max let a small book borrow the
#: biggest book's headroom and trip ITS OWN guard].
REACH = {"example-a": 255, "example-b": 132, "example-c": 15}
MIN = 5


def _parse(books: dict, slate: dict | None = None, retired=(), **top):
    """`slate` overrides the RUNNING sizes — the launch dicts stay fixed, which is exactly
    the state after a prior hot file applied [B2 needs this split to be expressible].
    `retired` is the hot slate's DROPPED set [continuous maker P1]."""
    payload = {"updated": "2026-08-06T03:00:00Z", "books": books, **top}
    return parse_hot_settings(json.dumps(payload), slate_sizes=slate or SLATE,
                              launch_cap_fills=CF, launch_reach=REACH, min_size=MIN,
                              retired=retired)


class TestWholeFileRefusals:
    def test_a_valid_file_parses_to_exactly_its_changes(self):
        got, why = _parse({"example-b": {"size": 22, "quote": "reduce_only"},
                           "example-c": {"cap_fills": 1}})
        assert why is None
        assert got == {"example-b": {"size": 22, "quote": "reduce_only"},
                       "example-c": {"cap_fills": 1}}

    def test_an_unknown_slug_is_a_SLATE_ADDITION_and_ignores_the_whole_file(self):
        got, why = _parse({"example-b": {"size": 22}, "brand-new-book": {"size": 5}})
        assert got is None and "ADDITION" in why, (
            "the valid usse entry must NOT survive — whole-file-or-nothing")

    def test_a_RETIRED_slug_is_SKIPPED_not_treated_as_a_slate_ADDITION(self):
        """⛔ [continuous maker P1] A hot-slate DROP takes a book off the running slate while
        this LEVEL-triggered file still names it. Read as a slate addition it would void the
        whole file — taking every OTHER book's drain, size override and latch ack with it, on the
        one axis the operator cannot see from the file. The dropped entry has nothing left to
        apply to; the rest of the file still applies."""
        dropped = {k: v for k, v in SLATE.items() if k != "example-c"}
        got, why = _parse({"example-b": {"size": 22}, "example-c": {"size": 5}},
                          slate=dropped, retired=("example-c",))
        assert why is None, f"the file must still apply, got {why}"
        assert got == {"example-b": {"size": 22}}, "the dropped book's entry is skipped, not applied"
        # ⛔ AND ONLY FOR A BOOK THE SLATE ACTUALLY DROPPED: a slug that was never on this run's
        # slate is still the ADDITION refusal, which is what bypasses preflight.
        got2, why2 = _parse({"example-b": {"size": 22}, "brand-new-book": {"size": 5}},
                            slate=dropped, retired=("example-c",))
        assert got2 is None and "ADDITION" in why2

    def test_a_never_hot_knob_arrives_as_an_unknown_key_and_ignores_the_file(self):
        for bad_top in ({"loss_cap": 20}, {"max_total_contracts": 500},
                        {"requote_s": 5}, {"i_understand_real_money": True}):
            got, why = _parse({"example-b": {"size": 22}}, **bad_top)
            assert got is None and "unknown top-level" in why, f"{bad_top} slipped through"

    def test_unknown_per_book_keys_refuse(self):
        got, why = _parse({"example-b": {"size": 22, "loss_cap": 1}})
        assert got is None and "unknown key" in why

    def test_unparseable_json_and_empty_books_refuse(self):
        assert parse_hot_settings("{not json", slate_sizes=SLATE, launch_cap_fills=CF,
                                  launch_reach=REACH, min_size=MIN)[0] is None
        assert _parse({})[0] is None

    def test_an_empty_book_spec_is_a_typo_not_a_noop(self):
        got, why = _parse({"example-b": {}})
        assert got is None and "typo" in why


class TestInterlocks:
    def test_cap_fills_RAISE_refuses_lowering_passes(self):
        """⛔ The review's sharpest catch: raising re-opens the adding side on exactly the
        carried-book class whose basis is operator-TYPED."""
        got, why = _parse({"example-a": {"cap_fills": 3}})
        assert got is None and "RAISES" in why
        got, why = _parse({"example-a": {"cap_fills": 1}})
        assert why is None and got["example-a"]["cap_fills"] == 1

    def test_a_size_raise_past_THIS_BOOKS_launch_reach_refuses(self):
        """⛔ The guard interlock, PER BOOK: example-b's own reach is 132 — a raise to 45 (135)
        refuses even though it is nowhere near example-a's 255; a small book must not borrow the
        biggest book's headroom."""
        got, why = _parse({"example-b": {"size": 45}})       # 45×3=135 > example-b's 132
        assert got is None and "THIS BOOK" in why
        got, why = _parse({"example-b": {"size": 40}})       # 40×3=120 ≤ 132
        assert why is None

    def test_the_HORMUZ_shape_a_small_book_under_the_slate_max_still_refuses(self):
        """⛔ The verification round's concrete case: example-c 5 → 14 is reach 42 — far under
        example-a's 255, but past example-c's OWN 15, and example-c's guard fires at 40. Under the slate-max
        interlock this passed and halted the whole run."""
        got, why = _parse({"example-c": {"size": 14}})
        assert got is None and "THIS BOOK" in why

    def test_the_reach_check_uses_the_HOT_cap_fills_when_both_change(self):
        """size 60 × (1+1) = 120 ≤ example-b's 132 passes ONLY because the same file lowers
        cap_fills; the interlock must evaluate the pair as it will actually run."""
        got, why = _parse({"example-b": {"size": 60, "cap_fills": 1}})
        assert why is None
        got, why = _parse({"example-b": {"size": 60}})       # 60×3=180 at launch cf → refuse
        assert got is None and "reach" in why

    def test_two_legal_files_cannot_COMPOSE_past_the_launch_reach(self):
        """⛔ [convergence B2] The exploit, verbatim: file A takes the paid raise (size 127
        × cf 1 → reach 254 ≤ example-a's 255 — legal alone), file B "restores" cap_fills to
        launch. Judged only against LAUNCH cf, B looks like a lowering; against the
        RUNNING size it is reach 381 on a guard sized at 280. The cap_fills block must
        run the reach interlock against the size that will actually run."""
        got, why = _parse({"example-a": {"size": 127, "cap_fills": 1}})
        assert why is None, "file A is legal on its own — 127×2=254 ≤ 255"
        running = {"example-a": 127, "example-b": 44, "example-c": 5}       # the state after A applied
        got, why = _parse({"example-a": {"cap_fills": 2}}, slate=running)
        assert got is None and "reach" in why, (
            "cap_fills back to launch against running size 127 is reach 381 — must refuse")
        got, why = _parse({"example-a": {"size": 85, "cap_fills": 2}}, slate=running)
        assert why is None, (
            "restoring BOTH in one file lands exactly at the launch reach 255 — the legal "
            "way back must stay open or the refusal teaches a live-run restart")

    def test_size_below_venue_minimum_and_bad_types_refuse(self):
        for spec in ({"size": 4}, {"size": "22"}, {"size": True},
                     {"cap_fills": 0}, {"cap_fills": "1"}, {"quote": "halt"},
                     {"quote": "pause"}):
            got, why = _parse({"example-b": spec})
            assert got is None, f"{spec} slipped through"


class TestRunIdStamp:
    """[B8, plumbing audit 2026-08-18] A file stamped for another run must not steer this
    one (M0.8: a stale prior-run file set two books reduce-only on a fresh run and the log
    looked healthy). Unstamped files stay cross-run BY DESIGN — the launch flow writes drain
    overrides before the run id exists."""

    def _stamped(self, stamp, run_id):
        payload = {"updated": "x", "books": {"example-b": {"quote": "reduce_only"}}}
        if stamp is not None:
            payload["run_id"] = stamp
        return parse_hot_settings(json.dumps(payload), slate_sizes=SLATE,
                                  launch_cap_fills=CF, launch_reach=REACH, min_size=MIN,
                                  run_id=run_id)

    def test_matching_stamp_applies(self):
        got, why = self._stamped("polymm-real-r1", "polymm-real-r1")
        assert why is None and "example-b" in got

    def test_foreign_stamp_is_refused_whole_file(self):
        got, why = self._stamped("polymm-real-OLD", "polymm-real-NEW")
        assert got is None and "B8" in why and "polymm-real-OLD" in why

    def test_unstamped_file_stays_cross_run(self):
        got, why = self._stamped(None, "polymm-real-r1")
        assert why is None and "example-b" in got

    def test_stamp_without_engine_run_id_applies(self):
        # a caller that passes no run_id (older call sites) keeps today's behavior
        got, why = self._stamped("polymm-real-r1", None)
        assert why is None and "example-b" in got


class TestSizeRaises:
    """✅  v1.1 — the raise, and the five rails it has to clear.

    ⛔ The hazard v1 was written around: the launch reach is the guard anchor AND the
    venue-breach threshold, so a raise that nobody re-anchors produces a guard that halts a
    whole run on LAWFUL inventory. v1 refused every raise because the maker was not told
    what the external guards watch for; v1.1 tells it, and refuses on the real number."""

    #: What the launcher actually started each guard with: its own reach + GUARD_MARGIN 25.
    #: ⛔ DO NOT "de-magic-number" these, or the 156/157 boundary literals below, by deriving
    #: them from `poly_launch.GUARD_MARGIN` [mm-review round 2]: they are the only place in
    #: the suite that carries that constant independently, so a derived form would agree with
    #: a GUARD_MARGIN of any value — including one changed by accident.
    GUARDS = {"example-a": 280, "example-b": 157, "example-c": 40}
    RUN = "polymm-real-r1"

    def _raise(self, books, *, stamp=RUN, guards=None, latched=(), reduce_only=(),
               loss_cap_ok=True, max_total=1_000, slate=None, cf=None):
        payload = {"updated": "x", "books": books}
        if stamp is not None:
            payload["run_id"] = stamp
        return parse_hot_settings(
            json.dumps(payload), slate_sizes=slate or SLATE, launch_cap_fills=CF,
            launch_reach=REACH, min_size=MIN, run_id=self.RUN,
            running_cap_fills=cf if cf is not None else CF,
            guard_thresholds=self.GUARDS if guards is None else guards,
            max_total_contracts=max_total, latched=latched, reduce_only=reduce_only,
            loss_cap_ok=loss_cap_ok)

    def test_a_stamped_raise_inside_the_REAL_guard_threshold_applies(self):
        """example-b: launch reach 132, guard 157. 52 × 3 = 156 is past the launch anchor — the
        exact edit v1 refused — and inside the guard that actually exists."""
        got, why = self._raise({"example-b": {"size": 52}})
        assert why is None, why
        assert got == {"example-b": {"size": 52}}

    def test_a_raise_PAST_the_real_guard_threshold_still_refuses(self):
        """One contract past: 53 × 3 = 159 > 157. The guard fires on |net| > 157, so a
        lawful 159 is the whole-run false halt the rail exists to prevent."""
        got, why = self._raise({"example-b": {"size": 53}})
        assert got is None
        assert "157" in why and "guard" in why.lower()

    def test_a_reach_EXACTLY_at_the_threshold_is_lawful(self):
        """The boundary is `≤` and it is REACHABLE: example-c 20 × (1+1) = 40 is exactly its
        guard's number, and the guard fires on `|net| > 40`, so 40 is still lawful. One
        contract further (21 × 2 = 42) is the false halt."""
        got, why = self._raise({"example-c": {"size": 20, "cap_fills": 1}})
        assert why is None, why
        got, why = self._raise({"example-c": {"size": 21, "cap_fills": 1}})
        assert got is None and "40" in why

    def test_an_UNSTAMPED_raise_refuses_even_though_lowerings_stay_cross_run(self):
        """⛔ The asymmetry: an unstamped file is cross-run BY DESIGN for
        the drain/lowering direction (the launch flow writes it before a run id exists), but
        a stale file that GROWS a live seat is the adding direction and must name its run."""
        got, why = self._raise({"example-b": {"size": 52}}, stamp=None)
        assert got is None and "STAMPED" in why
        got, why = self._raise({"example-b": {"size": 30}}, stamp=None)
        assert why is None, "the same file LOWERING must still apply unstamped"

    def test_a_raise_with_no_guard_plumbing_gets_the_v1_refusal(self):
        """The default state, and every pre-v1.1 call site: a maker that was not told what
        is watching it may not spend that headroom."""
        got, why = self._raise({"example-b": {"size": 52}}, guards={})
        assert got is None and "v1 CONSEQUENCE" in why
        got, why = self._raise({"example-b": {"size": 52}}, guards={"example-a": 280})
        assert got is None and "v1 CONSEQUENCE" in why, "another book's guard is not this one's"

    def test_an_adverse_LATCHED_book_refuses_a_raise(self):
        """A latched book is reduce-only by rail; sizing it up is the opposite decision, and
        `size` is no more an acknowledgement than `quote: normal` is."""
        got, why = self._raise({"example-b": {"size": 52}}, latched=("example-b",))
        assert got is None and "LATCHED" in why
        got, why = self._raise({"example-b": {"size": 52}}, latched=("example-a",))
        assert why is None, "another book's latch must not freeze this one"

    def test_a_REDUCE_ONLY_book_refuses_a_raise(self):
        """Rail 3b [mm-review C-probe2]: a book that cannot lawfully add gets no bigger
        seat — the raise would buy only bigger exits while permanently widening its breach
        threshold."""
        got, why = self._raise({"example-b": {"size": 52}}, reduce_only=("example-b",))
        assert got is None and "REDUCE-ONLY" in why
        got, why = self._raise({"example-b": {"size": 52}}, reduce_only=("example-a",))
        assert why is None, "another book's lane must not freeze this one"

    def test_a_global_rail_of_ZERO_is_OFF_not_a_rail_of_zero(self):
        """⛔ Repo convention: a non-positive bound DISABLES (the loss cap and `ws_near_cap`
        both read it that way). Refusing with 'past the rail 0' described a rail that is not
        in force. `None` is a different state — NOT PLUMBED — and still refuses."""
        got, why = self._raise({"example-b": {"size": 52}}, max_total=0)
        assert why is None, why
        got, why = self._raise({"example-b": {"size": 52}}, max_total=None)
        assert got is None and "v1 CONSEQUENCE" in why

    def test_the_rail_4_refusal_does_not_send_the_operator_to_LOOSEN_the_guard(self):
        """⛔ [mm-review MB2] It used to say "restart its guard at a higher
        --pause-threshold FIRST". Following that instruction loosens the only external
        watcher and then yields the byte-identical refusal, because thresholds are read once
        at start and no hot key can update them. The honest route is a relaunch."""
        why = self._raise({"example-b": {"size": 53}})[1]
        assert "RELAUNCH" in why and "does NOT help" in why

    def test_a_breached_loss_cap_refuses_a_raise(self):
        got, why = self._raise({"example-b": {"size": 52}}, loss_cap_ok=False)
        assert got is None and "LOSS CAP" in why
        got, why = self._raise({"example-b": {"size": 30}}, loss_cap_ok=False)
        assert why is None, "a LOWERING is the safe direction and must stay available"

    def test_a_new_CAP_past_the_global_rail_refuses(self):
        """⛔ Rail 5 is PER BOOK and it is not `Σcaps ≤ max_total`: on every launcher-started
        run Σcaps is ALREADY above max_total by construction (the launcher refuses a global
        cap that does not bind), so the Σ form would refuse every raise ever written. What it
        does say: one book may not be capped past the whole run's exposure budget, because
        gross exposure halts there whatever the per-book cap claims."""
        got, why = self._raise({"example-b": {"size": 52}}, max_total=103)   # new cap 104
        assert got is None and "GLOBAL rail" in why and "104" in why
        got, why = self._raise({"example-b": {"size": 52}}, max_total=104)
        assert why is None, "exactly at the rail is inside it"

    def test_the_rail_that_binds_is_NAMED_and_the_others_are_not(self):
        """An operator with a live seat and a refused file needs to know WHICH number to
        change. The same edit refused for three different reasons names three."""
        assert "guard" in self._raise({"example-b": {"size": 53}})[1].lower()
        assert "GLOBAL rail" in self._raise({"example-b": {"size": 52}}, max_total=50)[1]
        assert "LATCHED" in self._raise({"example-b": {"size": 52}}, latched=("example-b",))[1]

    def test_cap_fills_RAISES_stay_refused_in_v11(self):
        """v1.1 re-opens the SIZE direction only — raising cap_fills re-opens the adding side
        on a carried book whose basis is operator-TYPED, which no guard threshold answers."""
        got, why = self._raise({"example-c": {"cap_fills": 3}})
        assert got is None and "RAISES" in why

    def test_the_running_cap_fills_make_the_size_check_EXACT_not_looser(self):
        """example-a ran cap_fills down to 1, so its reach is 85×2=170 and 120×2=240 fits its own
        anchor 255 — legal, and not a raise at all. Judged against the LAUNCH cap_fills 2 it
        reads as 360 and refuses; that conservatism was the v1 approximation, and the pair is
        still evaluated as it will actually RUN."""
        running_cf = {"example-a": 1, "example-b": 2, "example-c": 2}
        got, why = self._raise({"example-a": {"size": 120}}, cf=running_cf)
        assert why is None, why
        got, why = self._raise({"example-a": {"size": 120}}, cf=CF)
        assert got is None and "THIS BOOK" in why
        # …and the cap_fills block still catches the restore against the running size.
        got, why = self._raise({"example-a": {"cap_fills": 2}},
                               slate={"example-a": 120, "example-b": 44, "example-c": 5}, cf=running_cf)
        assert got is None and "reach" in why


class TestAdverseLatchAck:
    """`clear_adverse_latch` — the only channel that clears a run-lifetime adverse latch.

    ⛔ It is a TIMESTAMP because this file is LEVEL-triggered: the
    parser returns every book in the file, stale entries are never pruned, and any later
    unrelated edit re-presents them. An undated ack left behind would clear a latch that had
    since re-fired. The parse's job is to guarantee the engine gets a comparable instant, or
    nothing at all."""

    def test_a_utc_ack_parses_to_its_epoch(self):
        got, why = _parse({"example-b": {"clear_adverse_latch": "2026-08-19T04:12:00Z"}})
        assert why is None
        # The literal epoch of that instant — not a re-derivation of the code's own parse.
        assert got == {"example-b": {"clear_adverse_latch": 1787112720.0}}

    def test_an_explicit_offset_is_normalized_to_the_same_instant(self):
        got, _ = _parse({"example-b": {"clear_adverse_latch": "2026-08-19T00:12:00-04:00"}})
        assert got["example-b"]["clear_adverse_latch"] == 1787112720.0

    def test_a_NAIVE_stamp_is_refused_whole_file(self):
        got, why = _parse({"example-b": {"clear_adverse_latch": "2026-08-19T04:12:00"},
                           "example-c": {"cap_fills": 1}})
        assert got is None and "tz-aware" in why, (
            "a naive stamp cannot be compared against a wall-clock latch, and guessing a zone "
            "can only guess toward clearing a latch nobody meant to clear")

    def test_a_non_string_or_unparseable_ack_is_refused_whole_file(self):
        for bad in ('{"clear_adverse_latch": true}', '{"clear_adverse_latch": 1787112720}',
                    '{"clear_adverse_latch": "yesterday"}', '{"clear_adverse_latch": ""}'):
            got, why = _parse({"example-b": json.loads(bad)})
            assert got is None and "clear_adverse_latch" in why, bad

    def test_the_ack_composes_with_ordinary_keys_in_one_book(self):
        got, why = _parse({"example-b": {"size": 22,
                                    "clear_adverse_latch": "2026-08-19T04:12:00Z"}})
        assert why is None
        assert got == {"example-b": {"size": 22, "clear_adverse_latch": 1787112720.0}}
