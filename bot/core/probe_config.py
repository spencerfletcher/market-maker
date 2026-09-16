"""`the probe registration file` — the ONE registration surface for the probe lane's knobs and admission.

⛔ **THE FILE IS THE REGISTRY**. Every rail that asks
*is this value registered?* (`registry_refusals`, the supervisor's `argv_findings`, the
amendment-15 event-risk lane scope) answers against THIS FILE, not a python literal.

Five design rules, all load-bearing, all pinned by tests:

1. **FAIL-CLOSED, AT THE POINT OF USE.** A missing file, a missing key, an unknown key or a wrong
   type RAISES `ProbeConfigError` — from `get_config()`, on the PROBE lane, ⛔ **never at import**
   (an import-time load made a probe-registry typo refuse MAIN-lane launches). No fallback, and
   absence is never a value; replaces `probe_sports.json`'s "absent ⇒ defaults" branch.
2. **PROVENANCE PER KEY.** Every required key must resolve an amendment tag (`_amendment`, or a
   per-key `_amendments` override). A value nobody can attribute is not registered.
3. **PRECEDENCE file < CLI.** The file supplies every argparse DEFAULT; a flag overrides it.
   ⛔ ARMED FLAGS STAY CLI-ONLY — `--i-understand-real-money`, `--assume-no-maintenance`.
   Arming is a per-line operator act.
4. **THE BANNER NAMES THE SOURCE.** `banner_lines` prints every resolved value with `file` / `cli`,
   read off the ARGV; both probe parsers set `allow_abbrev=False`, so an abbreviated flag cannot set
   a value the banner reports as file-sourced.
5. **HARD CEILINGS** on `max_cells` / `max_seconds` / `loss_cap` — outer bounds on a typo, never a
   policy. ⚠️ Residual risk stated on the constants.

⚠️ **THE MAKER'S LATCH ARMS ARE RECORDED HERE, NOT READ FROM HERE.** `latch.ban_arms_s` mirrors
`bot.poly_us.maker.LATCH_BAN_ARMS_S`; the maker reads its own constant and the two are pinned equal.
⛔ **`latch.rule` IS THE EXCEPTION — the maker DOES read it**, ONCE, at maker construction, on the
probe lane only, so a bad value refuses the LAUNCH rather than raising inside `_book_fill`.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence

#: ⛔ AMENDMENT 34 — ONE SIZING RULE, as registered: a SCALAR = that fixed size, a LIST = draw
#: uniformly from those rungs per cell (the randomized size A/B's shape, generalised).
PlanValue = int | tuple[int, ...]

#: The checked-in registry. Resolved off THIS file's location, not the cwd — the probe lane is
#: launched from several directories and a relative path would read a different registry per cwd.
PROBE_CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config", "probe.json")

#: Section → the keys it MUST carry. A key absent here is an unknown key (refused); a key here and
#: absent from the file is a missing key (refused). Both halves of the schema, neither optional.
_REQUIRED: dict[str, tuple[str, ...]] = {
    "lane": ("size_plan", "cap_fills", "cap_fills_registered",
             "max_cells", "max_seconds", "loss_cap", "requote_s", "requote_arms_registered",
             "order_ttl_s", "flow_floor_gate_mult",
             "fractional_close", "quotability_gate", "max_cells_per_second"),
    "supervisor": ("cycle_seconds", "kalshi_sidecar", "reseat_s"),
    "latch": ("rule", "ban_arms_s", "control_share"),
    "admission": ("groups", "class_refused"),
    #: AMENDMENT 27 — the BACKFILL path's own admission bar. Required, for `admission`'s reason:
    #: absence would read as "backfill anything the flow floor refused", the pre-amendment policy
    #: this section exists to end.
    "backfill": ("min_prints", "class_exclude"),
}
#: Section → keys it MAY carry that are VALUES (not `_META`). ⛔ ADDITIVE KEYS ONLY, and only where
#: ABSENCE HAS ONE OBVIOUS READING that is also the SAFEST one — `admission.explore` absent means
#: NO exploration, which is exactly the pre-amendment-24 policy. Everything else stays required:
#: absence is not a value. A key here still validates fully WHEN PRESENT, and still needs its
#: provenance tag when present.
#: ⛔ THE ONE `admission.explore` KEY THAT IS NOT A CELL CLASS [AMENDMENT 42, operator go
#: 2026-09-10]: the number of SCORE-DISAGREEMENT seats a pick may add — the highest-ranked
#: candidate of the SHADOW seat score that the incumbent rank refused. It is a pseudo-class so the
#: cap lives with every other per-pick seat cap and stamps into `admission_v`; the registry
#: warning that names an unknown class must SKIP it (`poly_probe_night._admission_warnings`).
EXPLORE_SCORE_DISAGREEMENT = "score_disagreement"
_OPTIONAL: dict[str, tuple[str, ...]] = {
    "admission": ("explore",),
    #: AMENDMENT 33 — absence = NO operator-declared seats, the disarmed
    #: state and the safest reading. Every entry still validates fully when present.
    #: AMENDMENT 54 — `burst_gate` absent = `BURST_GATE_ON`, the
    #: pre-amendment reading: the gate runs, exactly as every file before this key did.
    "lane": ("fixed_seats", "burst_gate"),
    #: AMENDMENT 37 — absence = NO class runs unlatched, the safe reading.
    "latch": ("off_classes",),
    #: AMENDMENT 41 — absence = NO class is game-clock guarded, the
    #: pre-amendment reading. ⛔ The SECTION is optional too (see `_clock_guard`), unlike every
    #: entry above, which are optional keys of REQUIRED sections.
    "clock_guard": ("classes",),
}
#: Keys a section may carry that are documentation, never values.
_META = ("_doc", "_amendment", "_amendments")

#: ⛔ THE OPTIONAL `launch` SECTION [operator ask 2026-09-04]. Key → its accepted python type; the
#: value is the DEFAULT for the matching `poly_probe_supervisor` flag, and an explicit flag still
#: overrides it (file < CLI, rule 3).
#: ⛔ IT IS THE ONE **OPTIONAL** SECTION: absence has one unambiguous reading — the pre-2026-09-04
#: line, every trial flag OFF — and `LAUNCH_DEFAULTS` is that reading written down, so a file
#: without the block composes a BYTE-IDENTICAL child line. ⛔ The vocabulary is still CLOSED.
_LAUNCH_TYPES: dict[str, type | tuple[type, ...]] = {
    "improve_ab": bool,
    #: AMENDMENT 23 — the per-RUN randomized REQUOTE-CADENCE arm, and the arm SET it draws from
    #: (a list of STRINGS, parsed to `Decimal`: the cadence is a registered value, never a float
    #: literal). `requote_ab: false` with a set present draws nothing.
    "requote_ab": bool,
    "requote_arms_s": list,
    "rebate_step": bool,
    "rebate_step_mode": str,
    "rebate_step_max": int,
    "depth_sidecar": bool,
    "gamestate_sidecar": bool,
    "conditional_poll": bool,
}
#: ⛔ ABSENCE = TODAY'S LINE. Not "the shipped values" — the values a supervisor line carried
#: before any of this existed.
LAUNCH_DEFAULTS: dict[str, Any] = {
    "improve_ab": False,
    "requote_ab": False,
    "requote_arms_s": [],
    "rebate_step": False,
    "rebate_step_mode": None,
    "rebate_step_max": None,
    "depth_sidecar": False,
    "gamestate_sidecar": False,
    "conditional_poll": False,
}
#: ⛔ `rebate_step_mode`'s CLOSED vocabulary, mirroring `bot.poly_us.maker.REBATE_STEP_MODES` (where
#: the modes are owned) so the loader refuses a typo without importing the maker into every picker.
REBATE_STEP_MODES: frozenset[str] = frozenset({"floor", "optimal"})

#: ⛔ OUTER BOUNDS, NOT POLICY. The file is operator-editable and a fat-fingered zero is the failure
#: these catch — `max_cells: 80` or `loss_cap: "10000"` would otherwise be a REGISTERED value with
#: an amendment tag beside it. They sit far outside every registered value (8 / 3h / <n>).
#: ⚠️ RESIDUAL RISK: a file edit WITHIN these bounds still arms, and `_amendment` tags are
#: PRESENCE-checked, never verified against the prereg. Registration surface, not authorisation.
#: ⛔ AMENDMENT 17's OUTER BOUND likewise: the flow floor's bar is `flow_floor_gate_mult ×
#: poly_burst_gate.DEFAULT_MIN_PRINTS`, and a fat-fingered 20 would refuse the whole venue. It is 2.
FLOW_FLOOR_MULT_CEILING = 10
MAX_CELLS_CEILING = 40   # 24 -> 40, AMENDMENT 40: 30 auto-picks + 6 declared seats (24 = , 20 = )
MAX_SECONDS_CEILING = 86400
LOSS_CAP_CEILING = Decimal("1000")
#: ⛔ OUTER BOUND ON A TYPO for ANY size this registry carries — a declared seat's, and every
#: value of `lane.size_plan`: every other rail on the
#: number (--max-total-contracts, lawful reach, the per-book cap) is DERIVED from it and scales
#: with a typo instead of bounding it. Far outside every operator value; never a policy.
SIZE_CEILING = 500
#: ⛔ AMENDMENT 31's OUTER BOUND on `lane.max_cells_per_second`. The MEASURED per-cycle request
#: model is linear in the cell count and measures no sustained CF ceiling, so no CF number is
#: quoted here. The binding number is the pacer's own `bot.poly_us.maker.DEFAULT_MAX_REQ_PER_S`;
#: the ceiling is an outer bound on a typo, never a policy.
CELLS_PER_SECOND_CEILING = Decimal("1")   # an outer bound on a typo  # placeholder — production value withheld

#: ⛔ AMENDMENT 31 — the CLOSED vocabulary of a `lane.fixed_seats` entry, and of its `park` block.
#: Unknown sub-key refuses: an operator's typo (`"sizes"`, `"tick"`) must never seat a book at a
#: number nobody registered, and a silently-ignored key is exactly that.
FIXED_SEAT_KEYS: frozenset[str] = frozenset({"slug", "size", "park", "until"})
FIXED_SEAT_PARK_KEYS: frozenset[str] = frozenset({"side", "ticks"})
FIXED_SEAT_PARK_SIDES: frozenset[str] = frozenset({"bid", "ask"})
#: ⛔ THE MAKER'S OWN PARK FLOOR, MIRRORED — `bot.poly_us.maker.PARK_MIN_TICKS_OFF` owns it, and a
#: schema pin holds the two equal. Spelled here for `REBATE_STEP_MODES`' reason exactly: so the
#: loader can refuse a too-close park without importing the maker engine into every picker.
#: ⛔ AND IT MUST BE CHECKED AT PARSE TIME. `poly_live_mm.parse_park_seats` refuses below it with a
#: `SystemExit` as the maker line is composed — i.e. mid-window, after the pick, with the evening
#: already spent. A declared park is registered hours earlier and can be refused then.
PARK_MIN_TICKS_OFF = 100  # placeholder — production value withheld

#: ⛔ THE `size_rule` TAPE TAG FOR A SCALAR PLAN VALUE [AMENDMENT 34]. There is NO size MODE key
#: any more: the rule is DERIVED from the resolved plan value, so config and tape cannot disagree.
SIZE_RULE_FIXED = "fixed"
#: ⛔ AMENDMENT 34 — THE `lane.size_plan` SUB-KEYS, a CLOSED vocabulary (a typo must never drop a
#: sizing rule silently). `by_class` names `scripts.poly_probe_night.CELL_CLASSES` classes;
#: `by_slug` names books. Resolution order is slug → class → default (`SizePlan.value_for`).
SIZE_PLAN_KEYS: frozenset[str] = frozenset({"default", "by_class", "by_slug"})


#: ⛔ THE `size_rule` TAPE VOCABULARY AS A SHAPE, not a set — the tags are DERIVED from plan
#: values, so any ladder an operator registers produces its own. `fixed_seat` is RETIRED
#: (AMENDMENT 34 tags a declared seat `fixed`, and the picks tape's own `fixed_seat` column
#: carries the distinction) and stays admissible so rows written before 2026-09-09 still read.
_SIZE_RULE_RE = re.compile(r"^(?:fixed|fixed_seat|rand\d+(?:_\d+)*)$")


def is_size_rule(tag: str) -> bool:
    """Is this a `size_rule` THIS PROGRAM could have written? Readers drop anything else rather
    than joining a row to a rule nobody registered."""
    return bool(_SIZE_RULE_RE.match(tag))


def size_rule_for(value: "PlanValue") -> str:
    """The `size_rule` tape tag a resolved plan value carries — DERIVED, never registered.

    A SCALAR is `fixed`; a LIST is `rand` + its rungs ascending, underscore-joined, so the shipped
    default `[5, 10]` tags exactly `rand5_10` (byte-identical to every row on tape since AMENDMENT
    16, which is what keeps `scripts.poly_size_calib.read_arms` admitting them) and any OTHER
    ladder tags a DIFFERENT rule — `[5, 10, 20]` → `rand5_10_20`. ⛔ A value alone cannot tell a
    drawn 5 from a declared 5; the tag is what separates the treatments on tape.
    """
    if isinstance(value, int):
        return SIZE_RULE_FIXED
    return "rand" + "_".join(str(v) for v in value)

#: ⛔ THE PROBE LANE'S ADVERSE-LATCH SCOPE — a CLOSED vocabulary, fail-closed, and the ONE key in
#: this file the maker itself reads.
#: `kill_reverted_lifetime` = the book is latched for the RUN LIFETIME: no deadline is written,
#: `maker.draw_ban_seconds` is never reached. Incumbent since cycle-4's falsifier (b) fired. It
#: is the value the per-night harm stop flips BACK to.
#: `rand900_1800` = the randomized timed ban — ⛔ **RETIRED AS A RUN RULE**
#: (`LATCH_RULES_RETIRED`). Its harm cap still governs: cumulative ≤ −<n> → PAUSE; any night ≤ −<n>
#: → the next night runs `kill_reverted_lifetime` until scored.
#: ⛔ AN UNKNOWN STRING REFUSES — a typo must not silently re-admit banned books nor un-arm a trial.
#: ⛔ THE TAPE TAG IS THIS STRING — `latch_rule` on the arming fill carries it verbatim.
#: `off` = ⛔ THE CONTROL ARM [AMENDMENT 19c, the private design notes].
#: The adverse-latch ARMING is skipped for this book: no `_latch_adverse`, no ban, no deadline.
#: EVERYTHING ELSE IS UNTOUCHED — mark tripwire, width shield, `cooldown_until`, per-book caps,
#: cap-fills, durable loss cap and teardown all still govern. The TRIGGER is still EVALUATED and the
#: arming row is still TAPED (`latch_rule=off`, `latch_ban_s` blank).
#: ⛔ IT IS NOT `--adverse-cooldown-s 0`, which kills the MARK TRIPWIRE too.
#: ⛔⛔ IT IS A TAPE TAG AND A PER-SLUG SET — **NOT** A `latch.rule` VALUE, and the loader REFUSES it
#: as one: admitting it made `latch.rule: "off"` a whole-run disarm past the 0.5 `control_share`
#: ceiling, printed the wrong rail scope in the banner, and filed a fully-unlatched night into the
#: PROTECTED arm. The ONLY way off is per-cell (`latch.control_share` draws, `--latch-off-slugs`
#: carries), so the ceiling, the tape and the banner all see it.
LATCH_RULE_LIFETIME = "kill_reverted_lifetime"
LATCH_RULE_RAND_900_1800 = "rand900_1800"
#: ⛔ RETIRED AS A RUN RULE. The tag stays a CONSTANT because the
#: 2026-09-02 cycle-1 fills tape carries it — but it is no longer LOADABLE: it used to load and
#: fall down the `kill_reverted_lifetime` branch, so the arm the tape named and the rail the run
#: served disagreed row by row. It refuses BY NAME instead.
LATCH_RULES_RETIRED: frozenset[str] = frozenset({LATCH_RULE_RAND_900_1800})
#: ⛔ THE RE-REGISTERED FOUR-ARM DRAW [operator-ordered 2026-09-02,
#: the private design notes]. Same draw shape as `rand900_1800` — uniform
#: over `bot.poly_us.maker.LATCH_BAN_ARMS_S`, INDEPENDENT OF SEVERITY — but over FOUR log-spaced
#: arms {60, 180, 600, 1800} s: the estimand is a CURVE, re-entry-window price P&L on log(ban).
LATCH_RULE_RAND_60_180_600_1800 = "rand60_180_600_1800"

#: ⛔ THE TWO-ARM CURVE [operator-decided 2026-09-04, the private design notes § 6].
#: Same draw shape, over {180, 1800} s: FOUR levels are structurally unreachable at ~5 usable
#: episodes a night with 53% censored. ⛔ The four-arm tag is NOT retired — this is an ADDITION.
LATCH_RULE_RAND_180_1800 = "rand180_1800"
LATCH_RULE_OFF = "off"
#: ⛔ `LATCH_RULE_OFF` IS NOT IN HERE. See its comment — it is a tape tag, not a run rule.
#: ⛔ THE LOADABLE RUN RULES. `LATCH_RULES_RETIRED` is deliberately NOT unioned in.
LATCH_RULES: frozenset[str] = frozenset({LATCH_RULE_LIFETIME,
                                         LATCH_RULE_RAND_60_180_600_1800,
                                         LATCH_RULE_RAND_180_1800})

#: ⛔⛔ **THE ARMS A RANDOM RULE'S TAG PROMISES, AND THE LOADER ENFORCES THE MATCH.** The maker draws
#: from ONE module constant (`bot.poly_us.maker.LATCH_BAN_ARMS_S`) that `latch.ban_arms_s` MIRRORS,
#: so a random tag whose registered arms are not the file's would tape one arm set while the run
#: served another. The loader REFUSES that pair by name; a tag added here is not runnable until the
#: maker's own constant is flipped to its arms — a money-path change and an operator act.
#: ⛔⛔ **THE REGISTERED CLUSTER FLOOR FOR THE BAN CURVE** — the TRIAL's number, not the analysis
#: tool's default, which the curve reader used to borrow. ⛔ The
#: CI is still printed from 5 clusters, labelled **PROVISIONAL**: the floor gates the VERDICT.
LATCH_MIN_CLUSTERS_PER_ARM = 10

LATCH_BAN_ARMS_BY_RULE: dict[str, tuple[float, ...]] = {
    LATCH_RULE_RAND_60_180_600_1800: (60.0, 180.0, 600.0, 1800.0),
    LATCH_RULE_RAND_180_1800: (180.0, 1800.0),
}

#: ⛔ THE PER-CELL CONTROL SHARE'S CEILING [AMENDMENT 19c]. The control arm FORGOES the latch's
#: protection on the cells it draws, so its share is bounded at half the slate; the re-reg harm cap
#: is denominated per NIGHT. A fat-fingered `"0.9"` refuses at load.
LATCH_CONTROL_SHARE_CEILING = Decimal("0.5")

#: ⛔ THE SUB-CONTRACT CLOSE LANE — a CLOSED vocabulary, fail-closed, same shape as
#: `latch.rule`.
#: ⛔ **`off` IS NOT BYTE-FOR-BYTE — one change is UNGATED, and it moves in BOTH directions.** The
#: per-market `minimumTradeQty` replaced the global 0.01 at order time regardless of this switch:
#:   · a book whose venue minimum is ABOVE 0.01 (**1** on 684 `tec-*` futures) now REPORTS a residue
#:     that used to be sent and refused — strictly fewer wasted requests;
#:   · a book whose venue minimum is BELOW 0.01 now SENDS a short-dust close the old code refused —
#:     benign by construction (passive, post-only, an EXACT reduce bounded by |inv|). **Deliberately
#:     NOT clamped to 0.01.**
#: `off` = the pre-build behaviour: a sub-contract LONG residue is REPORTED and never placed, and a
#: stood-down book holding dust quotes NOTHING. `on` = both become a PASSIVE, POST-ONLY, REDUCING
#: order sized to the EXACT fraction held (`ORDER_INTENT_SELL_LONG` long, `BUY_LONG` short).
#: ⛔ THE EVIDENCE GAP IS STATED RATHER THAN CLOSED: a fractional `SELL_LONG` has NEVER BEEN SENT by
#: this repo — the 2026-08-06 box probe echoed fractional quantities under `BUY_LONG` only, so until
#: the first fill every `sell_long` order is `[UNVERIFIED-BY-FILL]`. The taker alternative was
#: audited and rejected (the private design notes § Verdict).
#: ⛔ SHIPPED `on` PER THE OPERATOR ORDER (revert = this line to `off` plus a per-run go); an
#: UNKNOWN STRING REFUSES rather than falling back either way.
FRACTIONAL_CLOSE_ON = "on"
FRACTIONAL_CLOSE_OFF = "off"
FRACTIONAL_CLOSE_MODES: frozenset[str] = frozenset({FRACTIONAL_CLOSE_ON, FRACTIONAL_CLOSE_OFF})

#: ⛔ THE PICK-TIME QUOTABILITY GATE — a CLOSED vocabulary, fail-closed, same shape as
#: `latch.rule` / `fractional_close` [AMENDMENT 19a, operator-ordered 2026-09-02]. `on` = the gate
#: screens candidates at pick time; `off` = the pick is BYTE-IDENTICAL to the pre-build one.
#: ⛔ AN UNKNOWN STRING REFUSES rather than falling back either way.
#:
#: ⛔⛔ THE TOUCH-WIDTH CRITERION WAS PROPOSED AND IS **REFUTED** — DO NOT RE-ADD IT. A draft carried
#: `max_touch_ticks` (refuse a book whose duration-weighted median touch width is ≤ 1 tick, since
#: `pick_quotes` cannot IMPROVE a one-tick touch). The reasoning is correct and the SCREEN INVERTED
#: [night read]: the median touch width was 1 tick on every seated earner, so a width screen
#: would have de-seated nearly every cell including every earner, and the dead seats it passed
#: died of NO FLOW. FLOW separates dead from live, not WIDTH. The private suite pins its ABSENCE.
QUOTABILITY_ON = "on"
QUOTABILITY_OFF = "off"
QUOTABILITY_MODES: frozenset[str] = frozenset({QUOTABILITY_ON, QUOTABILITY_OFF})

#: ⛔ AMENDMENT 54 — `lane.burst_gate`: does the burst gate RUN at all. `off` means no gate
#: process, no drained start for a gated cell, every cell `normal` at admit. Fail-closed like
#: every other mode vocabulary: an unrecognised value refuses rather than resolving either way.
#: ⛔ ABSENT = `BURST_GATE_ON`, the pre-amendment line, so an older file behaves as it did.
BURST_GATE_ON = "on"
BURST_GATE_OFF = "off"
BURST_GATE_MODES: frozenset[str] = frozenset({BURST_GATE_ON, BURST_GATE_OFF})

#: The sub-keys of `lane.quotability_gate`. ⛔ SPELLED HERE AS THE OTHER HALF OF THE SCHEMA, like
#: `_REQUIRED`: unknown and missing sub-keys both refuse — and that refusal is what kills a re-added
#: width knob, which fails the LAUNCH by name rather than being silently ignored.
_QUOTABILITY_KEYS: tuple[str, ...] = ("mode", "window_s", "min_two_sided_share",
                                      "min_arrivals_per_h")

#: ⛔ OUTER BOUND ON A TYPO for the arrivals floor, in the spirit of `FLOW_FLOOR_MULT_CEILING`. The
#: registered bar is 100/h against measured earners at 650–1,704/h and dead seats at 2–18/h, so
#: anything near this ceiling would refuse the whole slate and end an evening silently.
QUOTABILITY_MAX_ARRIVALS_FLOOR = 2000

#: Source labels for the banner.
#: ⛔ `SRC_DEFAULT` is for a banner row that is NOT a registered key — a value the calling tool
#: resolved in code. Every key in `_REQUIRED` reads `file` or `cli`, never `default`: absence is not
#: a value here.
SRC_FILE = "file"
SRC_CLI = "cli"
SRC_DEFAULT = "default"


class ProbeConfigError(ValueError):
    """`the probe registration file` is missing, unreadable or untrustworthy. ⛔ NEVER caught into a fallback."""


@dataclass(frozen=True)
class SizePlan:
    """⛔ AMENDMENT 34 — `lane.size_plan`: THE SIZE PER BOOK, one registration.

    Replaces `lane.size` + `lane.size_mode` + `lane.size_registered` (all three REMOVED from the
    file; the loader refuses them as unknown keys, which is the operator's typo guard). One value
    per level, resolved slug → class → default by `value_for`; a LIST draws, a SCALAR is fixed.
    """
    default: PlanValue
    #: `scripts.poly_probe_night.CELL_CLASSES` name → its plan value.
    by_class: Mapping[str, PlanValue]
    #: Book slug → its plan value. ⛔ A `lane.fixed_seats` entry for the same slug WINS (its size
    #: is the operator's explicit number); the loader refuses the two DISAGREEING about one slug.
    by_slug: Mapping[str, PlanValue]

    def value_for(self, slug: str, cls: str) -> PlanValue:
        """The registered plan value for one cell — slug beats class beats default."""
        if slug in self.by_slug:
            return self.by_slug[slug]
        if cls in self.by_class:
            return self.by_class[cls]
        return self.default

    def values(self) -> tuple[PlanValue, ...]:
        """Every registered value, for the rails that must see them all (MIN_SIZE, the ceiling)."""
        return (self.default, *self.by_class.values(), *self.by_slug.values())

    @property
    def nominal(self) -> int:
        """⛔ THE ONE COMPARABLE LANE SIZE, for a SCORE — never a seat's size. The pick score
        prices a rebate at one size across the whole candidate pool, and an unchosen candidate has
        no drawn size. The DEFAULT plan's smallest rung: the conservative reading of the ladder."""
        return self.default if isinstance(self.default, int) else min(self.default)


@dataclass(frozen=True)
class FixedSeat:
    """⛔ AMENDMENT 31 — ONE OPERATOR-DECLARED SEAT, seated in every window IN ADDITION to the
    auto-pick the private design notes.

    ⛔ IT IS THE OPERATOR'S NUMBER, NOT A DRAW. `size` is written here, so no `lane.size_plan`
    draw ever touches a fixed seat. ⛔ AMENDMENT 32 — AND IT IS NOT
    LADDER-VALIDATED (an earlier cut refused an off-ladder declared size at parse time; that check
    is DELETED). ⛔ AMENDMENT 34 — it WINS over `size_plan.by_slug`, and the loader REFUSES a slug
    named in both (the seat expires, a by_slug entry does not). Resolve it through `seat_size`,
    and let the safety rails bound it.

    ⛔ THERE IS NO PER-SEAT `requote_s`, and there must not be one until the maker has per-book
    cadence: `poly_live_mm.py:1014 --requote-s` is ONE float and the quote loop sleeps ONE cadence
    (`poly_live_mm.py:1900`), so a declared 10 s on a 4 s lane would be quoted every 4 s and the
    request-rate rail would under-count by exactly that lie. A fixed seat rides the lane cadence.
    """
    slug: str
    #: ⛔ Decimal FROM ITS STRING FORM [CLAUDE.md § Code style] — it is a size that spends money.
    size: Decimal
    #: `""` = no park declaration (quote both sides like any picked cell). Otherwise `bid`/`ask`
    #: plus the tick offset, i.e. `--park-seat SLUG:SIDE:TICKS` moved into config.
    park_side: str = ""
    park_ticks: Decimal | None = None
    #: ⛔ THE REGISTRATION'S OWN EXPIRY — an ISO date (`YYYY-MM-DD`, UTC), or `""` for none. A
    #: pilot is a dated thing and a config entry is not: without this, three registered evenings
    #: become an indefinite seat the day nobody remembers to delete the line. Past it the entry is
    #: IGNORED with a banner line, never silently.
    until: str = ""

    def expired_at(self, window_start: float) -> bool:
        """Is this registration past its `until` date, for an evening OPENING at `window_start`?
        `""` never expires.

        ⛔ THE KEY IS THE EVENING'S OPENING DATE, NOT `now`. Every
        probe evening crosses midnight UTC, so an expiry evaluated against a mid-window clock
        would drop the seats out from under a running maker — and it would do it at 00:00Z, in the
        middle of the night the registration exists to produce. An evening that OPENS on `until`'s
        own date runs to completion, whatever the clock says later.

        ⛔ `until` IS INCLUSIVE: it names the LAST evening that may open. Expired iff the opening
        date is strictly after it.
        """
        if not self.until:
            return False
        opened = dt.datetime.fromtimestamp(window_start, dt.timezone.utc).date()
        return opened > dt.date.fromisoformat(self.until)


def seat_size(slug: str, cls: str, entry: Optional[FixedSeat], cfg: "ProbeConfig",
              rng: Optional[random.Random] = None) -> tuple[int, str]:
    """⛔ AMENDMENT 34 — THE ONE PLACE ANY SEAT'S SIZE IS RESOLVED, auto-picked or declared.
    Returns `(contracts, size_rule)`; nobody reads `FixedSeat.size` or a plan value directly.

    ⛔ A DECLARED SEAT (`entry`) WINS: its `size` is the operator's explicit number and outranks
    `lane.size_plan.by_slug` — which the loader refuses to co-exist with for the same slug, so the
    precedence is never silent. Otherwise the plan resolves slug → class → default, and a
    LIST value DRAWS uniformly — ⛔ NOTHING ABOUT THE CELL IS READ IN THE DRAW. That is the
    experiment (AMENDMENT 16, generalised).

    ⛔ THE RNG IS AN ARGUMENT, not a module global: the draw is deterministic under test and one
    evening's assignments come off one auditable stream.

    ⛔ IT IS THE HOOK, deliberately EMPTY of policy beyond the registered plan: risk-based sizing —
    a seat sized off its own carry or the evening's loss-cap headroom — belongs HERE, behind this
    signature, so that no consumer has to change. NONE OF THAT IS BUILT. What BOUNDS the number is
    the SAFETY rails (`maker.MIN_SIZE` via `unlaunchable_refusals`, `SIZE_CEILING` at parse time,
    `--max-total-contracts`, the lawful-reach gate, the cells/second rail, both loss caps).
    """
    if entry is not None:
        return int(entry.size), size_rule_for(int(entry.size))
    value = cfg.size_plan.value_for(slug, cls)
    if isinstance(value, int):
        return value, size_rule_for(value)
    rng = random.Random() if rng is None else rng
    return int(rng.choice(value)), size_rule_for(value)


@dataclass(frozen=True)
class ProbeConfig:
    """The resolved registry, and WHERE each value's authority comes from."""
    #: ⛔ AMENDMENT 34 — the ONE size registration, per book. Resolve THROUGH `seat_size`.
    size_plan: SizePlan
    cap_fills: int
    cap_fills_registered: frozenset[int]
    #: ⛔ AMENDMENT 31 — this bounds the AUTO-PICK ALONE. The seats an evening composes are
    #: `max_cells + len(fixed_seats)`: fixed seats are ADDITIVE while that sum fits
    #: `max_cells_per_second` at every drawable arm; when it does not, the loader REFUSES at parse
    #: time and the operator adds a seat by amending the ceiling, never by lowering this number.
    max_cells: int
    #: ⛔ AMENDMENT 31 — `lane.fixed_seats` AS REGISTERED, in file order, expired entries
    #: INCLUDED. ⛔ EVERY SEATING PATH MUST GO THROUGH `active_fixed_seats`, never this tuple: an
    #: expired entry read raw still supersedes an auto-picked book, and then nobody seats it
    #:. This field exists for the banner and for provenance.
    fixed_seats: tuple[FixedSeat, ...]
    #: ⛔ AMENDMENT 31 — the largest `total seats ÷ requote_s` this lane may compose, in
    #: quote-cycles per second. A rate compared against a threshold ⇒ `Decimal` from its string
    #: form. Owned here rather than in `poly_probe_night.MAX_CELLS_PER_SECOND` so the parse-time
    #: rail and the composed-line rail read ONE number.
    max_cells_per_second: Decimal
    max_seconds: int
    loss_cap: Decimal
    requote_s: float
    requote_arms_registered: tuple[float, ...]
    order_ttl_s: float | None
    #: ⛔ AMENDMENT 17 — the MULTIPLIER on the burst gate's own `DEFAULT_MIN_PRINTS`, never the bar
    #: itself; registering the product here would be the second copy the amendment exists to delete.
    flow_floor_gate_mult: int
    #: One of `FRACTIONAL_CLOSE_MODES` — whether the maker may PLACE sub-contract reducing orders.
    fractional_close: str
    #: ⛔ AMENDMENT 19a — the PICK-TIME QUOTABILITY GATE, flattened out of the file's nested
    #: `lane.quotability_gate`. One of `QUOTABILITY_MODES` plus its three thresholds: ONE
    #: registration, because a gate without thresholds half-registers a screen that spends money.
    quotability_mode: str
    quotability_window_s: float
    #: ⛔ A RATIO COMPARED AGAINST A THRESHOLD ⇒ `Decimal` FROM ITS STRING FORM [CLAUDE.md
    #: § Code style]. A JSON number would reach this loader as a float.
    quotability_min_two_sided_share: Decimal
    #: ⛔ THE ARRIVALS FLOOR, in aggressor arrivals per HOUR over the gate's window; below it a
    #: candidate is refused as no-flow. ⚠️ FIT ON ITS OWN MOTIVATING DATA (a single night's cells)
    #: and registered as PROVISIONAL.
    quotability_min_arrivals_per_h: int
    #: ⛔ AMENDMENT 54 — one of `BURST_GATE_MODES`. `off` = the burst gate does not run and every
    #: probe cell seats `normal` at admit (`poly_probe_night.cell_gated` is the one consumer).
    burst_gate_mode: str
    cycle_seconds: int
    #: ⛔ THE RESEAT PERIOD [continuous maker P3] — how often the ONE day-long child's book set is
    #: re-picked and rewritten through the hot slate. `0` DISARMS reseating entirely and the
    #: evening is the hourly relaunch loop, byte for byte.
    reseat_s: int
    #: Run `scripts.kalshi_book_watch` as a third supervisor sidecar beside every window
    #: (the <deploy unit> line). Read-only tape; never fatal.
    kalshi_sidecar: bool
    #: One of `LATCH_RULES` — the probe lane's adverse-latch SCOPE. See the constants.
    latch_rule: str
    latch_ban_arms_s: tuple[float, ...]
    #: ⛔ AMENDMENT 19c — the probability that a PICKED cell is drawn into the LATCH-ONLY CONTROL
    #: arm (`latch_arm=off`), a `Decimal` FROM ITS STRING FORM in [0, `LATCH_CONTROL_SHARE_CEILING`];
    #: it is compared against a drawn variate that decides whether real money runs unprotected.
    #: ⛔ SHIPPED `0` = NO CONTROL CELLS EVER, and the draw is never reached — the composed maker
    #: line is byte-identical to the pre-amendment one. Arming is an operator edit plus a per-run go.
    latch_control_share: Decimal
    #: AMENDMENT 37 — registered cell-class names whose SEATED cells run with the adverse latch
    #: NOT ARMED (`--latch-off-slugs`, tagged `latch_arm_rule=off_class` on the picks tape —
    #: NOT the randomized `control_share` tag, so the control estimate stays a random draw).
    #: Validated against the class registry by `poly_probe_night` (the regexes live there);
    #: an unknown name matches no book and WARNS — the latch stays ON, the safe direction.
    latch_off_classes: tuple[str, ...]
    #: AMENDMENT 41 — registered cell-class names whose SEATED cells run with the GAME-CLOCK GUARD
    #: armed (`--clock-guard-slugs`): reduce-only in the game's final stretch
    #: (`bot/poly_us/maker.py:clock_guard_active`). The derivative families (asc-/tsc-/astatc-/atc-);
    #: moneyline is deliberately absent. Validated against the class registry by
    #: `poly_probe_night`; an unknown name matches no book and WARNS.
    clock_guard_classes: tuple[str, ...]
    admission_groups: dict[str, int]
    admission_class_refused: frozenset[str]
    #: AMENDMENT 24 — class-name → the number of seats that class may hold PER PICK. A name here
    #: is ADMITTED at that cap; `class_refused` membership is the contradiction (one rule per
    #: class) and the loader refuses it. Absent = the class is not an exploration class.
    #: ⛔ ONE KEY IS NOT A CLASS: `EXPLORE_SCORE_DISAGREEMENT` [AMENDMENT 42] caps the SHADOW
    #: score's disagreement seat, not a sport — see that constant.
    admission_explore: dict[str, int]
    #: AMENDMENT 27 — the BACKFILL path's floor: a backfill candidate must show at least this many
    #: prints over the PICK LOOKBACK (`scripts.poly_probe_night.PICK_WINDOW_S`), never the flow
    #: floor's shorter burst-gate window. The FILTER path does not read it.
    backfill_min_prints: int
    #: AMENDMENT 27 — classes that are ADMITTED on the filter path and never BACKFILLED.
    backfill_class_exclude: frozenset[str]
    #: ⛔ THE `launch` SECTION, FLATTENED — the per-night TRIAL FLAGS' defaults, one field per
    #: `poly_probe_supervisor` flag. No `launch` block resolves these to `LAUNCH_DEFAULTS`.
    #: ⛔ DEFAULTS, not decisions: an explicit flag on the supervisor line overrides each (file < CLI).
    launch_improve_ab: bool
    #: AMENDMENT 23 — the per-RUN requote-cadence A/B, and its two registered arms in seconds.
    launch_requote_ab: bool
    launch_requote_arms_s: tuple[Decimal, ...]
    launch_rebate_step: bool
    #: One of `REBATE_STEP_MODES`, or `None` = say nothing and leave the maker's own default.
    launch_rebate_step_mode: str | None
    launch_rebate_step_max: int | None
    launch_depth_sidecar: bool
    launch_gamestate_sidecar: bool
    #: ⛔ NOT a supervisor flag (it has no `LAUNCH_FLAGS` entry): `poly_probe_night.maker_argv`
    #: reads it directly to decide whether the probe line carries `--conditional-poll`.
    launch_conditional_poll: bool
    #: Absolute path the values came from — printed, journaled, and asserted on by the wiring tests.
    path: str
    #: `"<section>.<key>"` → amendment tag. Every required key has an entry.
    provenance: dict[str, str]

    def tag(self, dotted: str) -> str:
        """The amendment tag behind one key, for a banner or a refusal message."""
        return self.provenance[dotted]

    def active_fixed_seats(self, window_start: float) -> tuple[FixedSeat, ...]:
        """The declared seats an evening OPENING at `window_start` may seat [AMENDMENT 31].

        ⛔ THE ONE ENTRY POINT FOR EVERY SEATING DECISION — the supersession set, the seat
        builder, the park declaration and the banner all ask this, never `fixed_seats` directly.
        One caller reading the raw tuple is enough to break it: an expired entry that still
        SUPERSEDES an auto-picked book takes that book off the slate and then seats nothing in its
        place, which is strictly worse than either outcome.

        ⛔ THE ARGUMENT IS THE EVENING'S OPENING INSTANT, not `now` — see `FixedSeat.expired_at`.
        """
        return tuple(s for s in self.fixed_seats if not s.expired_at(window_start))


def _warn(text: str) -> None:
    """One operator-facing line on stderr [AMENDMENT 33]: a SHAPE finding the loader states and
    loads through. Issue only — every refusal in this file is still a refusal."""
    print(f"⚠️  {text}", file=sys.stderr)


def _require_section(path: str, raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    if name not in raw:
        raise ProbeConfigError(
            f"{path}: missing required section {name!r} — state it explicitly. Absence is never "
            f"a value: this loader has no built-in defaults to fall back to.")
    section = raw[name]
    if not isinstance(section, dict):
        raise ProbeConfigError(f"{path}: section {name!r} must be a JSON object, got "
                               f"{type(section).__name__}")
    known = set(_REQUIRED[name]) | set(_OPTIONAL.get(name, ())) | set(_META)
    unknown = sorted(set(section) - known)
    if unknown:
        raise ProbeConfigError(
            f"{path}: section {name!r} has unknown key(s) {unknown} — expected "
            f"{sorted(_REQUIRED[name])}. A misspelled key would drop the value it was meant to "
            f"set while looking like a config that has it.")
    for key in _REQUIRED[name]:
        if key not in section:
            raise ProbeConfigError(f"{path}: section {name!r} is missing required key {key!r} — "
                                   f"state it explicitly, even when null/empty.")
    return section


def _launch(path: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """The OPTIONAL `launch` section, resolved to one value per `_LAUNCH_TYPES` key.

    ⛔ ABSENT SECTION = `LAUNCH_DEFAULTS`, and an absent KEY inside a present section takes its
    default too. ⛔ EVERYTHING ELSE FAILS CLOSED: an unknown key REFUSES, a wrong type REFUSES
    (`"true"` is not `true`, `1` is not a bool), an unregistered `rebate_step_mode` REFUSES."""
    out = dict(LAUNCH_DEFAULTS)
    if "launch" not in raw:
        return out
    section = raw["launch"]
    if not isinstance(section, dict):
        raise ProbeConfigError(f"{path}: section 'launch' must be a JSON object, got "
                               f"{type(section).__name__}")
    unknown = sorted(set(section) - set(_LAUNCH_TYPES) - set(_META))
    if unknown:
        raise ProbeConfigError(
            f"{path}: section 'launch' has unknown key(s) {unknown} — expected "
            f"{sorted(_LAUNCH_TYPES)}. A misspelled key would drop the trial it was meant to arm "
            f"while looking like a config that arms it.")
    for key, want in _LAUNCH_TYPES.items():
        if key not in section:
            continue
        value = section[key]
        # `rebate_step_mode` and `rebate_step_max` accept an explicit null = "say nothing".
        if value is None and LAUNCH_DEFAULTS[key] is None:
            out[key] = None
            continue
        # ⛔ `bool` IS A SUBCLASS OF `int` — `"rebate_step_max": true` must not read as 1.
        if not isinstance(value, want) or (want is int and isinstance(value, bool)):
            raise ProbeConfigError(
                f"{path}: launch.{key} must be {want.__name__}, got {value!r} "
                f"({type(value).__name__}). The vocabulary is CLOSED: a value of the wrong type "
                f"must never be coerced into arming (or disarming) a trial.")
        out[key] = value
    # ⛔ AMENDMENT 23 — the requote arm SET is STRINGS → `Decimal` (a cadence is money-adjacent
    # registered text, and a JSON float would arrive already rounded). ⛔ EXACTLY TWO when the
    # A/B is armed: the between-cycle harm cap is a PAIRWISE contrast, so a third arm would be
    # drawn and then never scored against anything.
    raw_arms = out["requote_arms_s"]
    if not all(isinstance(a, str) for a in raw_arms):
        raise ProbeConfigError(
            f"{path}: launch.requote_arms_s must be a list of decimal STRINGS (e.g. "
            f'["6", "10"]), got {raw_arms!r}')
    try:
        arms = tuple(Decimal(a) for a in raw_arms)
    except InvalidOperation:
        raise ProbeConfigError(f"{path}: launch.requote_arms_s has a value that is not a "
                               f"decimal number: {raw_arms!r}")
    if any(a <= 0 for a in arms) or len(set(arms)) != len(arms):
        raise ProbeConfigError(f"{path}: launch.requote_arms_s must be positive and distinct, "
                               f"got {raw_arms!r}")
    if out["requote_ab"] and len(arms) != 2:
        raise ProbeConfigError(
            f"{path}: launch.requote_ab is on but launch.requote_arms_s names {len(arms)} arm(s) "
            f"{raw_arms!r} — the harm cap differences TWO arms over matched books. Register "
            f"exactly two.")
    out["requote_arms_s"] = arms
    mode = out["rebate_step_mode"]
    if mode is not None and mode not in REBATE_STEP_MODES:
        raise ProbeConfigError(
            f"{path}: launch.rebate_step_mode {mode!r} is not one of {sorted(REBATE_STEP_MODES)} "
            f"(bot.poly_us.maker.REBATE_STEP_MODES). Fail-closed in BOTH directions: a typo must "
            f"not resolve to 'floor' (which leaves a paying base alone) nor to 'optimal' (which "
            f"re-sizes nearly every mid-band adding side). Spell it exactly.")
    if out["rebate_step_max"] is not None and out["rebate_step_max"] < 1:
        raise ProbeConfigError(
            f"{path}: launch.rebate_step_max must be a positive contract count or null, got "
            f"{out['rebate_step_max']!r}")
    return out


def _clock_guard(path: str, raw: Mapping[str, Any],
                 provenance: dict[str, str]) -> tuple[str, ...]:
    """The OPTIONAL `clock_guard` section → the guarded CLASS names [AMENDMENT 41, 2026-09-10].

    ⛔ ABSENT SECTION = NO GUARDED CLASS, which is the pre-amendment line and composes a
    byte-identical maker command. ⛔ EVERYTHING PRESENT FAILS CLOSED, exactly as `latch.off_classes`
    does: an unknown key REFUSES, a non-string list REFUSES, and the key owes its provenance tag.
    ⛔ The names are validated against the CELL-CLASS REGISTRY by `poly_probe_night` (the regexes
    live there); an unknown name matches no book and WARNS — nothing is guarded under it.
    """
    section = raw.get("clock_guard")
    if section is None:
        return ()
    if not isinstance(section, dict):
        raise ProbeConfigError(f"{path}: section 'clock_guard' must be a JSON object, got "
                               f"{type(section).__name__}")
    unknown = sorted(set(section) - set(_OPTIONAL["clock_guard"]) - set(_META))
    if unknown:
        raise ProbeConfigError(
            f"{path}: section 'clock_guard' has unknown key(s) {unknown} — expected "
            f"{sorted(_OPTIONAL['clock_guard'])}. A misspelled key would leave every book "
            f"UNGUARDED while looking like a config that guards them.")
    _provenance(path, "clock_guard", section, provenance)
    return _str_list(path, "clock_guard.classes", section.get("classes", []))


def _provenance(path: str, name: str, section: Mapping[str, Any],
                into: dict[str, str]) -> None:
    """Every required key resolves an amendment tag, or the file is refused. ⛔ Per-key `_amendments`
    overrides the section's `_amendment`; a value nobody can attribute to a decision is not
    registered."""
    per_key = section.get("_amendments", {})
    if not isinstance(per_key, dict) or not all(
            isinstance(k, str) and isinstance(v, str) and v.strip() for k, v in per_key.items()):
        raise ProbeConfigError(f"{path}: {name}._amendments must be an object of "
                               f"key → non-empty amendment tag, got {per_key!r}")
    # ⛔ `_REQUIRED.get`, not `_REQUIRED[...]` [AMENDMENT 41]: `clock_guard` is an OPTIONAL SECTION
    # with no required keys, and it still owes provenance for the key it does carry.
    stray = sorted(set(per_key) - set(_REQUIRED.get(name, ())) - set(_OPTIONAL.get(name, ())))
    if stray:
        raise ProbeConfigError(f"{path}: {name}._amendments tags {stray}, which are not keys of "
                               f"this section — a tag on a key that does not exist attributes "
                               f"nothing.")
    section_tag = section.get("_amendment", "")
    if not isinstance(section_tag, str):
        raise ProbeConfigError(f"{path}: {name}._amendment must be a string tag")
    # ⛔ An OPTIONAL key needs its tag only when it is PRESENT — an absent one registers nothing.
    for key in tuple(_REQUIRED.get(name, ())) + tuple(
            k for k in _OPTIONAL.get(name, ()) if k in section):
        tag = per_key.get(key) or section_tag
        if not tag.strip():
            raise ProbeConfigError(
                f"{path}: {name}.{key} carries no provenance — give the section an "
                f"'_amendment' tag or name this key in '_amendments'. The file IS the "
                f"registration surface, and an untagged value is not a registration.")
        into[f"{name}.{key}"] = tag


def _int(path: str, where: str, value: Any, *, minimum: int = 1,
         maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProbeConfigError(f"{path}: {where} must be an integer, got {value!r}")
    if value < minimum:
        raise ProbeConfigError(f"{path}: {where} must be ≥ {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ProbeConfigError(
            f"{path}: {where} {value} is above the hard ceiling {maximum}. That ceiling is an "
            f"outer bound on a typo, not a policy — no amendment has ever registered a value "
            f"near it, so a number this large is a fat finger. Raising it is a code change AND "
            f"an amendment, deliberately.")
    return value


def _ladder(path: str, where: str, value: Any, *, default: int,
            maximum: int | None = None) -> frozenset[int]:
    """A REGISTERED LADDER of integer rungs, containing the default it governs. ⛔ A default outside
    its own ladder means every launch refuses its own default.

    ⛔ `maximum` IS THE TYPO CEILING ON A RUNG. The ladder is the
    operator's to widen (AMENDMENT 33 warns instead of refusing off it), so the rung itself is now
    the only place a mistyped size can be caught before every derived rail scales with it."""
    if (not isinstance(value, list) or not value
            or not all(isinstance(v, int) and not isinstance(v, bool) and v >= 1 for v in value)):
        raise ProbeConfigError(f"{path}: {where} must be a non-empty list of positive integers "
                               f"(the registered LADDER), got {value!r}")
    if maximum is not None and max(value) > maximum:
        raise ProbeConfigError(
            f"{path}: {where} rung {max(value)} is above the hard ceiling {maximum}. That ceiling "
            f"is an outer bound on a typo, not a policy — every rail on a size is DERIVED from "
            f"the number and would scale with a fat finger instead of bounding it.")
    if default not in value:
        raise ProbeConfigError(
            f"{path}: the default {default} is not in {where} {sorted(value)} — the default "
            f"must itself be a registered rung, or every launch refuses its own default.")
    return frozenset(value)


def _plan_value(path: str, where: str, value: Any) -> PlanValue:
    """One `lane.size_plan` value: a whole-contract SCALAR, or a non-empty LIST to draw from.

    ⛔ `SIZE_CEILING` BOUNDS EVERY RUNG [AMENDMENT 33's rail, kept]: every rail on a size
    (`--max-total-contracts`, the lawful reach, the per-book cap) is DERIVED from the number and
    would scale with a fat finger instead of bounding it."""
    values = value if isinstance(value, list) else [value]
    if (not values or not all(isinstance(v, int) and not isinstance(v, bool) and v >= 1
                              for v in values)):
        raise ProbeConfigError(
            f"{path}: {where} must be a whole number of contracts, or a non-empty list of them "
            f"to draw from, got {value!r}")
    if max(values) > SIZE_CEILING:
        raise ProbeConfigError(
            f"{path}: {where} value {max(values)} is above the hard ceiling {SIZE_CEILING}. That "
            f"ceiling is an outer bound on a typo, not a policy — every rail on a size is DERIVED "
            f"from the number and would scale with a fat finger instead of bounding it.")
    return tuple(sorted(values)) if isinstance(value, list) else int(value)


def _size_plan(path: str, value: Any) -> SizePlan:
    """`lane.size_plan` [AMENDMENT 34]. ⛔ THE SUB-KEY VOCABULARY IS CLOSED — a typo (`by_class`
    spelled `class`) would silently size every book at the default."""
    if not isinstance(value, dict):
        raise ProbeConfigError(f"{path}: lane.size_plan must be an object with "
                               f"{sorted(SIZE_PLAN_KEYS)}, got {value!r}")
    unknown = sorted(set(value) - SIZE_PLAN_KEYS)
    if unknown:
        raise ProbeConfigError(
            f"{path}: lane.size_plan has unknown key(s) {unknown} — the vocabulary is "
            f"{sorted(SIZE_PLAN_KEYS)} and a typo must never drop a sizing rule silently.")
    if "default" not in value:
        raise ProbeConfigError(f"{path}: lane.size_plan.default is REQUIRED — it is the size "
                               f"every book with no rule of its own is seated at.")
    levels: dict[str, dict[str, PlanValue]] = {}
    for level in ("by_class", "by_slug"):
        raw = value.get(level, {})
        if not isinstance(raw, dict):
            raise ProbeConfigError(f"{path}: lane.size_plan.{level} must be an object "
                                   f"(name → size), got {raw!r}")
        levels[level] = {str(k): _plan_value(path, f"lane.size_plan.{level}.{k}", v)
                         for k, v in raw.items()}
    return SizePlan(default=_plan_value(path, "lane.size_plan.default", value["default"]),
                    by_class=MappingProxyType(levels["by_class"]),
                    by_slug=MappingProxyType(levels["by_slug"]))


def _share(path: str, where: str, value: Any) -> Decimal:
    """A fraction in (0, 1], `Decimal` PARSED FROM ITS STRING FORM.

    ⛔ A JSON *number* is refused outright rather than coerced, for the `_money` reason exactly. This
    value is compared against a measured duty share on the money path."""
    if not isinstance(value, str):
        raise ProbeConfigError(
            f"{path}: {where} is a threshold compared against a measured ratio and must be a "
            f"STRING (e.g. \"0.50\"), got {value!r}. A JSON number reaches this loader as a "
            f"float, and Decimal(float) launders the float's error into the Decimal.")
    try:
        out = Decimal(value)
    except InvalidOperation as exc:
        raise ProbeConfigError(f"{path}: {where} is not a decimal number: {value!r}") from exc
    if not Decimal(0) < out <= Decimal(1):
        raise ProbeConfigError(f"{path}: {where} must be a share in (0, 1], got {value!r}")
    return out


def _str_list(path: str, where: str, value: Any) -> tuple[str, ...]:
    """A list of non-empty strings, or refuse."""
    if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
        raise ProbeConfigError(f"{path}: {where} must be a list of non-empty strings, got {value!r}")
    return tuple(v.strip() for v in value)


def _control_share(path: str, value: Any) -> Decimal:
    """`latch.control_share` — a probability in [0, `LATCH_CONTROL_SHARE_CEILING`].

    ⛔ A SEPARATE PARSER FROM `_share` [AMENDMENT 19c]: `_share`'s band is (0, 1], but here **0 is
    the SHIPPED, DISARMED value** and the top of the band is half the slate, so reusing `_share`
    would refuse the disarmed default and admit a 100%-control night. ⛔ STRING FORM: this value
    decides whether a real-money book runs with the adverse latch off."""
    if not isinstance(value, str):
        raise ProbeConfigError(
            f"{path}: latch.control_share is a probability compared against a drawn variate and "
            f"must be a STRING (e.g. \"0\" or \"0.25\"), got {value!r}. A JSON number reaches "
            f"this loader as a float, and Decimal(float) launders the float's error into the "
            f"Decimal.")
    try:
        out = Decimal(value)
    except InvalidOperation as exc:
        raise ProbeConfigError(
            f"{path}: latch.control_share is not a decimal number: {value!r}") from exc
    if not Decimal(0) <= out <= LATCH_CONTROL_SHARE_CEILING:
        raise ProbeConfigError(
            f"{path}: latch.control_share must be a probability in "
            f"[0, {LATCH_CONTROL_SHARE_CEILING}], got {value!r}. 0 is the shipped, DISARMED "
            f"value (no control cells are ever drawn); the ceiling is half the slate because a "
            f"control cell FORGOES the adverse latch, and a majority-control night is not a "
            f"control arm — it is an un-latched night.")
    return out


def _quotability(path: str, value: Any) -> tuple[str, float, Decimal, int]:
    """`lane.quotability_gate` → `(mode, window_s, min_two_sided_share, min_arrivals_per_h)`.

    ⛔ ONE KEY, FOUR VALUES, BY DESIGN [AMENDMENT 19a]: turning the screen on without stating what it
    screens on, or moving a threshold while the switch is off, half-registers something that decides
    whether real money is spent on a cell. ⛔ UNKNOWN AND MISSING SUB-KEYS BOTH REFUSE."""
    if not isinstance(value, dict):
        raise ProbeConfigError(
            f"{path}: lane.quotability_gate must be a JSON object carrying "
            f"{list(_QUOTABILITY_KEYS)}, got {type(value).__name__}")
    unknown = sorted(set(value) - set(_QUOTABILITY_KEYS))
    if unknown:
        raise ProbeConfigError(
            f"{path}: lane.quotability_gate has unknown key(s) {unknown} — expected "
            f"{list(_QUOTABILITY_KEYS)}. A misspelled key would drop the threshold it was meant "
            f"to set while looking like a config that has it.")
    missing = [k for k in _QUOTABILITY_KEYS if k not in value]
    if missing:
        raise ProbeConfigError(
            f"{path}: lane.quotability_gate is missing {missing} — state every threshold "
            f"explicitly, even when the gate is {QUOTABILITY_OFF!r}. Absence is never a value, "
            f"and a gate whose thresholds appear only when it is armed is a gate nobody can "
            f"review while it is off.")
    mode = value["mode"]
    if mode not in QUOTABILITY_MODES:
        raise ProbeConfigError(
            f"{path}: lane.quotability_gate.mode {mode!r} is not one of "
            f"{sorted(QUOTABILITY_MODES)}. The vocabulary is CLOSED and fail-closed: an "
            f"unrecognised mode must never resolve to {QUOTABILITY_OFF!r} (silently un-arming a "
            f"registered screen) nor to {QUOTABILITY_ON!r} (silently arming one). Spell it "
            f"exactly.")
    return (mode,
            _seconds(path, "lane.quotability_gate.window_s", value["window_s"]),
            _share(path, "lane.quotability_gate.min_two_sided_share",
                   value["min_two_sided_share"]),
            _int(path, "lane.quotability_gate.min_arrivals_per_h", value["min_arrivals_per_h"],
                 maximum=QUOTABILITY_MAX_ARRIVALS_FLOOR))


def _burst_gate(path: str, lane: Mapping[str, Any]) -> str:
    """`lane.burst_gate` → its mode. ⛔ ABSENT IS `BURST_GATE_ON` [AMENDMENT 54] — the one reading
    under which a file written before the key behaves exactly as it did."""
    value = lane.get("burst_gate", BURST_GATE_ON)
    if value not in BURST_GATE_MODES:
        raise ProbeConfigError(
            f"{path}: lane.burst_gate {value!r} is not one of {sorted(BURST_GATE_MODES)}. The "
            f"vocabulary is CLOSED and fail-closed: an unrecognised value must never resolve to "
            f"{BURST_GATE_OFF!r} (silently un-arming the gate) nor to {BURST_GATE_ON!r} "
            f"(silently arming it). Spell it exactly, or omit the key for {BURST_GATE_ON!r}.")
    return value


def _positive_decimal(path: str, where: str, value: Any) -> Decimal:
    """A positive `Decimal` PARSED FROM ITS STRING FORM — `_money`'s rule without the ceiling.
    A JSON *number* is refused outright rather than coerced, for `_money`'s reason exactly."""
    if not isinstance(value, str):
        raise ProbeConfigError(
            f"{path}: {where} must be a STRING (e.g. \"5\"), got {value!r}. A JSON number reaches "
            f"this loader as a float, and Decimal(float) launders the float's error into the "
            f"Decimal.")
    try:
        out = Decimal(value)
    except InvalidOperation as exc:
        raise ProbeConfigError(f"{path}: {where} is not a decimal number: {value!r}") from exc
    if out <= 0:
        raise ProbeConfigError(f"{path}: {where} must be positive, got {value!r}")
    return out


def _fixed_seats(path: str, value: Any) -> tuple[FixedSeat, ...]:
    """⛔ AMENDMENT 31 — `lane.fixed_seats`, validated whole or refused whole.

    ⛔ AMENDMENT 32 — A DECLARED SIZE IS NOT LADDER-VALIDATED. There is no
    membership check against `lane.size_plan` (which sizes the AUTO-PICKS) nor against
    any second registered list: the guardrails exist against ISSUES, not against the operator
    changing config. What still bounds a declared seat is the SAFETY rails — `maker.MIN_SIZE`,
    `--max-total-contracts` (Σ cell sizes × cap_fills), the lawful-reach gate, the cells/second
    rail and both loss caps. A non-integral size still refuses: that is a malformed value, not a
    choice.

    ⛔ AMENDMENT 33 — OPTIONAL: an ABSENT key reads as NO seats, which is the
    disarmed state and the fail-safe direction. ⛔ AN UNKNOWN SUB-KEY STILL REFUSES [operator
    2026-09-09, clarified: "a typo should refuse — that could be bad"]: the vocabulary is CLOSED,
    and a key this loader ignores is a value the operator believes is registered.

    ⛔ A MALFORMED ENTRY REFUSES THE WHOLE LIST [design doc § 2 Phase 1]: seating a partial list
    would run an evening the operator never declared, and the entries are not independent (they
    share the rail and the seat count).
    """
    if not isinstance(value, list):
        raise ProbeConfigError(f"{path}: lane.fixed_seats must be a JSON list (shipped `[]` = no "
                               f"operator-declared seats), got {value!r}")
    out: list[FixedSeat] = []
    seen: set[str] = set()
    for i, raw in enumerate(value):
        where = f"lane.fixed_seats[{i}]"
        if not isinstance(raw, dict):
            raise ProbeConfigError(f"{path}: {where} must be a JSON object, got {raw!r}")
        unknown = sorted(set(raw) - FIXED_SEAT_KEYS)
        if unknown:
            raise ProbeConfigError(
                f"{path}: {where} carries unknown key(s) {unknown} — the entry vocabulary is "
                f"{sorted(FIXED_SEAT_KEYS)} and it is CLOSED. A key this loader ignores is a "
                f"value the operator believes is registered and that nothing enforces. ⛔ There "
                f"is deliberately NO per-seat 'requote_s': the maker has one cadence for the "
                f"whole line, so a per-seat one would be a number the rail counts and the venue "
                f"never sees.")
        until = raw.get("until", "")
        if not isinstance(until, str):
            raise ProbeConfigError(f"{path}: {where}.until must be an ISO date STRING "
                                   f"\"YYYY-MM-DD\" (UTC, inclusive), got {until!r}")
        if until:
            try:
                dt.datetime.strptime(until, "%Y-%m-%d")
            except ValueError as exc:
                raise ProbeConfigError(
                    f"{path}: {where}.until {until!r} is not an ISO date \"YYYY-MM-DD\": {exc}. "
                    f"⛔ REFUSED rather than treated as absent — an unparseable expiry read as "
                    f"'no expiry' is exactly the indefinite seat this key exists to prevent."
                ) from exc
        missing = sorted({"slug", "size"} - set(raw))
        if missing:
            raise ProbeConfigError(f"{path}: {where} is missing required key(s) {missing}")
        slug = raw["slug"]
        if not isinstance(slug, str) or not slug.strip():
            raise ProbeConfigError(f"{path}: {where}.slug must be a non-empty string, got "
                                   f"{slug!r}")
        if slug in seen:
            raise ProbeConfigError(
                f"{path}: {where}.slug {slug!r} is declared twice — one entry per book. Two "
                f"entries for one slug is either a duplicate seat (double the declared size at "
                f"the venue) or two disagreeing sizes with no rule for which wins.")
        seen.add(slug)
        size = _positive_decimal(path, f"{where}.size", raw["size"])
        if size != size.to_integral_value():
            raise ProbeConfigError(
                f"{path}: {where}.size {raw['size']!r} must be a whole number of contracts — a "
                f"fractional declared size is a malformed value, not an operator choice. ⛔ The "
                f"VALUE itself is not railed here (AMENDMENT 32): a declared seat sizes off the "
                f"operator's number, and the safety rails (maker.MIN_SIZE, "
                f"--max-total-contracts, the lawful reach, the loss caps) are what bound it.")
        if size > SIZE_CEILING:
            raise ProbeConfigError(
                f"{path}: {where}.size {raw['size']!r} is above the typo ceiling "
                f"{SIZE_CEILING} — every derived rail scales with this number, so "
                f"the loader is the one place a mistyped size can refuse before the pick.")
        park_side, park_ticks = "", None
        if "park" in raw:
            park = raw["park"]
            if not isinstance(park, dict):
                raise ProbeConfigError(f"{path}: {where}.park must be a JSON object "
                                       f"{{\"side\": \"bid\"|\"ask\", \"ticks\": \"2\"}}, got "
                                       f"{park!r}")
            stray = sorted(set(park) - FIXED_SEAT_PARK_KEYS)
            if stray:
                raise ProbeConfigError(f"{path}: {where}.park carries unknown key(s) {stray} — "
                                       f"the vocabulary is {sorted(FIXED_SEAT_PARK_KEYS)} and it "
                                       f"is CLOSED.")
            absent = sorted(FIXED_SEAT_PARK_KEYS - set(park))
            if absent:
                raise ProbeConfigError(
                    f"{path}: {where}.park is missing {absent} — a park declaration is a side AND "
                    f"an offset, and half of one has no safe reading.")
            park_side = park["side"]
            if park_side not in FIXED_SEAT_PARK_SIDES:
                raise ProbeConfigError(
                    f"{path}: {where}.park.side {park_side!r} is not one of "
                    f"{sorted(FIXED_SEAT_PARK_SIDES)}. The vocabulary is CLOSED and fail-closed: "
                    f"a typo must never resolve to a side, which is the side real money rests on.")
            park_ticks = _positive_decimal(path, f"{where}.park.ticks", park["ticks"])
            if park_ticks != park_ticks.to_integral_value():
                raise ProbeConfigError(f"{path}: {where}.park.ticks must be a whole number of "
                                       f"ticks, got {park['ticks']!r}")
            if park_ticks < PARK_MIN_TICKS_OFF:
                raise ProbeConfigError(
                    f"{path}: {where}.park.ticks {park['ticks']!r} is below the maker's "
                    f"{PARK_MIN_TICKS_OFF}-tick park floor. A park that close is a touch-join "
                    f"under a shorter name, and the battery that admits a park seat skips the "
                    f"screens that would catch one. ⛔ REFUSED HERE rather than by "
                    f"the launch shim as the maker line is composed, which is "
                    f"mid-window and costs the evening.")
        out.append(FixedSeat(slug=slug, size=size, park_side=park_side,
                             park_ticks=park_ticks, until=until))
    return tuple(out)


def score_seat_cap(admission: Mapping[str, Any]) -> int:
    """How many ADDITIVE score-disagreement seats a pick may hold [AMENDMENT 42], off the RAW
    admission section.

    ⛔ READ DEFENSIVELY AND EARLY: the seat rail runs BEFORE `admission.explore` is validated, so
    anything that is not a positive int contributes 0 here and the explore validation refuses the
    file on its own terms. ⛔ AND IT IS A SEAT TERM, NOT A CLASS CAP: the seat is additive to
    `lane.max_cells`, so the rail's total has to carry it or the ceiling is a number the evening
    can exceed by design.
    """
    explore = admission.get("explore")
    if not isinstance(explore, Mapping):
        # ⛔ A MALFORMED SECTION IS NOT THIS FUNCTION'S REFUSAL: the rail runs first, and
        # `explore` (a list, a string, `null`) is refused BY NAME a few lines below with the
        # operator-facing message. Contribute 0 and let that one speak.
        return 0
    value = explore.get(EXPLORE_SCORE_DISAGREEMENT)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return 0


def _seat_rail(path: str, *, max_cells: int, n_fixed: int, arms: Sequence[float],
               ceiling: Decimal, n_score: int = 0) -> None:
    """⛔ AMENDMENT 31 — THE REQUEST-BUDGET RAIL, CHECKED AT PARSE TIME OVER EVERY DRAWABLE ARM.

    ⛔ FIXED SEATS ARE ADDITIVE TO `max_cells` WHILE THE SUM FITS THE RAIL:
    `max_cells` bounds the AUTO-PICK alone, the seat count an evening can compose is
    `max_cells + len(fixed_seats) + the score-disagreement cap` [AMENDMENT 42 added the third
    term], and the rail term is that total ÷ the cadence. Every seat requotes at the lane cadence
    (see `FixedSeat`).
    ⛔ IT IS NOT AN UNCONDITIONAL "NEVER DISPLACES". When the sum does not fit at some drawable
    arm the LOADER REFUSES here, at parse time — the operator then adds a seat by amending the
    ceiling (or dropping the arm), never by quietly lowering `max_cells` to make room, which is
    the displacement this rule exists to prevent.

    ⛔ AT PARSE TIME, over the arms the evening can actually DRAW — not over every registered arm
    (`requote_arms_registered` carries 2 s, which 12 picks already exceed today and which no
    evening draws). Checking here is what keeps a declared seat from losing an evening to a
    refusal in the middle of the window: the composed line's own rail refuses after the pick.
    """
    for arm in sorted(arms):
        # ⛔ A NAMED REFUSAL, NOT A `TypeError`. `_launch` already parses
        # and validates every drawable arm before this runs, so no shipped path reaches here with
        # junk — but this function takes a bare `Sequence[float]` and a money rail that raises an
        # unnamed exception on a bad input is a rail an operator cannot act on.
        if isinstance(arm, bool) or not isinstance(arm, (int, float, Decimal)):
            raise ProbeConfigError(f"{path}: a drawable requote arm is not a number: {arm!r}")
        if arm <= 0:
            continue
        product = Decimal(max_cells + n_fixed + n_score) / Decimal(str(arm))
        if product > ceiling:
            raise ProbeConfigError(
                f"{path}: lane.max_cells {max_cells} + {n_fixed} fixed seat(s) + {n_score} "
                f"score-disagreement seat(s) = {max_cells + n_fixed + n_score} seats at the "
                f"{arm:g}s requote arm is "
                f"{product:.2f} quote-cycles/s, above lane.max_cells_per_second {ceiling}. "
                f"Fixed seats are ADDITIVE to max_cells, so the request budget has to hold the "
                f"SUM at every arm this evening can draw. Add a seat by amending "
                f"lane.max_cells_per_second or dropping the {arm:g}s arm — NOT by lowering "
                f"lane.max_cells, which displaces the picks the seats were meant to sit beside. "
                f"Or remove a fixed seat (an EXPIRED entry still counts here: it has no evening "
                f"to key its `until` off at parse time, so delete the dead line).")


def _seconds(path: str, where: str, value: Any) -> float:
    """A positive, finite duration. Accepts int or float — seconds are not money."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProbeConfigError(f"{path}: {where} must be a number of seconds, got {value!r}")
    out = float(value)
    if not out > 0 or out != out or out in (float("inf"), float("-inf")):
        raise ProbeConfigError(f"{path}: {where} must be a positive finite number of seconds, "
                               f"got {value!r}")
    return out


def _money(path: str, where: str, value: Any, *, maximum: Decimal,
           allow_zero: bool = False) -> Decimal:
    """⛔ MONEY IS `Decimal` PARSED FROM ITS STRING FORM [CLAUDE.md § Code style]. A JSON *number* is
    refused outright rather than coerced: `json.load` has already made it a float, and
    `Decimal(0.47)` launders that error into the Decimal while looking like the right spelling."""
    if not isinstance(value, str):
        raise ProbeConfigError(
            f"{path}: {where} is money and must be a STRING (e.g. \"100\"), got {value!r}. A "
            f"JSON number reaches this loader as a float, and Decimal(float) launders the "
            f"float's error into the Decimal.")
    try:
        out = Decimal(value)
    except InvalidOperation as exc:
        raise ProbeConfigError(f"{path}: {where} is not a decimal number: {value!r}") from exc
    # `allow_zero` is the loss cap's alone: "0" is the maker's own OFF spelling (`--loss-cap 0`
    # prints DISABLED) and the retired lifetime ratchet [AMENDMENT 49]. A zero anywhere else
    # (a size, the evening cap) is a typo, not a decision.
    if out < 0 or (out == 0 and not allow_zero):
        raise ProbeConfigError(f"{path}: {where} must be "
                               f"{'non-negative' if allow_zero else 'positive'}, got {value!r}")
    if out > maximum:
        raise ProbeConfigError(
            f"{path}: {where} {value!r} is above the hard ceiling {maximum}. That ceiling is an "
            f"outer bound on a typo, not a policy — the registered lane cap is two orders under "
            f"it. Raising it is a code change AND an amendment, deliberately.")
    return out


def load(path: str = "") -> ProbeConfig:
    """Read and validate `the probe registration file`. ⛔ Raises rather than defaulting, always."""
    path = path or PROBE_CONFIG_FILE
    if not os.path.exists(path):
        raise ProbeConfigError(
            f"{path} does not exist — the probe lane REFUSES to start. This file is the lane's "
            f"registration surface (size, cap-fills ladder, cells, loss cap, arms, admission); "
            f"running without it would mean running on code defaults nobody registered. "
            f"Restore it from git.")
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ProbeConfigError(f"{path} exists but could not be read as JSON: {exc}. Fix the "
                               f"file — there is no fallback to built-in defaults.") from exc
    if not isinstance(raw, dict):
        raise ProbeConfigError(f"{path}: top level must be a JSON object, got "
                               f"{type(raw).__name__}")
    # ⛔ `launch` IS OPTIONAL AND THEREFORE NOT IN `_REQUIRED` — see `_launch`.
    unknown = sorted(set(raw) - set(_REQUIRED) - {"_doc", "launch", "clock_guard"})
    if unknown:
        raise ProbeConfigError(f"{path}: unknown top-level key(s) {unknown} — expected "
                               f"{sorted(_REQUIRED)} (plus an optional '_doc', 'launch' and "
                               f"'clock_guard').")

    provenance: dict[str, str] = {}
    launch = _launch(path, raw)
    clock_guard_classes = _clock_guard(path, raw, provenance)
    lane = _require_section(path, raw, "lane")
    supervisor = _require_section(path, raw, "supervisor")
    latch = _require_section(path, raw, "latch")
    admission = _require_section(path, raw, "admission")
    backfill = _require_section(path, raw, "backfill")
    for name, section in (("lane", lane), ("supervisor", supervisor),
                          ("latch", latch), ("admission", admission),
                          ("backfill", backfill)):
        _provenance(path, name, section, provenance)

    size_plan = _size_plan(path, lane["size_plan"])
    cap_fills = _int(path, "lane.cap_fills", lane["cap_fills"])
    cap_fills_registered = _ladder(path, "lane.cap_fills_registered",
                                   lane["cap_fills_registered"], default=cap_fills)

    arms = lane["requote_arms_registered"]
    if not isinstance(arms, list) or not arms:
        raise ProbeConfigError(f"{path}: lane.requote_arms_registered must be a non-empty list "
                              f"of arms in seconds, got {arms!r}")
    requote_arms = tuple(_seconds(path, "lane.requote_arms_registered[]", a) for a in arms)
    requote_s = _seconds(path, "lane.requote_s", lane["requote_s"])
    # ⛔ AMENDMENT 33 — REGISTRATION IS SHAPE: an unregistered default arm
    # warns and loads. `REQUOTE_S_MIN/MAX`, the ws-grace mirror and the cells/second rail are the
    # safety gates on this number and they are untouched.
    if requote_s not in requote_arms:
        _warn(f"lane.requote_s {requote_s:g} is not on lane.requote_arms_registered "
              f"{[f'{a:g}' for a in requote_arms]} ({path})")

    # ⛔ AMENDMENT 31 — the seats, then the rail over their SUM with `max_cells`. Both at parse
    # time, so a declared seat that cannot fit the request budget refuses the LAUNCH rather than
    # the composed line mid-window.
    max_cells_value = _int(path, "lane.max_cells", lane["max_cells"], maximum=MAX_CELLS_CEILING)
    fixed_seats = _fixed_seats(path, lane.get("fixed_seats", []))
    # ⛔ AMENDMENT 34 — ONE SURFACE PER SLUG, AND DUPLICATION IS THE REFUSAL
    #. A `fixed_seats` entry WINS over `size_plan.by_slug`, so an entry in both is at
    # best a number nothing reads — and at worst the seat's `until` expires and the SAME book is
    # then auto-picked at the by_slug size with no declaration behind it. Equal values do not
    # make it safe: the expiry is exactly when the two stop agreeing about whether to seat at all.
    for seat in fixed_seats:
        if seat.slug in size_plan.by_slug:
            raise ProbeConfigError(
                f"{path}: {seat.slug} is declared in lane.fixed_seats (size {int(seat.size)}"
                f"{', until ' + seat.until if seat.until else ''}) AND named in "
                f"lane.size_plan.by_slug at {size_plan.by_slug[seat.slug]!r}. The declared seat "
                f"WINS while it is live, and the by_slug entry OUTLIVES it — past `until` the "
                f"book could be auto-picked at that size with no declaration. Delete one.")
    max_cells_per_second = _positive_decimal(path, "lane.max_cells_per_second",
                                             lane["max_cells_per_second"])
    if max_cells_per_second > CELLS_PER_SECOND_CEILING:
        raise ProbeConfigError(
            f"{path}: lane.max_cells_per_second {lane['max_cells_per_second']!r} is above the "
            f"hard ceiling {CELLS_PER_SECOND_CEILING}. That ceiling is an outer bound on a typo, "
            f"not a policy — the CF wall is ~10 req/s sustained at ~2 requests per cell-cycle.")
    # ⛔ THE HARD CEILING IS ON THE TOTAL [AMENDMENT 31], not on the auto-pick's share of it: 16 is
    # a bound on how many books this lane may hold at once, and a fixed seat holds one.
    # ⛔ AND THE SCORE-DISAGREEMENT SEAT IS A THIRD TERM [AMENDMENT 42, money-path review
    # 2026-09-10]: it is ADDITIVE to `max_cells` exactly as a declared seat is, so leaving it out
    # of the total made the ceiling a number the evening exceeds by design.
    n_score = score_seat_cap(admission)
    if max_cells_value + len(fixed_seats) + n_score > MAX_CELLS_CEILING:
        raise ProbeConfigError(
            f"{path}: lane.max_cells {max_cells_value} + {len(fixed_seats)} fixed seat(s) + "
            f"{n_score} score-disagreement seat(s) = "
            f"{max_cells_value + len(fixed_seats) + n_score} seats, above the hard ceiling "
            f"{MAX_CELLS_CEILING}. The ceiling is on the TOTAL the lane may hold.")
    # ⛔ THE ARMS THE EVENING CAN DRAW — `launch.requote_arms_s` when the cadence A/B is armed,
    # otherwise the one default arm. NOT `requote_arms_registered`, which carries arms no launch
    # selects (2 s) and would refuse the shipped file on a slate it never composes.
    _seat_rail(path, max_cells=max_cells_value, n_fixed=len(fixed_seats), n_score=n_score,
               arms=([float(a) for a in launch["requote_arms_s"]]
                     if launch["requote_ab"] and launch["requote_arms_s"] else [requote_s]),
               ceiling=max_cells_per_second)

    ttl = lane["order_ttl_s"]
    order_ttl_s = None if ttl is None else _seconds(path, "lane.order_ttl_s", ttl)

    fractional_close = lane["fractional_close"]
    if fractional_close not in FRACTIONAL_CLOSE_MODES:
        raise ProbeConfigError(
            f"{path}: lane.fractional_close {fractional_close!r} is not one of "
            f"{sorted(FRACTIONAL_CLOSE_MODES)}. The vocabulary is CLOSED and fail-closed: a typo "
            f"must never resolve to {FRACTIONAL_CLOSE_OFF!r} (silently un-arming the sub-contract "
            f"close the operator ordered) nor to {FRACTIONAL_CLOSE_ON!r} (silently arming an "
            f"order intent no fill has yet proven). Spell it exactly.")

    (quotability_mode, quotability_window_s, quotability_min_share,
     quotability_min_arrivals) = _quotability(path, lane["quotability_gate"])
    burst_gate_mode = _burst_gate(path, lane)

    if not isinstance(supervisor["kalshi_sidecar"], bool):
        raise ProbeConfigError(f"{path}: supervisor.kalshi_sidecar must be true or false, got "
                               f"{supervisor['kalshi_sidecar']!r}")

    latch_rule = latch["rule"]
    # ⛔ `off` IS REFUSED HERE BY NAME. It is a real string in this module's vocabulary — the control
    # arm's TAPE TAG — so a plain "not one of" refusal would leave an operator staring at a value the
    # code demonstrably knows. This branch names the per-cell spelling that IS admissible.
    if latch_rule == LATCH_RULE_OFF:
        raise ProbeConfigError(
            f"{path}: latch.rule {LATCH_RULE_OFF!r} is REFUSED. It is the per-CELL control arm's "
            f"tape tag, never a whole-run rule: as a run rule it would disarm the adverse latch "
            f"on EVERY book, walking past the latch.control_share ceiling "
            f"({LATCH_CONTROL_SHARE_CEILING}) that is the only rail bounding how much of a night "
            f"may run unprotected. It would also make the launch banner print "
            f"'REDUCE-ONLY for the REST OF THE RUN' over a run in which nothing latches, and — "
            f"because --latch-off-slugs would be empty — file every slug of a fully-unlatched "
            f"night into the PROTECTED arm of the trial on the verdict tape. To run control "
            f"cells, set latch.control_share (0 < share <= {LATCH_CONTROL_SHARE_CEILING}) and let "
            f"the pick draw them.")
    # ⛔ A RETIRED TAG REFUSES BY NAME, AND IS NEVER MAPPED TO THE LIVE RULE. It is
    # a real string in this module's vocabulary and on the fills tape.
    if latch_rule in LATCH_RULES_RETIRED:
        raise ProbeConfigError(
            f"{path}: latch.rule {latch_rule!r} is RETIRED and is REFUSED as a run rule. It is a "
            f"TAPE tag for armings already written under it, never a rule a new run may serve: "
            f"it used to load and then run the {LATCH_RULE_LIFETIME!r} rail, so the tape said "
            f"one arm set and the run served another. The live rule is "
            f"{LATCH_RULE_RAND_60_180_600_1800!r} "
            f"(see the private design notes); the per-night harm stop flips "
            f"back to {LATCH_RULE_LIFETIME!r}. Spell one of those.")
    if latch_rule not in LATCH_RULES:
        raise ProbeConfigError(
            f"{path}: latch.rule {latch_rule!r} is not one of {sorted(LATCH_RULES)}. The "
            f"vocabulary is CLOSED and fail-closed: an unrecognised rule must never resolve to "
            f"{LATCH_RULE_LIFETIME!r} (which would silently un-arm a registered trial) nor to "
            f"{LATCH_RULE_RAND_60_180_600_1800!r} (which would re-admit latched books on a lane "
            f"whose own falsifier fired; {LATCH_RULE_RAND_900_1800!r} is RETIRED and refuses "
            f"above). Spell it exactly.")
    ban_arms = latch["ban_arms_s"]
    if not isinstance(ban_arms, list) or not ban_arms:
        raise ProbeConfigError(f"{path}: latch.ban_arms_s must be a non-empty list of arms in "
                               f"seconds, got {ban_arms!r}")
    # ⛔ THE TAG AND THE ARMS MUST AGREE — see `LATCH_BAN_ARMS_BY_RULE`. The maker draws from ONE
    # constant that `ban_arms_s` mirrors, so a tag registered with other arms would tape an arm set
    # the run never served. Fail-closed, by name, before anything is armed.
    # ⛔ FAIL-CLOSED ON A MAP MISS: a `rand*` tag in `LATCH_RULES` with no entry here is HALF
    # registered — its name promises a draw and nothing says over what — and must refuse rather than
    # inherit whatever `ban_arms_s` happens to carry.
    if latch_rule.startswith("rand") and latch_rule not in LATCH_BAN_ARMS_BY_RULE:
        raise ProbeConfigError(
            f"{path}: latch.rule {latch_rule!r} is a randomized tag with NO registered arms in "
            f"bot/core/probe_config.py LATCH_BAN_ARMS_BY_RULE. A tag that names a draw but not "
            f"its arms cannot be served: register the arms beside the tag.")
    promised = LATCH_BAN_ARMS_BY_RULE.get(latch_rule)
    if promised is not None:
        try:
            actual = tuple(float(a) for a in ban_arms)
        except (TypeError, ValueError):
            actual = ()
        if actual != promised:
            raise ProbeConfigError(
                f"{path}: latch.rule {latch_rule!r} is REGISTERED over arms "
                f"{list(promised)} s but latch.ban_arms_s carries {ban_arms!r}. The maker draws "
                f"from bot.poly_us.maker.LATCH_BAN_ARMS_S, which this key MIRRORS, so this pair "
                f"would tape one arm set while the run served another. Flip the maker's constant "
                f"and this key together, or spell the tag whose arms are already wired.")
    latch_control_share = _control_share(path, latch["control_share"])
    latch_off_classes = _str_list(path, "latch.off_classes", latch.get("off_classes", []))

    groups, refused = admission["groups"], admission["class_refused"]
    #: ⛔ ABSENT = `{}` = NO EXPLORATION [AMENDMENT 24], the pre-amendment policy and the safe
    #: reading. This is the ONE additive key; every other admission key still fails closed.
    explore = admission.get("explore", {})
    if not isinstance(groups, dict) or not all(
            isinstance(g, str) and isinstance(v, int) and not isinstance(v, bool)
            for g, v in groups.items()):
        raise ProbeConfigError(f"{path}: admission.groups must be an object of group-name → "
                               f"integer cap (0 = refused), got {groups!r}")
    if any(v < 0 for v in groups.values()):
        raise ProbeConfigError(f"{path}: a negative cap in admission.groups is not a policy — "
                               f"use 0 to refuse a group, got {groups!r}")
    if not isinstance(refused, list) or not all(isinstance(c, str) for c in refused):
        raise ProbeConfigError(f"{path}: admission.class_refused must be a list of class-name "
                               f"strings, got {refused!r}")
    # AMENDMENT 24. ⛔ POSITIVE ONLY: `0` here would be a second spelling of a refusal, and two
    # ways to refuse a class is how one of them stops being read.
    if not isinstance(explore, dict) or not all(
            isinstance(c, str) and isinstance(v, int) and not isinstance(v, bool) and v > 0
            for c, v in explore.items()):
        raise ProbeConfigError(f"{path}: admission.explore must be an object of class-name → "
                               f"POSITIVE integer seat cap (refuse a class in "
                               f"admission.class_refused, never with a 0 here), got {explore!r}")
    both = sorted(set(explore) & set(refused))
    if both:
        raise ProbeConfigError(
            f"{path}: class(es) {both} are in BOTH admission.explore and "
            f"admission.class_refused — one rule per class. An exploration cap ADMITS the class, "
            f"so leaving it on the refused list states the opposite policy in the same file.")

    # AMENDMENT 27 — the backfill section. ⛔ A LIST OF STRINGS, like `class_refused`: an unknown
    # name here excludes nothing and never refuses the evening; the launch banner prints the list.
    backfill_exclude = backfill["class_exclude"]
    if not isinstance(backfill_exclude, list) or not all(
            isinstance(c, str) for c in backfill_exclude):
        raise ProbeConfigError(f"{path}: backfill.class_exclude must be a list of class-name "
                               f"strings, got {backfill_exclude!r}")

    return ProbeConfig(
        size_plan=size_plan,
        cap_fills=cap_fills,
        cap_fills_registered=cap_fills_registered,
        max_cells=max_cells_value,
        fixed_seats=fixed_seats,
        max_cells_per_second=max_cells_per_second,
        max_seconds=_int(path, "lane.max_seconds", lane["max_seconds"],
                         maximum=MAX_SECONDS_CEILING),
        loss_cap=_money(path, "lane.loss_cap", lane["loss_cap"], maximum=LOSS_CAP_CEILING,
                        allow_zero=True),
        requote_s=requote_s,
        requote_arms_registered=requote_arms,
        order_ttl_s=order_ttl_s,
        flow_floor_gate_mult=_int(path, "lane.flow_floor_gate_mult",
                                  lane["flow_floor_gate_mult"],
                                  maximum=FLOW_FLOOR_MULT_CEILING),
        fractional_close=fractional_close,
        quotability_mode=quotability_mode,
        quotability_window_s=quotability_window_s,
        quotability_min_two_sided_share=quotability_min_share,
        quotability_min_arrivals_per_h=quotability_min_arrivals,
        burst_gate_mode=burst_gate_mode,
        cycle_seconds=_int(path, "supervisor.cycle_seconds", supervisor["cycle_seconds"]),
        # ⛔ `minimum=0` — 0 is the DISARMED reading and the only value below the floor that is
        # legal. The ceiling is the lane's own day: a period longer than the child it re-seats
        # is a knob that never fires.
        reseat_s=_int(path, "supervisor.reseat_s", supervisor["reseat_s"], minimum=0,
                      maximum=MAX_SECONDS_CEILING),
        kalshi_sidecar=supervisor["kalshi_sidecar"],
        latch_rule=latch_rule,
        latch_ban_arms_s=tuple(_seconds(path, "latch.ban_arms_s[]", a) for a in ban_arms),
        latch_control_share=latch_control_share,
        latch_off_classes=latch_off_classes,
        clock_guard_classes=clock_guard_classes,
        admission_groups=dict(groups),
        admission_class_refused=frozenset(refused),
        admission_explore=dict(explore),
        backfill_min_prints=_int(path, "backfill.min_prints", backfill["min_prints"]),
        backfill_class_exclude=frozenset(backfill_exclude),
        launch_improve_ab=launch["improve_ab"],
        launch_requote_ab=launch["requote_ab"],
        launch_requote_arms_s=tuple(launch["requote_arms_s"]),
        launch_rebate_step=launch["rebate_step"],
        launch_rebate_step_mode=launch["rebate_step_mode"],
        launch_rebate_step_max=launch["rebate_step_max"],
        launch_depth_sidecar=launch["depth_sidecar"],
        launch_gamestate_sidecar=launch["gamestate_sidecar"],
        launch_conditional_poll=launch["conditional_poll"],
        path=path,
        provenance=provenance,
    )


#: ⛔ ONE RESOLUTION PER PATH, PER PROCESS — keyed on the resolved path so a test pointing the loader
#: elsewhere gets its own entry, and so the picker, launcher and supervisor cannot hold three
#: policies read at three instants of one evening.
_CACHE: dict[str, ProbeConfig] = {}


def get_config(path: str = "") -> ProbeConfig:
    """The probe registry, resolved ONCE per process and cached. ⛔ CALL THIS, not `load`.

    ⛔ LAZY, AND PROBE-SCOPED: an import-time load in `scripts.poly_probe_night` made a
    `the probe registration file` typo refuse MAIN-LANE launches. A rail must fail only what it guards.
    """
    key = path or PROBE_CONFIG_FILE
    if key not in _CACHE:
        _CACHE[key] = load(key)
    return _CACHE[key]


def source_of(flags: Sequence[str], argv: Sequence[str], *,
              file_supplied: bool = True) -> str:
    """`file` / `cli` / `default` for one resolved value.

    ⛔ READ OFF THE ARGV, not off a value comparison: `--size 5` when the file also says 5 is a CLI
    act, and the bug this catches is a value that changed surface without changing number.
    """
    for token in argv:
        for flag in flags:
            if token == flag or token.startswith(flag + "="):
                return SRC_CLI
    return SRC_FILE if file_supplied else SRC_DEFAULT


def banner_lines(cfg: ProbeConfig, resolved: Mapping[str, Any],
                 argv: Sequence[str], flags: Mapping[str, Sequence[str]]) -> list[str]:
    """The resolved probe registry, one line per value, WITH its source and amendment tag. `resolved`
    is post-argparse; `flags` maps each dotted key to the CLI spellings that could have set it.
    """
    width = max((len(k) for k in resolved), default=0)
    out = [f"  probe registry: {cfg.path}"]
    for key, value in resolved.items():
        src = source_of(flags.get(key, ()), argv, file_supplied=key in cfg.provenance)
        shown = _clip("(none)" if value is None else str(value), 34)
        tag = cfg.provenance.get(key, "NOT REGISTERED — this value came from code, not the file")
        out.append(f"    {key:<{width}} = {shown:<34} [{src:<4}] {_clip(tag, 72)}")
    return out


def _clip(text: str, width: int) -> str:
    """One line's worth. ⛔ The FULL amendment text stays in the file — the banner is a pointer."""
    text = " ".join(text.split())
    if len(text) <= width:
        return text
    cut = text[:width].rsplit(" ", 1)[0] or text[:width]
    return cut + "…"
