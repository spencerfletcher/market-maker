"""Hot-reloadable run settings — the PURE half (M0.8, design + review converged 2026-08-05).

The operator edits `hot_settings_poly.json` (pause.json's posture: an operator control
file, checked at the top of every quote cycle); this module decides whether the file is
LEGAL. Whole-file-or-nothing: an unknown key, an unknown slug, a never-hot knob or a failed
interlock ignores the ENTIRE file loudly — a file that half-applies is a config nobody
chose. The engine applies only what this module returns.

The whitelist and its two interlocks [review verdict, verbatim reasons]:
  · per-book `size`   — but `size × (cap_fills+1)` must stay ≤ THIS BOOK's own launch
    lawful reach: the external pos-guards are per book and process-fixed, so a hot raise
    past one makes that guard pause the whole run on a LAWFUL overshoot (a false halt that
    trains the operator to ignore the real one). ⚠️ v1 consequence, deliberate: no size may
    exceed its LAUNCH value — a lowered size can be hot-RESTORED up to launch (the ceiling
    is the frozen launch reach, so lower-and-undo works without a restart), but going PAST
    launch needs the same file to lower cap_fills enough to keep reach inside it. The maker
    does not know the guards' real thresholds, so it cannot spend that headroom; v1.1
    plumbs them in. [convergence C3: an earlier version said "forbids every size raise",
    which read as irreversible and invited a live-run restart to undo a lowering.]
    ✅ **v1.1 RE-OPENS THE RAISE**, on plumbing and never on trust:
    the caller may pass `guard_thresholds` (the REAL per-book `--pause-threshold` the
    out-of-process guards were started with), `running_cap_fills`, `max_total_contracts`,
    the set of adverse-`latched` books and `loss_cap_ok`. A file that pushes a book's reach
    past its anchor is then judged against the guard that actually exists instead of the
    launch-frozen reach — and is REFUSED unless every one of these holds:
      1. the file is STAMPED with this run's `run_id`. Unstamped stays cross-run for
         LOWERINGS (the launch flow writes drains before a run id exists), but a raise is
         the adding direction: a prior run's leftover file must never grow a live seat.
      2. every raise input is present. Missing plumbing = the v1 refusal, unchanged: a
         maker that was not told the guards' thresholds may not spend their headroom.
      3. the book is not adverse-LATCHED. A latched book is reduce-only by rail; sizing it
         up is the opposite decision, and `size` is not an acknowledgement (same reasoning
         as `quote: normal`). 3b: nor otherwise REDUCE-ONLY (quote lane, `--pull-at`,
         wind-down) — such a book cannot lawfully add, so a raise buys only bigger exits
         while widening its breach threshold [mm-review C-probe2].
      4. `size × (cap_fills+1) ≤ guard_thresholds[book]` — the guard fires on `|net| >
         threshold`, so a reach at the threshold is still lawful and a reach past it is the
         same whole-run false halt v1 was written to prevent.
      5. the book's new CAP ≤ `max_total_contracts` — the plan's global rail. ⚠️ Per book,
         NOT `Σcaps ≤ max_total`: Σcaps is above max_total on every launcher-started run by
         construction (the launcher refuses a non-binding global cap), so the Σ form would
         refuse every raise forever. See `_raise_refusal`.
      6. the loss cap is NOT BREACHED (`loss_cap_ok`) — a run already past its cap does not
         get a bigger seat. ⚠️ SCOPE, and it is not "has headroom": it passes one cent inside
         the cap. It is a not-breached check, NOT a contracts→dollars conversion. No adverse cost per contract is measured in this repo, so a dollar
         ceiling on a size would be an invented number; the contract-space rails (4) and
         (5) are what actually bound the raise.
    ⚠️ The raise moves ANCHORS the engine must re-derive (caps, lawful reach, the venue
    breach threshold) and leaves the out-of-process guards holding LAUNCH numbers — see
    `PolyMaker._rederive_anchors`, which is the one place that re-derivation happens and the
    place the stale-guard line is printed. `cap_fills` raises stay refused (v1.1 is size).

    ⛔ HOW FAR A RAISE CAN GO, in one number: `guard_threshold / launch_reach`, i.e.
    `1 + GUARD_MARGIN/(size × (cap_fills+1))`. That is 3.5× at size 5/cf 1, 2.25× at 10/1 and
    only 1.18× at 44/2 — the margin is a CONSTANT, so the bigger the seat the smaller the
    multiple. The edit that motivated this feature (10 → 30 at cf 1) is STILL refused: it
    wants 3×, the guard allows 2.25×. Raises are for small seats; a large one relaunches.
  · per-book `cap_fills` — never above its LAUNCH value in v1, and the restore re-runs the
    reach interlock against the size that will actually run [convergence B2: two
    individually-legal files must not compose past the anchor — "lowering only" was the
    wording that hid it]. Raising past launch re-opens the adding side, and on a carried
    book the basis is operator-TYPED — a raise averages a typed number into real fills.
    Same removal-direction asymmetry as slate membership.
  · per-book `quote` ∈ {normal, reduce_only} — reduce_only is the REMOVE-a-book direction,
    routed through the SAME clamp path as the wind-down and --pull-at (a third reduce-only
    implementation is two more than should exist). ⚠️ `quote: normal` does NOT clear the
    adverse-round-trip latch (see below) — it is a lane setting, not an acknowledgement.
  · per-book `clear_adverse_latch` — an ISO-8601 **UTC, tz-aware** timestamp acknowledging a
    specific adverse-round-trip latch. Returned as a parsed epoch; the engine clears only when
    it is NEWER than the latch it names. ⛔ TIMESTAMPED BECAUSE THIS FILE IS LEVEL-TRIGGERED
   : the parser returns every book in the file (not a delta),
    entries are never pruned (removing one is documented as not-a-revert) and the mtime gate
    only suppresses a STEADY file — so a plain boolean/enum ack left in the file would be
    re-applied by any later unrelated edit, clearing a latch that had since RE-FIRED and
    quoting two-sided back into the flow that trapped the book twice. A timestamp makes the
    ack name the event it acknowledges: re-fire after the ack → the latch is newer → no clear;
    a deliberate re-clear means the operator writes a FRESH timestamp.
NEVER hot (arrive as unknown keys → whole-file ignore): loss-cap, arming, slate ADDITIONS
(an unknown slug IS an addition), max_total_contracts, requote_s (the heartbeat's
stale_after_s is launch-computed — a hot cadence change makes a healthy maker read STALE
to every watcher).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable, Optional

TOP_KEYS = {"updated", "note", "books", "run_id"}
BOOK_KEYS = {"cap_fills", "size", "quote", "clear_adverse_latch"}
QUOTE_MODES = {"normal", "reduce_only"}


def _parse_ack_ts(value: object) -> Optional[float]:
    """An ISO-8601 **tz-aware** stamp → epoch seconds, or None if it is not one.

    ⛔ NAIVE STAMPS ARE REFUSED, not localized. The engine compares this against a wall-clock
    latch time, and guessing a zone on an operator's typed string can only guess in the
    direction that clears a latch the operator did not mean to clear. `Z` is accepted because
    it is what the rest of this repo's operator files are stamped with.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc).timestamp()


#: The v1 refusal, kept verbatim as a constant so the engine, the launcher's pre-write
#: validation and the tests all quote ONE string. A raise reaches this when the caller could
#: not tell the parser what the external guards are actually watching for.
_V1_RAISE_REFUSAL = (
    "⚠️ v1 CONSEQUENCE: no size past its LAUNCH value unless the same file lowers cap_fills "
    "enough to pay for it (a previously LOWERED size may be restored up to launch — lowering "
    "is not irreversible, do NOT restart the run to undo one). Deliberate — the maker was "
    "not told the external guards' real thresholds (they are launcher-side, each its own "
    "margin above that book's reach), so it cannot spend that headroom safely. Launch via "
    "poly_launch, or pass --guard-threshold, to re-open raises. [⛔ the margin is NOT re-typed "
    "here: it is poly_launch.GUARD_MARGIN, and a second copy of a rail's constant is a copy "
    "that goes stale.]")


def _raise_refusal(*, slug: str, size: int, cap_fills: int, reach_new: int,
                   anchor: int, stamped_for_run: bool,
                   guard_thresholds: Optional[dict[str, int]],
                   running_cap_fills: Optional[dict[str, int]],
                   max_total_contracts: Optional[int],
                   latched: frozenset, reduce_only: frozenset,
                   loss_cap_ok: bool) -> Optional[str]:
    """None if this book may be RAISED past its anchor, else the operator-facing refusal.

    ⛔ Rails 1–7 of the v1.1 contract, in the order the module docstring numbers them:
    1 stamped · 2 plumbed · 3 not adverse-latched · 3b not otherwise reduce-only · 4 inside
    the real guard threshold · 5 inside the global rail · 6 loss cap not breached.
    Every refusal NAMES THE BINDING RAIL — an operator with a live seat and a refused file
    needs to know which number to change, not that "something" said no.

    ⛔ `anchor` is the reach the run is CURRENTLY authorised to, and it is not monotone: the
    engine releases it back toward the live reach once no inventory is using the raised band
    (`PolyMaker._rederive_anchors`). That release is what makes these rails re-run on a
    RE-raise into a band an earlier file opened — without it the band was rail-free for the
    rest of the process, and an unstamped file on a latched book with a breached loss cap
    walked straight back into it.
    """
    head = (f"{slug}: size {size} × (cap_fills {cap_fills}+1) = {reach_new} exceeds THIS "
            f"BOOK's lawful reach {anchor} — its pos-guard was sized at launch, and passing "
            f"it turns a lawful overshoot into a whole-run false pause")
    if guard_thresholds is None or slug not in guard_thresholds \
            or running_cap_fills is None or max_total_contracts is None:
        return f"{head} — {_V1_RAISE_REFUSAL}"
    if not stamped_for_run:
        return (f"{head} — a raise must be STAMPED: put this run's `run_id` in the file. "
                f"An UNSTAMPED file stays cross-run for lowerings only (the launch flow "
                f"writes drains before a run id exists); a leftover file from a prior run "
                f"must never GROW a live seat.")
    if slug in latched:
        return (f"{head} — the book is adverse-LATCHED (reduce-only by rail). Sizing it up "
                f"is the opposite decision; clear the latch with a dated "
                f"`clear_adverse_latch` first, in its own file, and decide the size after.")
    if slug in reduce_only:
        # ⛔ [mm-review C-probe2] A reduce-only book cannot lawfully ADD, so a raise there buys
        # only bigger exits — while permanently ratcheting this book's reach anchor, and with
        # it the venue-breach halt threshold, wider than anything it can legitimately hold.
        # Refused rather than "applied but not ratcheted": an un-ratcheted raise becomes a
        # breach the moment the operator flips the lane back to normal, which is a trap set by
        # the very edit that looked harmless.
        return (f"{head} — the book is REDUCE-ONLY (quote lane, --pull-at or wind-down). It "
                f"cannot lawfully add, so a bigger seat buys only bigger exits while widening "
                f"the breach threshold for a book that must not grow. Flip it to "
                f"`quote: normal` first if you mean to quote it larger.")
    if not loss_cap_ok:
        return (f"{head} — the LOSS CAP is BREACHED (session or durable lifetime loss is past "
                f"the cap): a run that is already halting does not get a bigger seat. ⚠️ This "
                f"rail is NOT-BREACHED, not 'has headroom' — it passes at one cent inside the "
                f"cap.")
    cap_new = size * cap_fills
    # ⛔ 0 DISABLES, the repo convention this module must not invent an exception to
    # (`ws_near_cap` and the loss cap both read a non-positive bound as OFF). A `<= 0` global
    # rail is an operator who turned the rail off; refusing here with "past the rail 0" was a
    # false message about a rail that is not in force. `None` still means NOT PLUMBED and is
    # refused above — off and unknown are different states.
    if max_total_contracts > 0 and cap_new > max_total_contracts:
        # ⛔ THE GLOBAL RAIL, per book — deliberately NOT "Σcaps ≤ max_total" [ build
        # 2026-08-20]. Σcaps is ALREADY above max_total on every launcher-started run by
        # construction (`Plan.max_total = Σcaps − MAX_TOTAL_MARGIN`, and the launcher REFUSES
        # a non-binding global cap), so a Σcaps form would refuse every raise ever and this
        # feature would be dead on arrival. What the rail can honestly say is that ONE book
        # may not be sized past the whole run's exposure budget: gross exposure is bounded by
        # `max_total_contracts` live, so a per-book cap above it is a number that can never
        # be reached — an operator steering by a fiction. The account bound itself is not at
        # risk here; the fiction is.
        return (f"{head} — and its new cap {cap_new} is past the run's GLOBAL rail "
                f"max_total_contracts {max_total_contracts}, which is never hot. Gross "
                f"exposure halts at {max_total_contracts} whatever this book's cap says, so "
                f"the raise buys a cap that cannot be reached. Relaunch to move the rail.")
    guard = guard_thresholds[slug]
    if reach_new > guard:
        return (f"{head} — and past the EXTERNAL guard's real threshold {guard} (its "
                f"external position guard fires on |net| > {guard}), so the raise turns a lawful "
                f"overshoot into a whole-run false halt. The most this book can reach is "
                f"{guard}; to go further, RELAUNCH the run at a larger seat. ⛔ Restarting "
                f"the guard at a higher threshold does NOT help and makes things worse: this "
                f"maker read its thresholds once, at start, and no hot key can update them — "
                f"you would loosen the only external watcher and get this identical refusal.")
    return None


def parse_hot_settings(raw_text: str, *, slate_sizes: dict[str, int],
                       launch_cap_fills: dict[str, int], launch_reach: dict[str, int],
                       min_size: int,
                       run_id: Optional[str] = None,
                       running_cap_fills: Optional[dict[str, int]] = None,
                       guard_thresholds: Optional[dict[str, int]] = None,
                       max_total_contracts: Optional[int] = None,
                       latched: Iterable[str] = (),
                       reduce_only: Iterable[str] = (),
                       loss_cap_ok: bool = False,
                       retired: Iterable[str] = ()) -> tuple[Optional[dict], Optional[str]]:
    """(per-book overrides, None) or (None, refusal-reason). Whole-file semantics.

    The returned dict maps slug → {key: value} with ONLY the whitelisted keys, every value
    already validated against the interlocks. The caller applies it verbatim or not at all.

    ⛔ INPUT CONTRACT [convergence round 4]: `slate_sizes` is the RUNNING sizes — live,
    deliberately, because the cap_fills reach check must judge the size that will actually
    run (B2). `launch_cap_fills` and `launch_reach` are FROZEN launch anchors. Two frozen,
    one live is the design, not an inconsistency to normalize: freezing `slate_sizes`
    restores the B2 two-file exploit byte-for-byte. (The size block's cf fallback was the
    LAUNCH cap_fills — conservative, since running cf ≤ launch cf always; ✅ v1.1 plumbs
    `running_cap_fills` and the fallback is now EXACT when it is supplied. It is exact, not
    relaxed: the pair is still judged as it will actually RUN, and a later cap_fills restore
    is still caught by the cap_fills block's own interlock against the running size.)

    ⛔ `launch_reach` is the CURRENT anchor, not a launch constant, once raises are in play:
    the engine lifts a book's entry when a rail-checked raise applies (`_rederive_anchors`)
    and never lowers it. Passing a genuinely frozen dict keeps v1 behaviour exactly.

    The v1.1 raise inputs are all optional and DEFAULT TO REFUSING a raise — a caller that
    knows none of them (the launcher's pre-write validation, every v1 call site) gets v1.
    `latched` is the set of adverse-latched books; `loss_cap_ok` is the engine's own
    "the cap is not breached" answer, never a number this module re-derives.
    """
    latched = frozenset(latched)
    reduce_only = frozenset(reduce_only)
    retired = frozenset(retired)
    try:
        data = json.loads(raw_text)
    except (ValueError, TypeError) as exc:
        return None, f"unparseable JSON ({exc})"
    if not isinstance(data, dict):
        return None, "top level is not an object"
    # [B8, plumbing audit 2026-08-18] Optional run scoping: a file stamped with a `run_id`
    # applies ONLY to that run — a stale file from a prior run once set two books reduce-only
    # on a fresh prime-time run and the log looked healthy throughout (M0.8; it failed safe
    # by off-slate accident, not by design). An UNSTAMPED file stays cross-run on purpose:
    # the launch flow writes drain overrides BEFORE the run id exists. Stamp whenever the
    # edit targets a specific live run.
    stamp = data.get("run_id")
    if stamp is not None and run_id is not None and str(stamp) != str(run_id):
        return None, (f"stamped for run {stamp!r}, this is {run_id!r} — a prior run's file "
                      f"must not steer this one (B8)")
    unknown_top = set(data) - TOP_KEYS
    if unknown_top:
        return None, (f"unknown top-level key(s) {sorted(unknown_top)} — never-hot knobs "
                      f"arrive exactly this way, so the whole file is ignored")
    books = data.get("books")
    if not isinstance(books, dict) or not books:
        return None, "`books` missing or empty — nothing to apply is a malformed file, not a no-op"
    out: dict[str, dict] = {}
    #: slug → its post-change lawful reach, for books this file RAISES past their anchor.
    #: Read by the cap_fills block, which must let through the ONE pair the size block already
    #: cleared against the real guard and refuse every other way past the anchor.
    raised: dict[str, int] = {}
    for slug, spec in books.items():
        if slug in retired and slug not in slate_sizes:
            # ⛔ SKIPPED, NOT REFUSED [continuous maker P1]. A hot-slate DROP takes a book off the
            # running slate, and this file is LEVEL-triggered: the operator's entry for that book
            # is still in it, and reading it as a slate ADDITION would void the whole file —
            # taking every OTHER book's drain, size override and latch ack with it, on the one
            # axis the operator cannot see. A dropped book has nothing left to apply to.
            continue
        if slug not in slate_sizes:
            return None, (f"{slug!r} is NOT on the slate — a slate ADDITION bypasses "
                          f"preflight, registration, freshness, the calendar gate and the "
                          f"subordinated rail; removal-direction only")
        if slug not in launch_reach or slug not in launch_cap_fills:
            # Unreachable today (the launch dicts are built over the same slugs as the
            # slate), but the dynamic-slate design adds books mid-run — a book with no
            # frozen launch anchor has nothing to judge a reach against [convergence N5].
            return None, (f"{slug}: on the live slate but missing a launch anchor — "
                          f"refusing rather than guessing a reach ceiling")
        if not isinstance(spec, dict):
            return None, f"{slug}: spec is not an object"
        unknown = set(spec) - BOOK_KEYS
        if unknown:
            return None, f"{slug}: unknown key(s) {sorted(unknown)}"
        entry: dict = {}
        if "size" in spec:
            v = spec["size"]
            if not isinstance(v, int) or isinstance(v, bool) or v < min_size:
                return None, f"{slug}: size {v!r} is not an int ≥ the venue minimum {min_size}"
            # ✅ v1.1: the RUNNING cap_fills when the caller supplies them (exact), else the
            # launch value (conservative — running cf ≤ launch cf always).
            cf_running = (running_cap_fills or launch_cap_fills).get(slug,
                                                                     launch_cap_fills[slug])
            cf_for_reach = spec.get("cap_fills", cf_running)
            if isinstance(cf_for_reach, int) and not isinstance(cf_for_reach, bool):
                # ⛔ PER-BOOK ceiling [verification (a)]: the external guards are per book
                # (own launch reach + 25), so a slate-max ceiling let a small book borrow
                # the biggest book's headroom and trip ITS OWN guard — mia 5→14 is reach 42,
                # far under dem's 255, and mia's guard fires at 40, halting the whole run.
                reach_new = v * (cf_for_reach + 1)
                if reach_new > launch_reach[slug]:
                    # ✅ v1.1: a raise is no longer unconditionally refused — it is
                    # judged against the guard that ACTUALLY exists. Every input defaults to
                    # absent, and absent means the v1 refusal (`_raise_refusal` rail 2).
                    refusal = _raise_refusal(
                        slug=slug, size=v, cap_fills=cf_for_reach, reach_new=reach_new,
                        anchor=launch_reach[slug],
                        stamped_for_run=(stamp is not None and run_id is not None
                                         and str(stamp) == str(run_id)),
                        guard_thresholds=guard_thresholds,
                        running_cap_fills=running_cap_fills,
                        max_total_contracts=max_total_contracts,
                        latched=latched, reduce_only=reduce_only,
                        loss_cap_ok=loss_cap_ok)
                    if refusal is not None:
                        return None, refusal
                    raised[slug] = reach_new
            entry["size"] = v
        if "cap_fills" in spec:
            v = spec["cap_fills"]
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                return None, f"{slug}: cap_fills {v!r} is not an int ≥ 1"
            if v > launch_cap_fills[slug]:
                return None, (f"{slug}: cap_fills {v} RAISES the launch value "
                              f"{launch_cap_fills[slug]} — lowering only in v1: raising "
                              f"re-opens the adding side, and on a carried book that "
                              f"averages an operator-TYPED basis into real fills")
            # ⛔ The reach interlock runs HERE TOO [convergence B2]: a cap_fills entry
            # multiplies whatever size is RUNNING, and without this check two
            # individually-legal files compose past the anchor — {size:127, cap_fills:1}
            # takes the paid raise (reach 254 ≤ 255), then {cap_fills:2} reads as a
            # "restore ≤ launch" while producing reach 381 against a guard sized at 280.
            # Evaluate the pair as it will actually run: the file's own (already
            # validated) size if present, else the running one.
            eff_size = entry.get("size", slate_sizes[slug])
            # ✅ v1.1: the ONE pair this may lawfully exceed the anchor by is the raise the
            # size block ALREADY cleared against the real guard, in this same file and for
            # this same book — `raised[slug]` is that exact reach. Anything else (a cap_fills
            # entry alone, or a different pair) is still the B2 re-spend and still refuses.
            if eff_size * (v + 1) > launch_reach[slug] \
                    and raised.get(slug) != eff_size * (v + 1):
                return None, (f"{slug}: size {eff_size} × (cap_fills {v}+1) = "
                              f"{eff_size * (v + 1)} exceeds THIS BOOK's launch lawful "
                              f"reach {launch_reach[slug]} — restoring cap_fills after a "
                              f"paid size raise re-spends headroom the raise already "
                              f"spent; lower size in the same file to pay for it")
            entry["cap_fills"] = v
        if "quote" in spec:
            v = spec["quote"]
            if v not in QUOTE_MODES:
                return None, f"{slug}: quote {v!r} not in {sorted(QUOTE_MODES)}"
            entry["quote"] = v
        if "clear_adverse_latch" in spec:
            ack = _parse_ack_ts(spec["clear_adverse_latch"])
            if ack is None:
                return None, (f"{slug}: clear_adverse_latch "
                              f"{spec['clear_adverse_latch']!r} is not a tz-aware ISO-8601 UTC "
                              f"timestamp (e.g. \"2026-08-19T04:12:00Z\") — a naive or "
                              f"unparseable stamp cannot be compared against the latch it "
                              f"claims to acknowledge")
            entry["clear_adverse_latch"] = ack
        if not entry:
            return None, f"{slug}: empty spec — a book named with no change is a typo"
        out[slug] = entry
    return out, None
