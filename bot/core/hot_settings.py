"""Hot-reloadable run settings — the PURE half.

The operator edits `hot_settings_poly.json` (pause.json's posture: an operator control
file, checked at the top of every quote cycle); this module decides whether the file is
LEGAL. Whole-file-or-nothing: an unknown key, an unknown slug, a never-hot knob or a failed
interlock ignores the ENTIRE file loudly — a file that half-applies is a config nobody
chose. The engine applies only what this module returns.

The whitelist and its two interlocks:
  · per-book `size`   — but `size × (cap_fills+1)` must stay ≤ THIS BOOK's own launch
    lawful reach: the external pos-guards are per book and process-fixed, so a hot raise
    past one makes that guard pause the whole run on a LAWFUL overshoot (a false halt that
    trains the operator to ignore the real one). ⚠️ v1 consequence, deliberate: no size may
    exceed its LAUNCH value — a lowered size can be hot-RESTORED up to launch (the ceiling
    is the frozen launch reach, so lower-and-undo works without a restart), but going PAST
    launch needs the same file to lower cap_fills enough to keep reach inside it. The maker
    does not know the guards' real thresholds, so it cannot spend that headroom; v1.1
    plumbs them in. ⚠️ Word this carefully: an earlier draft said "forbids every size raise",
    which read as irreversible and invited a live-run restart just to undo a lowering.
  · per-book `cap_fills` — never above its LAUNCH value in v1, and the restore re-runs the
    reach interlock against the size that will actually run, because two individually-legal
    files must not COMPOSE past the anchor — "lowering only" was the wording that hid that.
    Raising past launch re-opens the adding side, and on a carried
    book the basis is operator-TYPED — a raise averages a typed number into real fills.
    Same removal-direction asymmetry as slate membership.
  · per-book `quote` ∈ {normal, reduce_only} — reduce_only is the REMOVE-a-book direction,
    routed through the SAME clamp path as the wind-down and --pull-at (a third reduce-only
    implementation is two more than should exist).
NEVER hot (arrive as unknown keys → whole-file ignore): loss-cap, arming, slate ADDITIONS
(an unknown slug IS an addition), max_total_contracts, requote_s (the heartbeat's
stale_after_s is launch-computed — a hot cadence change makes a healthy maker read STALE
to every watcher).
"""
from __future__ import annotations

import json
from typing import Optional

TOP_KEYS = {"updated", "note", "books"}
BOOK_KEYS = {"cap_fills", "size", "quote"}
QUOTE_MODES = {"normal", "reduce_only"}


def parse_hot_settings(raw_text: str, *, slate_sizes: dict[str, int],
                       launch_cap_fills: dict[str, int], launch_reach: dict[str, int],
                       min_size: int) -> tuple[Optional[dict], Optional[str]]:
    """(per-book overrides, None) or (None, refusal-reason). Whole-file semantics.

    The returned dict maps slug → {key: value} with ONLY the whitelisted keys, every value
    already validated against the interlocks. The caller applies it verbatim or not at all.

    ⛔ INPUT CONTRACT: `slate_sizes` is the RUNNING sizes — live, deliberately, because the
    cap_fills reach check must judge the size that will actually run. `launch_cap_fills` and
    `launch_reach` are FROZEN launch anchors. Two frozen, one live is the design, not an
    inconsistency to normalize: freezing `slate_sizes` restores the two-file compose-past-the-
    anchor exploit byte-for-byte. (The size block's cf fallback is the
    LAUNCH cap_fills — conservative, since running cf ≤ launch cf always; making it exact
    needs the running cap_fills plumbed in (v1.1) — never "fix" the asymmetry by relaxing
    the bound.)
    """
    try:
        data = json.loads(raw_text)
    except (ValueError, TypeError) as exc:
        return None, f"unparseable JSON ({exc})"
    if not isinstance(data, dict):
        return None, "top level is not an object"
    unknown_top = set(data) - TOP_KEYS
    if unknown_top:
        return None, (f"unknown top-level key(s) {sorted(unknown_top)} — never-hot knobs "
                      f"arrive exactly this way, so the whole file is ignored")
    books = data.get("books")
    if not isinstance(books, dict) or not books:
        return None, "`books` missing or empty — nothing to apply is a malformed file, not a no-op"
    out: dict[str, dict] = {}
    for slug, spec in books.items():
        if slug not in slate_sizes:
            return None, (f"{slug!r} is NOT on the slate — a slate ADDITION bypasses "
                          f"preflight, registration, freshness, the calendar gate and the "
                          f"subordinated rail; removal-direction only")
        if slug not in launch_reach or slug not in launch_cap_fills:
            # Unreachable today (the launch dicts are built over the same slugs as the
            # slate), but the dynamic-slate design adds books mid-run — a book with no
            # frozen launch anchor has nothing to judge a reach against.
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
            cf_for_reach = spec.get("cap_fills", launch_cap_fills[slug])
            if isinstance(cf_for_reach, int) and not isinstance(cf_for_reach, bool):
                # ⛔ PER-BOOK ceiling: the external guards are per book
                # (own launch reach + 25), so a slate-max ceiling let a small book borrow
                # the biggest book's headroom and trip ITS OWN guard — mia 5→14 is reach 42,
                # far under dem's 255, and mia's guard fires at 40, halting the whole run.
                if v * (cf_for_reach + 1) > launch_reach[slug]:
                    return None, (f"{slug}: size {v} × (cap_fills {cf_for_reach}+1) = "
                                  f"{v * (cf_for_reach + 1)} exceeds THIS BOOK's launch "
                                  f"lawful reach {launch_reach[slug]} — its pos-guard was "
                                  f"sized at launch, and passing it turns a lawful "
                                  f"overshoot into a whole-run false pause. ⚠️ v1 "
                                  f"CONSEQUENCE: no size past its LAUNCH value unless the "
                                  f"same file lowers cap_fills enough to pay for it (a "
                                  f"previously LOWERED size may be restored up to launch — "
                                  f"lowering is not irreversible, do NOT restart the run "
                                  f"to undo one). Deliberate — the maker does not know the "
                                  f"external guards' real thresholds (launcher-side, "
                                  f"reach+25), so it cannot spend that headroom safely; "
                                  f"v1.1 plumbs them in and re-opens raises.")
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
            # ⛔ The reach interlock runs HERE TOO: a cap_fills entry
            # multiplies whatever size is RUNNING, and without this check two
            # individually-legal files compose past the anchor — {size:127, cap_fills:1}
            # takes the paid raise (reach 254 ≤ 255), then {cap_fills:2} reads as a
            # "restore ≤ launch" while producing reach 381 against a guard sized at 280.
            # Evaluate the pair as it will actually run: the file's own (already
            # validated) size if present, else the running one.
            eff_size = entry.get("size", slate_sizes[slug])
            if eff_size * (v + 1) > launch_reach[slug]:
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
        if not entry:
            return None, f"{slug}: empty spec — a book named with no change is a typo"
        out[slug] = entry
    return out, None
