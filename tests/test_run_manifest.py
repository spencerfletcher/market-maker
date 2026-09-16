"""Run manifest (bot/core/run_manifest.py) — the launch config that survives the process.

Plumbing audit 2026-08-18: config was unrecoverable for 5 of 7 backfilled runs; the risk audit
had to INFER cap_fills on a live real-money slate. The manifest is evidence, not a gate — a
write failure logs loudly and never stops a run.
"""
from __future__ import annotations

import csv
import json

from bot.core import run_manifest


def test_write_and_load_round_trip(tmp_path):
    p = str(tmp_path / "runs.csv")
    ok = run_manifest.write_run_row(
        p, run_id="polymm-real-x", mode="real",
        slugs=["a", "b"], sizes={"a": 20, "b": 10}, cap_fills={"a": 1, "b": 1},
        max_total_contracts=110, loss_cap="40", requote_s=10.0, seconds=24000,
        carries=[{"slug": "a", "qty": "7", "basis": "0.3330"}],
        book_source="rest", order_ws=True, conditional_poll=False,
        kill_switch_path="pause.json", argv=["poly_live_mm", "--slugs", "a,b"])
    assert ok is True
    row = run_manifest.load_run_row("polymm-real-x", p)
    assert row is not None
    assert json.loads(row["sizes"]) == {"a": 20, "b": 10}
    assert json.loads(row["carries"])[0]["basis"] == "0.3330"
    assert row["max_total_contracts"] == "110"
    assert row["loss_cap"] == "40"
    assert row["started_ts"] != "" and row["started_iso"] != ""


def test_append_only_two_runs_two_rows(tmp_path):
    p = str(tmp_path / "runs.csv")
    run_manifest.write_run_row(p, run_id="r1", mode="dry")
    run_manifest.write_run_row(p, run_id="r2", mode="real")
    with open(p, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["run_id"] for r in rows] == ["r1", "r2"]


def test_unknown_field_is_refused_loudly(tmp_path):
    """A typo'd field silently dropped is config archaeology returning — refuse instead."""
    import pytest
    with pytest.raises(ValueError, match="cap_fils"):
        run_manifest.write_run_row(str(tmp_path / "runs.csv"), run_id="r", cap_fils=1)


def test_write_failure_returns_False_and_never_raises(tmp_path):
    """The manifest is evidence, not a gate: an unwritable path must not stop a launch."""
    bad = str(tmp_path / "no_such_dir" / "runs.csv")
    assert run_manifest.write_run_row(bad, run_id="r") is False


def test_missing_fields_write_blank_never_zero(tmp_path):
    p = str(tmp_path / "runs.csv")
    run_manifest.write_run_row(p, run_id="r1", mode="shadow")
    row = run_manifest.load_run_row("r1", p)
    assert row["max_total_contracts"] == ""      # not-recorded is blank, never 0
    assert row["loss_cap"] == ""


def test_load_absent_file_or_run_is_None(tmp_path):
    p = str(tmp_path / "runs.csv")
    assert run_manifest.load_run_row("r1", p) is None
    run_manifest.write_run_row(p, run_id="other")
    assert run_manifest.load_run_row("r1", p) is None


# ── `fractional_close` — the sub-contract-close ERA column ────────────────

def test_fractional_close_is_APPENDED_AT_THE_END_and_round_trips(tmp_path):
    """⛔ ADDITIVE-AT-END, because `migrate_header` accepts nothing else: the on-disk header must
    be a PREFIX of `HEADER`. A column inserted mid-header would misfile every name-mapped read of
    every row written under the old one."""
    # ⚠️ RE-AIMED 2026-09-02 when `latch_off_slugs` was appended after it [AMENDMENT 19c]. The
    # claim is unchanged in KIND — this column stays where it landed and nothing is reordered
    # ahead of it; only the tail of the header moved, which is the one edit `migrate_header`
    # accepts.
    # ⚠️ RE-AIMED AGAIN 2026-09-04 when `quote_mode` was appended — same kind
    # of edit, same rule: the tail grows, nothing ahead of it moves.
    # ⚠️ RE-AIMED AGAIN 2026-09-10 when `clock_guard_slugs` was appended [AMENDMENT 41] — and
    # aimed at the ORDER this time, not at an index, so the next tail column re-aims nothing.
    assert (run_manifest.HEADER.index("session_id")
            < run_manifest.HEADER.index("fractional_close")
            < run_manifest.HEADER.index("latch_off_slugs")), "nothing was reordered ahead of it"
    p = str(tmp_path / "runs.csv")
    assert run_manifest.write_run_row(p, run_id="r1", mode="real", fractional_close="on")
    assert (run_manifest.load_run_row("r1", p) or {})["fractional_close"] == "on"


def test_a_PRE_DUST_manifest_MIGRATES_and_keeps_every_old_value(tmp_path):
    """The blocking case the column's own predecessor documented: without the in-place migration
    the widened field lands under `csv.DictReader`'s `None` key and every reader of the new column
    silently gets nothing. ⛔ BLANK IS BLANK — a pre-2026-09-02 run records nothing here, and no
    reader may take that for `off`."""
    p = str(tmp_path / "runs.csv")
    old = list(run_manifest.HEADER[:-2])                 # the header as it shipped pre-
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=old)
        w.writeheader()
        w.writerow({c: "" for c in old} | {"run_id": "old-run", "mode": "real",
                                           "loss_cap": "100", "session_id": "polysess-1-aa"})
    assert run_manifest.write_run_row(p, run_id="new-run", mode="real", fractional_close="off")
    before = run_manifest.load_run_row("old-run", p) or {}
    assert before["loss_cap"] == "100" and before["session_id"] == "polysess-1-aa", before
    assert before["fractional_close"] == "", "a pre-column run has none — blank, never invented"
    assert (run_manifest.load_run_row("new-run", p) or {})["fractional_close"] == "off"


# ── `latch_off_slugs` — the LATCH-ONLY CONTROL ARM's cells [AMENDMENT 19c, 2026-09-02] ─────────

def test_latch_off_slugs_is_APPENDED_AT_THE_END_and_round_trips_as_JSON(tmp_path):
    """⛔ ADDITIVE-AT-END, for `fractional_close`'s reason exactly: `migrate_header` requires the
    on-disk header to be a PREFIX of `HEADER`, and a mid-header insert would misfile every
    name-mapped read of every row written under the old one.

    ⛔ AND IT IS A LIST IN ONE COLUMN, never exploded per book — this module's own design rule
    (the tape-width churn trap), and the same shape `sizes`/`carries` already use."""
    # Columns have been appended after it; the pin is the ORDER, not a fixed index.
    assert (run_manifest.HEADER.index("fractional_close")
            < run_manifest.HEADER.index("latch_off_slugs")
            < run_manifest.HEADER.index("quote_mode"))
    p = str(tmp_path / "runs.csv")
    assert run_manifest.write_run_row(p, run_id="r1", mode="real",
                                      latch_off_slugs=["aec-mlb-a", "aec-mlb-b"])
    assert (run_manifest.load_run_row("r1", p) or {})["latch_off_slugs"] == \
        '["aec-mlb-a","aec-mlb-b"]'


def test_a_PRE_CONTROL_ARM_manifest_MIGRATES_and_the_new_column_is_BLANK(tmp_path):
    """⛔ BLANK IS BLANK, AND HERE IT IS LOAD-BEARING FOR THE TRIAL. A pre-2026-09-02 run records
    nothing, and `poly_probe_night.manifest_latch_off` reads that as UNKNOWN — never as `[]`,
    which would assert every book of that run was a `ban` cell and pool the whole pre-trial
    history into the control arm's comparison group."""
    p = str(tmp_path / "runs.csv")
    old = list(run_manifest.HEADER[:-1])
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=old)
        w.writeheader()
        w.writerow({c: "" for c in old} | {"run_id": "old-run", "mode": "real",
                                           "loss_cap": "100"})
    assert run_manifest.write_run_row(p, run_id="new-run", mode="real", latch_off_slugs=[])
    before = run_manifest.load_run_row("old-run", p) or {}
    assert before["loss_cap"] == "100" and before["latch_off_slugs"] == ""
    # …while a run that RECORDED an empty list says something different and knowable.
    assert (run_manifest.load_run_row("new-run", p) or {})["latch_off_slugs"] == "[]"


# ── `quote_mode` — the improve-vs-join arm ──────────────────────────

def test_quote_mode_is_APPENDED_AT_THE_END_and_a_BLANK_reads_as_improve(tmp_path):
    """⛔ ADDITIVE-AT-END, for `latch_off_slugs`' reason exactly. ⛔ AND THE BLANK RULE HAS ONE
    OWNER, `quote_mode_of`: every run written before this column ran the `improve` rule, because
    the rule was not a choice then — it was the code. MUTANT: `return row.get("quote_mode")` →
    RED on the pre-column row."""
    # Appended after `latch_off_slugs`; later columns (`rebate_step_mode`, `clock_guard_slugs`)
    # were appended after IT, so the pin is the ORDER, not a fixed index.
    assert (run_manifest.HEADER.index("latch_off_slugs")
            < run_manifest.HEADER.index("quote_mode")
            < run_manifest.HEADER.index("rebate_step_mode"))
    p = str(tmp_path / "runs.csv")
    assert run_manifest.write_run_row(p, run_id="r1", mode="real", quote_mode="join")
    assert (run_manifest.load_run_row("r1", p) or {})["quote_mode"] == "join"
    assert run_manifest.quote_mode_of(run_manifest.load_run_row("r1", p)) == "join"
    # A pre-column run: blank on disk, `improve` to a reader.
    assert run_manifest.write_run_row(p, run_id="r2", mode="real")
    assert (run_manifest.load_run_row("r2", p) or {})["quote_mode"] == ""
    assert run_manifest.quote_mode_of(run_manifest.load_run_row("r2", p)) == "improve"
    assert run_manifest.quote_mode_of(None) == "improve"


# ── `rebate_step_mode` — floor-vs-optimal sizing [2026-09-04] ──────────────────────────────────

def test_rebate_step_mode_is_APPENDED_AT_THE_END_and_a_BLANK_reads_as_floor(tmp_path):
    """⛔ ADDITIVE-AT-END, for `quote_mode`'s reason exactly: `migrate_header` requires the
    on-disk header to be a PREFIX of `HEADER`, and a pre-column manifest must WIDEN rather than
    misfile the new field under `DictReader`'s `None` key.

    ⛔ AND THE BLANK RULE HAS ONE OWNER, `rebate_step_mode_of`: every run written before this
    column sized adding sides by the floor rule, because that rule was not a choice then — it
    was the code. MUTANT: `return row.get("rebate_step_mode")` → RED on the pre-column row."""
    p = str(tmp_path / "runs.csv")
    # A PRE-COLUMN manifest on disk: the widening must preserve its row and leave the cell blank.
    old = list(run_manifest.HEADER[:run_manifest.HEADER.index("rebate_step_mode")])
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=old)
        w.writeheader()
        w.writerow({c: "" for c in old} | {"run_id": "old-run", "mode": "real",
                                           "loss_cap": "100"})
    assert run_manifest.write_run_row(p, run_id="r1", mode="real", rebate_step_mode="optimal")
    before = run_manifest.load_run_row("old-run", p) or {}
    assert before["loss_cap"] == "100" and before["rebate_step_mode"] == ""
    assert run_manifest.rebate_step_mode_of(before) == "floor"
    assert (run_manifest.load_run_row("r1", p) or {})["rebate_step_mode"] == "optimal"
    assert run_manifest.rebate_step_mode_of(run_manifest.load_run_row("r1", p)) == "optimal"
    assert run_manifest.rebate_step_mode_of(None) == "floor"


# ── `clock_guard_slugs` — the GAME-CLOCK GUARD's books [AMENDMENT 41, 2026-09-10] ──────────────

def test_clock_guard_slugs_is_APPENDED_AT_THE_END_and_BLANK_IS_BLANK(tmp_path):
    """⛔ ADDITIVE-AT-END, for `latch_off_slugs`' reason exactly: `migrate_header` widens every
    manifest in place and accepts no other shape. ⛔ BLANK IS BLANK — no reader may read an
    empty cell as "every book was guarded", because the quote tape only carries
    `hold_cause=clock_guard` where the guard actually FIRED."""
    assert run_manifest.HEADER[-1] == "clock_guard_slugs"
    p = str(tmp_path / "runs.csv")
    old = list(run_manifest.HEADER[:-1])
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=old)
        w.writeheader()
        w.writerow({c: "" for c in old} | {"run_id": "old-run", "mode": "real",
                                           "loss_cap": "100"})
    assert run_manifest.write_run_row(p, run_id="r1", mode="real",
                                      clock_guard_slugs=["tsc-nfl-a", "atc-mlb-b"])
    before = run_manifest.load_run_row("old-run", p) or {}
    assert before["loss_cap"] == "100" and before["clock_guard_slugs"] == ""
    assert (run_manifest.load_run_row("r1", p) or {})["clock_guard_slugs"] == \
        '["tsc-nfl-a","atc-mlb-b"]'
