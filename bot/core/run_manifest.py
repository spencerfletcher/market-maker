"""One append-only row per maker run, written at start — the launch config that survives the
process.

Why this exists [plumbing audit 2026-08-18, (c)§3.1 ≡ (e)§2.1 — one fix, specified twice]:
the resolved config lived only in the process cmdline and died with it. Config was
unrecoverable for 5 of 7 backfilled runs; the runs-ledger `config` column is hand-retyped;
`poly_live_report` must be re-told the carries on a crontab line; and the 2026-08-18 risk
audit had to INFER `cap_fills` on a live real-money slate. Every one of those reads this file
instead once it exists.

Design rules:
- APPEND-ONLY CSV, one row per run start. No overwrite, no rotation (rows are tiny).
- Values that are per-book specs (sizes, cap_fills, carries) are stored as compact JSON in one
  column — never exploded into per-book columns (the tape-width churn trap).
- A manifest write failure must NEVER stop a run — it logs loudly and returns False. The
  manifest is evidence, not a gate.
- Read rows by header NAME. New columns append on the right; old rows stay valid.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import subprocess
import time
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_PATH = os.path.join("logs", "poly_live_mm_runs.csv")

#: One entry of the `carries` column's JSON list. ⛔ `source` IS PROVENANCE, AND IT DECIDES
#: WHETHER A LATER RUN MAY ANCHOR ITS BASIS REPLAY ON THIS NUMBER
#::
#:   · `"recorded"` — derived by `maker_state.recorded_carries` from THIS lane's durable record
#:     with the basis REPLAYED from its own fills under the full derive-phase guard set. A later
#:     `venue_close.replay_run_fills` anchors on it, so a residual chains across cycles.
#:   · `"declared"` — an operator `--carry` / `--adopt-existing` string. An UNVERIFIED assertion
#:     (it can be the venue UI's complement collateral), so a later replay REFUSES to anchor on
#:     it and the book stays `basis_source="reset"` for a hand price.
#: ⛔ A ROW WITHOUT THE KEY READS AS `"declared"` — every manifest row written before this build
#: was an operator declaration, and the fail-closed direction is "do not auto-book".
CARRY_ENTRY_KEYS = ("slug", "qty", "basis", "source")
CARRY_SOURCE_RECORDED = "recorded"
CARRY_SOURCE_DECLARED = "declared"

HEADER = ["run_id", "started_ts", "started_iso", "mode", "slugs", "sizes", "cap_fills",
          "max_total_contracts", "loss_cap", "requote_s", "seconds", "flatten_wait_s",
          "adverse_cooldown_s", "mark_trip_per_ct", "carries", "book_source", "order_ws",
          "conditional_poll", "kill_switch_path", "git_sha", "argv",
          # ⛔ APPENDED AT THE END, DELIBERATELY [audit C-9, 2026-09-01]. The SUPERVISOR's
          # session key — `polysess-<epoch>-<6hex>`, minted once per supervisor process and
          # carried down to every child. It exists because NO COLUMN joined a launch line to a
          # manifest row: the join had to be argv-normalisation + time adjacency + the burst
          # gate's `--run-id`, over observed offsets of 51–834 s
          # the private design notes. With this column that whole
          # document is `join(launch_attempts.jsonl, poly_live_mm_runs.csv, on="session_id")`.
          # ⛔ BLANK IS BLANK — a hand launch, or any run started outside a supervisor, records
          # nothing here. A fabricated id would join runs that shared no session.
          "session_id",
          # ⛔ APPENDED AT THE END, DELIBERATELY — `migrate_header` widens
          # every existing manifest in place, and additive-at-end is the only shape it accepts.
          # The run's resolved `lane.fractional_close` mode (`on`/`off`): whether this run's
          # maker was permitted to PLACE sub-contract reducing orders (the teardown's long-dust
          # close, and a stood-down book's dust reducer). It is here because the change is an
          # ERA, not a per-fill flag — a residue that silently stopped surviving teardown, and a
          # book that silently stopped going dark, are both invisible on the fills tape, so
          # without this column the two eras cannot be told apart afterwards.
          # ⛔ BLANK IS BLANK: every run written before this column existed records nothing here,
          # and nothing may read a blank as `off`.
          "fractional_close",
          # ⛔ APPENDED AT THE END, DELIBERATELY [AMENDMENT 19c, 2026-09-02] — `migrate_header`
          # widens every existing manifest in place, and additive-at-end is the only shape it
          # accepts. The run's LATCH-ONLY CONTROL cells as a JSON list of slugs: the books on
          # which the adverse-latch ARMING was skipped while every other rail still governed
          # (the private design notes). It is here because the arm is a
          # property of the RUN's launch, and the verdict read joins on it — the fills tape shows
          # a `latch_rule=off` row only where a trip HAPPENED, so a control cell that never
          # tripped would otherwise be indistinguishable from a `ban` cell that never tripped,
          # and the contrast's denominator would be wrong.
          # ⛔ BLANK IS BLANK: every run written before this column, and every run with no control
          # cells, records nothing here. Nothing may read a blank as "all books".
          "latch_off_slugs",
          # ⛔ APPENDED AT THE END, DELIBERATELY — `migrate_header`
          # widens every existing manifest in place, and additive-at-end is the only shape it
          # accepts. The run's QUOTE MODE: `improve` (step one tick inside the touch when there
          # is room — every run before this column) or `join` (post AT the touch on any side
          # `improve` would have stepped inside). It is a property of the RUN's launch, drawn
          # per cycle by the supervisor's `--improve-ab`, and it is the arm the A/B's matched
          # (run x book) clusters are formed on.
          # ⛔ BLANK READS AS `improve`, and this is the ONE column on this manifest where
          # that is true [operator decision 2026-09-04]: the mode is not a feature that was
          # switched on, it is a NAME for behaviour every run has always had, and `join` is the
          # only thing that has ever needed recording. `quote_mode_of` is the one reader that
          # applies the rule, so no caller re-derives it.
          "quote_mode",
          # ⛔ APPENDED AT THE END, DELIBERATELY [rebate-step optimal, 2026-09-04] —
          # `migrate_header` widens every existing manifest in place, and additive-at-end is the
          # only shape it accepts. The run's REBATE STEP MODE: `floor` (raise an adding side to
          # the smallest size whose credit clears the half-cent boundary — every run before this
          # column) or `optimal` (the size in `[base, ceiling]` maximising PAID cents per
          # contract). It is a property of the RUN's launch and it changes the SIZE that rested,
          # so a fills-tape read that pools the two modes compares different exposure per book.
          # ⛔ BLANK READS AS `floor`, for `quote_mode`'s reason exactly: `floor` is not a
          # feature that was switched on, it is the NAME for the only sizing rule that existed.
          # `rebate_step_mode_of` is the one reader that applies the rule.
          "rebate_step_mode",
          # ⛔ APPENDED AT THE END, DELIBERATELY [AMENDMENT 41, 2026-09-10] — `migrate_header`
          # widens every existing manifest in place, and additive-at-end is the only shape it
          # accepts. The run's GAME-CLOCK GUARD books as a JSON list of slugs: the derivative
          # seats that went REDUCE-ONLY in their game's final stretch
          # (bot/poly_us/maker.py:clock_guard_active). It is here for `latch_off_slugs`' reason
          # exactly — the guard is a property of the RUN's launch, and the quote tape only shows
          # `hold_cause=clock_guard` where the guard ACTUALLY fired, so a guarded book whose game
          # never reached its final stretch inside the window is otherwise indistinguishable from
          # an unguarded one, and any read's denominator would be wrong.
          # ⛔ BLANK IS BLANK: every run written before this column, and every run with no guarded
          # books, records nothing here. Nothing may read a blank as "all books".
          "clock_guard_slugs"]

#: ⛔ THE COLUMN NAME HAS ONE SPELLING, for the readers that must test the ROW for its PRESENCE
#: (`poly_probe_night.manifest_clock_guard`): absent column ≠ blank cell.
CLOCK_GUARD_COL = "clock_guard_slugs"

#: Every run written before the `quote_mode` column ran the `improve` rule — the rule was not a
#: choice then, it was the code. See the column comment.
QUOTE_MODE_DEFAULT = "improve"

#: Every run written before the `rebate_step_mode` column ran the `floor` rule — same reason.
REBATE_STEP_MODE_DEFAULT = "floor"


def schema_refusal(path: str, header: list[str]) -> str | None:
    """The operator-facing refusal if `path`'s on-disk header is not `header`, else None.

    ⛔ An append-only tape is opened "a" and its header is written only when the file is NEW, so
    after a schema change every restart appends rows of one width under a header of another.
    `csv.DictReader` files the surplus under the `None` key and every consumer reads on as if
    nothing happened — the mixed-width failure `tests/test_maker_supervisor_sever.py` pins on the
    scan file, where it let a live-run gate read mid and half-spread from the wrong columns.

    An absent or empty file is not a mismatch: the caller is about to write the header itself."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    with open(path, newline="") as fh:
        on_disk = next(csv.reader(fh), [])
    if on_disk == header:
        return None
    missing = [c for c in header if c not in on_disk]
    stem = os.path.basename(path).replace(".csv", "")
    return (f"⛔ REFUSING to append to {path}: its header is {len(on_disk)} columns, this build "
            f"writes {len(header)}" + (f" (new: {', '.join(missing)})" if missing else "") + ".\n"
            f"   Rotate it first, per the repo convention:\n"
            f"     mv {path} logs/rotated/{stem}.pre-<tag>.csv\n"
            f"   then re-run. Appending would put rows of two different widths under one header.")


def _git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, timeout=5).stdout.strip()
    except Exception:
        return ""


def migrate_header(path: str) -> bool:
    """Widen an existing manifest to the CURRENT `HEADER`, IN PLACE, preserving every row.

    ⛔ **WITHOUT THIS, APPENDING A WIDENED ROW IS SILENT DATA LOSS**. `write_run_row` appends with `csv.DictWriter(fieldnames=HEADER)` and
    writes the header ONLY when the file is empty — so on a manifest carrying the pre-`session_id`
    21-column header a 22-field row lands one field past the header's width, and
    `csv.DictReader` files it under the **`None`** key. `load_run_row(...)["session_id"]` is then
    `None`, and every downstream sink that reads it (the ledger `config` cell via
    `poly_teardown_row`, `poly_probe_night.read_run`) records nothing while the tape looks
    healthy. This is the repo's most expensive log-bug class arriving on the manifest.

    ⛔ **REWRITE, NEVER `.pre-<tag>` FREEZE-ROTATION.** `_open_writer`'s rotation is right for the
    per-run tapes and WRONG here for the reason `poly_probe_night._migrate_header` states about
    the verdicts tape: the manifest is CUMULATIVE and is the only record of what a past run was
    configured as (`basis_chain` walks it BACK across runs). Freezing it would orphan every
    historical row from `load_run_row`, which reads the live file alone.

    Four properties, each pinned:

    - **IDEMPOTENT** — an already-current header returns immediately and rewrites nothing.
    - **ADDITIVE-AT-END ONLY** — the on-disk header must be a PREFIX of `HEADER`. A reordering or
      a rename is not a widening, and quietly re-mapping one would move real config values into
      the wrong columns.
    - **REFUSES, NEVER TRUNCATES** — an unrecognised or non-prefix header returns `False` and the
      file is left byte-for-byte alone. The APPEND is then also refused (see `write_run_row`),
      because writing under a header we do not understand is the very corruption this prevents.
      Losing one run's config row is recoverable; misfiling every future row is not.
    - **ATOMIC** — written to a sibling temp file and `os.replace`d, so an interrupted migration
      leaves the original intact.

    ⚠️ **UNLOCKED READ-MODIFY-WRITE, AND THAT IS ACCEPTED [review r3 item 5].** There is no file
    lock. The only window in which it matters is the **FIRST launch after this build deploys** —
    after that the header is current and the function returns without touching the file, so the
    RMW happens once in the tape's life. If two makers happened to start inside that one window,
    both would read the pre-migration rows and the second `os.replace` would win, **losing at most
    the first one's manifest ROW** — evidence, not money, and `write_run_row` already documents
    that a lost row licenses no inference. A lock would add a failure mode (a stale lock file
    refusing a launch) strictly worse than the loss it prevents.

    ⛔ **AND IT MUST NEVER RAISE** — see this module's header: a manifest write failure logs and
    returns False, the run goes on. The exception net is deliberately wider than `OSError`
    because this function DECODES the file, which the plain append never did: an OOM-torn tail
    (six OOM kills on record for this box) yields a non-UTF-8 byte and a `UnicodeDecodeError`,
    and a truncated quoted field yields `csv.Error`. Either would otherwise propagate out of
    `write_run_row` and kill a real-money launch at the manifest write.

    Returns True when the file is safe to append to.
    """
    tmp = ""
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return True                      # a fresh file gets the current header on write
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            header = list(reader.fieldnames or [])
            if header == HEADER:
                return True                  # ⛔ idempotent: the common case, no rewrite
            if header != HEADER[:len(header)]:
                log.error(
                    f"⛔ run manifest {path}: on-disk header is NOT a prefix of this build's "
                    f"HEADER — refusing to migrate or append. On disk: {header}. Expected a "
                    f"prefix of: {HEADER}. Nothing was changed. A reorder/rename is not a "
                    f"widening, and appending under it would misfile every future row; check "
                    f"out the build that wrote this file, or archive it deliberately.")
                return False
            rows = list(reader)
        added = HEADER[len(header):]
        log.warning(f"run manifest {path}: widening the header with {added} — the "
                    f"{len(rows)} existing row(s) read BLANK there, never 0")
        tmp = f"{path}.tmp-{os.getpid()}"
        with open(tmp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in HEADER})
        os.replace(tmp, path)
        tmp = ""                             # replaced, so there is nothing left to clean up
        return True
    # ⛔ WIDER THAN `OSError` ON PURPOSE [review r3 item 1] — see the docstring. `UnicodeDecodeError`
    # is an OOM-torn tail; `csv.Error` is a truncated quoted field. Neither is an `OSError`, and
    # before the migration existed nothing here decoded the file at all, so this net is NEW
    # exposure introduced by this build and closed in the same one.
    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        log.error(f"⚠️ run manifest header migration FAILED ({type(exc).__name__}: {exc}) — "
                  f"refusing to append under an unknown header ({path}); this run's config will "
                  f"be unrecoverable, but the RUN IS UNAFFECTED")
        return False
    finally:
        # ⛔ NO `.tmp-<pid>` LEFT BESIDE THE LIVE TAPE [review r3 item 2]. A disk-full or a torn
        # read mid-write would otherwise strand a partial copy of the manifest in `logs/`, where
        # the next reader's glob or the operator's eye finds a second manifest-shaped file.
        # ⚠️ Cleared to "" immediately after a successful `os.replace`, so this can never unlink
        # the file we just installed.
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                log.warning(f"run manifest: could not remove the partial migration file {tmp} — "
                            f"remove it by hand; it is NOT a manifest and nothing should read it")


def write_run_row(path: str | None = None, **fields: Any) -> bool:
    """Append one run row. Unknown field names are refused loudly (a typo'd field silently
    dropped is config archaeology returning); missing fields write blank — blank means
    not-recorded, never zero. Returns False (and logs) on any I/O failure — the run goes on."""
    unknown = set(fields) - set(HEADER)
    if unknown:
        raise ValueError(f"unknown manifest field(s): {sorted(unknown)}")
    p = path or DEFAULT_PATH
    # ⛔ BEFORE THE FIRST WIDENED APPEND, ALWAYS [review r1 BLOCKING]. A refusal here refuses the
    # APPEND too: the alternative is writing a 22-field row under a 21-column header, where the
    # last field lands under `csv.DictReader`'s `None` key and every reader of the new column
    # silently gets nothing. Losing one row is recoverable; misfiling all of them is not.
    if not migrate_header(p):
        return False
    row = dict(fields)
    row.setdefault("started_ts", f"{time.time():.3f}")
    row.setdefault("started_iso", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    row.setdefault("git_sha", _git_sha())
    for k, v in list(row.items()):
        if isinstance(v, (dict, list)):
            row[k] = json.dumps(v, separators=(",", ":"), default=str)
    try:
        fresh = not os.path.exists(p) or os.path.getsize(p) == 0
        with open(p, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER)
            if fresh:
                w.writeheader()
            w.writerow({k: row.get(k, "") for k in HEADER})
        return True
    except OSError as exc:
        log.error(f"⚠️ run manifest write FAILED ({exc}) — the run continues, but this run's "
                  f"config will be unrecoverable after the process exits ({p})")
        return False


def quote_mode_of(row: dict | None) -> str:
    """The run's quote mode, resolving a blank/absent cell to `improve`.

    ⛔ THE ONE PLACE THE BLANK RULE LIVES. A pre-column run recorded nothing here, and its
    behaviour was `improve` by construction — but a reader that spelled that rule itself would be
    a second copy to keep in step with the column comment. `None` (no manifest row at all) is
    still `improve`: the caller that wants "not recorded" tests the row, not this."""
    return (row or {}).get("quote_mode") or QUOTE_MODE_DEFAULT


def rebate_step_mode_of(row: dict | None) -> str:
    """The run's rebate step mode, resolving a blank/absent cell to `floor` [2026-09-04].

    ⛔ THE ONE PLACE THE BLANK RULE LIVES, exactly as `quote_mode_of` owns its own. A pre-column
    run recorded nothing here and sized by the floor rule by construction; `None` (no manifest
    row at all) is still `floor` — a caller that wants "not recorded" tests the row, not this."""
    return (row or {}).get("rebate_step_mode") or REBATE_STEP_MODE_DEFAULT


def load_run_row(run_id: str, path: str | None = None) -> dict | None:
    """The manifest row for one run, by header name. None if absent or unreadable — the caller
    must treat None as not-recorded, never as a set of defaults."""
    p = path or DEFAULT_PATH
    if not os.path.exists(p):
        return None
    try:
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                if r.get("run_id") == run_id:
                    return r
    except OSError:
        return None
    return None
