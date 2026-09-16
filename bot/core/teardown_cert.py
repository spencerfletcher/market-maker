"""The teardown CERTIFICATION record — one append-only row per real run's teardown.

Why this exists the private design notes: the
2026-08-21 teardown closed with three disagreeing verify reads, its own refusal to certify — *"a
later FLAT cannot overrule it — a stale replica can serve an old flat row"* — and
`⚠️ RESIDUAL INVENTORY, reported not force-sold` across ten books. **That verdict then had to be
remembered by a human overnight, and was effectively lost:** the next night's carry list was
hand-derived from inventory the tool had just said not to trust. The design's words for it are
that deriving carefully from distrusted inventory is *worse* than refusing, because it launders
the uncertainty into a confident-looking `--carry` string.

`poly_night` v1 shipped the READER and nothing wrote the file, so every derivation needed the
`--teardown-certified <run_id>` operator attestation. This is the writer.

⛔ **WHY HERE AND NOT IN `poly_teardown_row`.** The coordinating brief suggested that module as
the natural home, and it is the wrong one for a mechanical reason: `poly_teardown_row` is
TAPE-derived and makes **no venue read at all** (its own tool_index row ends *"End state is
TAPE-derived — venue-confirm it"*). It cannot produce this signal. The only place in the repo
that holds an authoritative post-teardown venue verdict is `poly_live_mm`'s teardown `finally`
block, via `_flatness_verdict` — which already reads the venue, already refuses to self-certify
from local belief, and already runs the three-read disagreement loop. That verdict was simply
printed and thrown away. So the writer is called from there, and the FORMAT lives here — in
`bot/core/`, mirroring `run_manifest.py` — because `poly_night` imports `poly_live_mm`
(`parse_carry`), so `poly_live_mm` cannot import `poly_night` back without a cycle. One module
both sides depend on is the only shape that gives the format one owner.

Design rules, inherited from `run_manifest` deliberately:

- **APPEND-ONLY.** No overwrite, no rotation (rows are tiny, one per teardown).
- **A write failure must NEVER stop a teardown** — it logs loudly and returns False. A teardown
  is the one moment real orders are being cancelled; evidence must not raise into it.
- **Read rows by NAME.** New keys append; old rows stay valid.

⛔ **AND THE TWO FAIL-CLOSED DIRECTIONS, WHICH ARE THE WHOLE POINT:**

1. **ABSENCE IS NOT CERTIFICATION.** A crashed teardown writes nothing, and `poly_night` falls
   through to *requiring* the operator flag. That is why a failed write is safe to swallow: the
   worst case is the v1 behaviour.
2. **CANNOT-VERIFY IS NOT FLAT.** `None` positions is the reconciler's own rule and the bug this
   repo shipped once (`reconcile.py` reported "confirmed flat" for a month while we held
   positions). An unreadable venue records **NOT certified** — not certified, and not nothing:
   "the teardown could not see the venue" is a stronger, different statement than "no teardown
   ran", and only the record can carry it.
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

DEFAULT_PATH = os.path.join("logs", "teardown_certifications.jsonl")

# The one certified-flat verdict, as a constant: the post-teardown retry loop compares against it
# rather than substring-matching a sentence that could drift. Owned here (moved from
# `scripts/poly_live_mm.py`, which re-exports it) so `is_certified` needs no script import.
VERDICT_FLAT = "  teardown/verify: venue confirms FLAT on this run's market(s)."


@dataclass(frozen=True)
class Certification:
    """One teardown's verdict. `certified` is the only field anything gates on."""
    run_id: str
    lane: str
    certified: bool
    note: str
    ts: float
    iso: str

    def as_row(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "lane": self.lane, "certified": self.certified,
                "note": self.note, "ts": self.ts, "iso": self.iso}


def decide(verdict: Optional[str],
           reads: Optional[Sequence[str]] = None) -> tuple[bool, str]:
    """`(certified, note)` from `poly_live_mm`'s post-teardown flatness verdict.

    ⛔ **EQUALITY AGAINST THE CONSTANT, NEVER A SUBSTRING MATCH.** `poly_live_mm` keeps
    `VERDICT_FLAT` as a module constant precisely so *"the post-teardown retry loop compares
    against it rather than substring-matching a sentence that could drift"* — and here the trap
    is sharper than drift: **the UNREADABLE verdict contains the word "certified"** (inside
    "flatness NOT certified"), so a `"certified" in verdict` test would certify exactly the
    cannot-verify failure mode. [Measured 2026-08-22: the VENUE-DISAGREES verdict does not
    carry the word — the trap is one of the two failure modes, and equality closes both.] The import is function-level because `bot/core` must
    not depend on `scripts/` at module scope.

    ⛔ **RETRY READS ARE EVIDENCE, NEVER AN UPGRADE** [inherited from the 2026-07-29 rule]. A
    non-empty `reads` means the first read disagreed and the loop ran; a later FLAT inside it can
    itself be the stale row — an old snapshot predating the position — so certifying on it would
    record "confirms FLAT" over real inventory nothing is watching. Any retried sequence is
    NOT certified, whatever it ended on.

    Anything unrecognised — `None`, blank, a sentence from a future refactor — is NOT certified.
    """
    if reads:
        return False, "; ".join(str(r).strip() for r in reads)
    if verdict == VERDICT_FLAT:
        return True, VERDICT_FLAT.strip()
    return False, (str(verdict).strip() if verdict else "no flatness verdict was produced")


def decide_residual(venue_pos: Optional[dict], believed: dict) -> tuple[bool, str]:
    """v2b [2026-08-23]: certified = the venue MATCHES THE INTENDED END STATE — which for a
    carry-forward run is the teardown's own declared residual, not flat. The flat-only
    `decide()` recorded `certified=false` on the FIRST real halt-teardown purely because
    inventory was deliberately carried, and the reader's (correct) no-override rule then
    blocked the next derivation — the normal operating mode could never certify.

    ⛔ Fail direction unchanged: `venue_pos=None` is CANNOT-VERIFY → False (None ≠ flat, the
    reconciler rule). Any book the venue holds that belief does not, any quantity mismatch,
    and any believed position the venue lacks → False. Quantities compared as
    `Decimal(str(...))` — string forms differ ('-10' vs '-10.0000') while the values match.
    `believed` = the run's final per-slug inventory (LOCAL BELIEF); an empty dict means
    "believed flat", which makes this a strict superset of the flat check.
    """
    if venue_pos is None:
        return False, "venue positions UNREADABLE — cannot-verify is not a match"
    from decimal import Decimal, InvalidOperation
    try:
        ven = {s2: Decimal(str(q if not isinstance(q, (tuple, list)) else q[0]))
               for s2, q in venue_pos.items()}
        bel = {s2: Decimal(str(q)) for s2, q in believed.items()}
    except (InvalidOperation, TypeError, ValueError) as exc:
        return False, f"unparseable quantity ({exc!r}) — cannot-verify is not a match"
    ven = {s2: q for s2, q in ven.items() if q != 0}
    bel = {s2: q for s2, q in bel.items() if q != 0}
    if ven == bel:
        n = len(ven)
        return True, (f"venue MATCHES the declared residual exactly ({n} book(s) carried)"
                      if n else "venue confirms FLAT (residual check)")
    extra = sorted(set(ven) - set(bel))
    missing = sorted(set(bel) - set(ven))
    diff = sorted(s2 for s2 in set(ven) & set(bel) if ven[s2] != bel[s2])
    return False, (f"venue DISAGREES with the declared residual — extra={extra} "
                   f"missing={missing} qty_mismatch={diff}")


def record(*, run_id: str, lane: str, certified: bool, note: str,
           path: Optional[str] = None) -> bool:
    """Append one certification row. Returns False (and shouts) on any I/O failure.

    ⚠️ NEVER RAISES. This is called from a teardown `finally` block, beside a live client and
    after real orders have been cancelled; an exception here would skip everything after it.
    """
    p = path or DEFAULT_PATH
    now = time.time()
    row = Certification(
        run_id=str(run_id), lane=str(lane), certified=bool(certified), note=str(note),
        ts=now, iso=datetime.fromtimestamp(now, tz=timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ")).as_row()
    try:
        parent = os.path.dirname(p)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(p, "a") as fh:
            fh.write(json.dumps(row) + "\n")
        return True
    except (OSError, ValueError) as exc:
        print(f"⚠️ teardown certification write FAILED ({exc}) — this teardown's verdict will "
              f"not survive the process, and the next launch will have to be told by hand "
              f"(--teardown-certified {run_id}) ({p})", file=sys.stderr, flush=True)
        return False


def newest_for(run_id: str, path: Optional[str] = None) -> Optional[dict]:
    """The newest recorded verdict for one run, or None.

    ⛔ A malformed line is SKIPPED, never read as a certification — an unparseable record is not
    an affirmative one. An unreadable FILE is None, which the caller must treat as "no record",
    never as a pass.

    ⚠️ LAST ROW WINS, deliberately. A later row can clear an earlier NOT-certified, because the
    reader's refusal text asks the operator to *"reconcile the venue by hand … then record the
    certification"*, and appending a durable, timestamped, auditable row IS that act. What still
    cannot clear it is the ephemeral `--teardown-certified` FLAG — that distinction (a recorded
    act vs a typed one) is the whole basis of `poly_night.certification_finding`'s branch order,
    and the earlier row remains in the file either way.
    """
    p = path or DEFAULT_PATH
    try:
        with open(p) as fh:
            raw = fh.read()
    except OSError:
        return None
    found: Optional[dict] = None
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict) and rec.get("run_id") == run_id:
            found = rec
    return found
