"""
scripts/maker_supervisor.py  (Option B — scanner-driven start/stop)
──────────────────────────────────────────────────────────────────
Automates the "watch for a live wide maker-free market → fire a bounded attended maker run → repeat" loop,
so we CAPITALIZE when a market appears instead of pre-picking one that resolves out from under us. It
reads a discovery-scan CSV (produced by a market-scanning tool that is NOT part of this repository —
see `--scan-csv` and `_EXPECTED_SCAN_HDR` for the shape it expects) and, when a SUSTAINED
BALANCED-wide window exists (mid-band + streak gate — NOT any wide book, which mid-game is usually a
lopsided converging whipsaw = max adverse selection), launches ONE bounded `scripts/kalshi_live_mm` run —
with the reviewed go/no-go config and a wider exit skew (`--inv-coef 0.02`). NO maker code change: the
maker still does its own one-shot selection per run; the supervisor just decides WHEN to run and STEERS
the series. The fire gate (`--min-mid`/`--max-mid`/`--min-halfspread`/`--min-wide-streak`) BIASES a real
run away from the whipsaw regime — the SAME book must read balanced-wide for N consecutive ~60s samples —
but a 60s sampler cannot resolve a faster swing (Nyquist), so the maker's loss cap remains the real
backstop. `_row_is_balanced_wide` is the gate's per-row source of truth.

⚠️ SAFETY — this orchestrates REAL-MONEY runs, so:
- **DRY by default.** Without `--live` it only LOGS "would start" — it fires nothing. `--live` is required to
  place real orders (it passes `--i-understand-real-money` to the maker), and even then each start is gated on
  a fresh **verify-flat** (inherited inventory is invisible to the maker's cap) and bounded by `--run-seconds`
  + the maker's own loss cap + flatten-on-exit. A `--max-runs` cap bounds how many real runs fire.
- **Attended.** Automating start weakens "attended"; run `--live` only while an operator is watching. Each run
  is a clean one-shot the maker can end (bounded seconds → teardown + flatten). Unattended auto-start is a
  separate, bigger decision — not this tool.

⚠️ STEERING — the subtlety a review of the scanner turned up: a 🟢 means a live wide market EXISTS, not that the maker's own
volume-ranked selection would pick it (it ranks by VOLUME, not width, and takes only `--markets`). So the
supervisor points the maker at the EXACT surfaced ticker via `--ticker`. The maker still validates that
ticker (eligibility / maker-free / contested / two-sided book) and DROPS it if bad, so a stale surfaced
ticker can never force a bad quote. It never uses `--pregame-only` (that would exclude the live lane a 🟢
flags).

⚠️ STALE-SCAN FAIL-SAFE: acts only on a scan row newer than `--scan-max-age-s`; a dead/stale scanner → no run.

⛔ **THIS TOOL IS CURRENTLY INERT AND WILL FIRE NOTHING, `--live` OR NOT** — `_SUPERVISOR_ALLOWED_SERIES`
is deliberately EMPTY (see the block above it for the reasoning). Every candidate is
refused at selection, so `--live` polls, logs a skip line, and never launches. That is the intended
state: nothing has been vetted for an UNATTENDED real-money run. An ATTENDED run does not use this
tool at all — it invokes `scripts.kalshi_live_mm` directly.

Run (DRY, detached):  setsid nohup .venv/bin/python -m scripts.maker_supervisor > logs/maker_supervisor.log 2>&1 &
Run (LIVE, attended): .venv/bin/python -m scripts.maker_supervisor --live --max-runs 3   # fires nothing today

⛔ **THIS TOOL ONLY WORKS AGAINST A NARROW SCAN, AND THE PROJECT IS MOVING AWAY FROM ONE.**
`_read_scan` surfaces exactly ONE candidate — the single global-widest `best_ticker` of the latest
row — and requires it to stay the SAME ticker for `--min-wide-streak` consecutive samples. Against
a widened discovery universe (hundreds of maker-free series competing for
that one slot) the target churns every sample, the streak never reaches 2, and the supervisor logs
`streak 0/2 … waiting` forever without firing. That shape is already visible on a TWO-series scan —
a whole overnight of consecutive `streak 0/2` lines and not one launch.

Every failure mode here is refuse-to-fire, so this is a usability limit, not a safety one — but do
NOT arm it against a widened scan expecting it to work. It requires a scan whose series set is a
SUBSET of `_SUPERVISOR_ALLOWED_SERIES` — ⚠️ **which is now the EMPTY set, so that condition is
unsatisfiable by construction and no scan can drive this tool at all.** Fixing it properly means
per-series candidates in the scan, not one global-widest column — and re-authorising at least one
series, deliberately.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import signal
import subprocess
import sys
import time
from decimal import Decimal, InvalidOperation

from bot.kalshi.client import KalshiClient
from bot.core.qty_parse import _first_qty   # canonical fail-closed position-qty parser (None = cannot-verify)

# The reviewed go/no-go config, plus the wider exit skew. markets=1 keeps both the venue rate budget and
# the loss cap comfortably live. --confirm is required for the maker to run at all;
# --i-understand-real-money is appended ONLY under --live.
_MAKER_CFG = ["--maker-free-only", "--markets", "1", "--size", "1", "--inv-cap", "3", "--inv-coef", "0.02",
              "--obi-coef", "0", "--improve-ticks", "0", "--loss-cap", "3", "--requote-s", "10",
              "--min-price", "0.15", "--max-price", "0.85", "--flatten-on-exit", "--flatten-wait-s", "60",
              "--confirm"]


def _row_is_balanced_wide(r: list[str], min_mid: Decimal, max_mid: Decimal, min_hs: Decimal) -> bool:
    """A census row whose surfaced WIDEST live market is itself BALANCED and wide enough to fire on.

    The scanner surfaces only ONE best_ticker (widest-first), so this fires only when the widest live book
    is ALSO balanced — conservative (a balanced book that is merely 2nd-widest is not surfaced, so it is
    skipped) but it can never fire on a lopsided converging longshot, which is the point. mid/half-spread
    are parsed as Decimal from the CSV's string form — these are threshold comparisons, so never float.
    """
    try:
        if int(r[5]) <= 0 or not r[7]:              # n_live_wide, best_ticker
            return False
        mid = Decimal(r[9]); hs = Decimal(r[10])    # best_mid, best_halfspread
    except (InvalidOperation, ValueError, IndexError):
        return False
    return min_mid <= mid <= max_mid and hs >= min_hs


def _read_scan(path: str, max_age_s: float, *, min_mid: Decimal, max_mid: Decimal, min_hs: Decimal):
    """Latest census → (fresh, best_ticker, best_halfspread, best_mid, streak).

    `streak` = number of trailing consecutive census rows whose widest live market is balanced-and-wide
    (`_row_is_balanced_wide`); a whipsawing or lopsided book breaks the streak. best_ticker/hs/mid are the
    LATEST row's (the most current target). fresh=False if the latest row is stale/missing.
    """
    try:
        rows = list(csv.reader(open(path, newline="")))
    except OSError:
        return False, "", "", "", 0
    # ⛔ ASSERT THE HEADER. This function reads by FIXED INDEX (r[7] best_ticker, r[9] best_mid,
    # r[10] best_halfspread) against the standing rule to map by header NAME. It fails safe
    # against the census's 19-column schema only by ACCIDENT — twice over, and both accidents are
    # incidental. What it does NOT survive is a change to the SCAN'S OWN schema: insert one column
    # before index 9 and this reads best_mid/best_halfspread from the wrong columns while r[7] stays
    # a legitimate AUTHORISED ticker, so the sever passes, the flat check passes, and a real-money
    # run fires on a book that never satisfied the balanced-wide gate. That is fail-OPEN on the money
    # path. Refusing on an unexpected header is the cheap fix.
    if not rows or rows[0][:11] != _EXPECTED_SCAN_HDR:
        print(f"[{time.time():.0f}] ⛔ REFUSING: {path} header is not the expected scan schema "
              f"(got {rows[0][:11] if rows else 'empty'}) — this reader maps by fixed index, so a "
              f"changed schema would silently read the wrong columns.", flush=True)
        return False, "", "", "", 0
    # ⛔ WIDTH MUST MATCH THE HEADER, and `>= 11` was the hole the header check could not see.
    # `maker_market_scan` opens its CSV with "a" and writes the header ONLY when the file is missing
    # or empty — so on the exact event this guard exists for (a column inserted into its schema), the
    # deployed file KEEPS ITS OLD HEADER while the appended rows carry the new shape. The header
    # assert passes, `>= 11` tolerates the drift, and every index after the insertion point reads the
    # wrong column: demonstrated returning fresh=True, streak=2, authorised=True on a book far too
    # tight to have passed the width gate. Sever passes, flat check passes, real money fires on a book
    # that never satisfied the balanced-wide gate.
    # Comparing against the header ACTUALLY PRESENT catches both the changed-header and the
    # appended-under-a-changed-schema case.
    data = [r for r in rows[1:] if len(r) == len(rows[0]) and r[0]]
    if not data:
        return False, "", "", "", 0
    r = data[-1]
    try:
        age = time.time() - float(r[0])
    except ValueError:
        return False, "", "", "", 0
    if age > max_age_s:
        return False, "", "", "", 0
    # SAME-TICKER + CONTIGUOUS streak: the streak must be continuity of THE book we would fire
    # on (r[7]), not of the widest-contested SLOT — else a brand-new, freshly-wide market inherits an older
    # market's streak and fires with ~one cycle of history (the whipsaw we're trying to avoid). And a gap
    # ≫ the scan cadence (e.g. a scanner restart) must break it — two in-band rows an hour apart are not
    # "sustained". NOTE: a 60s sample rate cannot resolve a ~10s whipsaw (Nyquist), so this biases toward a
    # stable book but the maker's loss cap, not this gate, is the real backstop for a between-sample swing.
    target = r[7]
    streak = 0
    newer_ts = None
    for row in reversed(data):
        if row[7] != target or not _row_is_balanced_wide(row, min_mid, max_mid, min_hs):
            break
        try:
            ts = float(row[0])
        except (ValueError, IndexError):
            break
        if newer_ts is not None and newer_ts - ts > max_age_s:
            break
        streak += 1
        newer_ts = ts
    return True, r[7], r[10], r[9], streak


async def _is_flat() -> bool | None:
    """True if the account is flat, False if any open position, None if the read failed (CANNOT-VERIFY)."""
    c = KalshiClient()
    try:
        pos = await c.get_positions()
    except Exception as e:
        print(f"  ⚠️ flat check FAILED ({e!r}) — treating as NOT-verifiable", flush=True)
        return None
    finally:
        await c.close()
    for p in pos:
        # FAIL-CLOSED: the old `p.get("position_fp", p.get("position", 0))` returned None on a
        # present-but-null position_fp and the bare except SKIPPED an unparseable row — either could read a
        # NON-flat account as flat and start a real run on inherited inventory (invisible to inv=cur-base).
        # Reuse reconcile._first_qty: None = CANNOT-VERIFY (never a fabricated 0) → refuse the run.
        q = _first_qty(p, "position_fp", "position")
        if q is None:
            print(f"  ⚠️ position row {p.get('ticker','?')} has unreadable qty — CANNOT VERIFY flat "
                  f"(fail-closed, refusing)", flush=True)
            return None
        if q != 0:
            return False
    return True


# ⛔ THE SEVER. What this supervisor may point REAL MONEY at is defined HERE, in the
# money path, and NEVER by whatever series happen to appear in the scan CSV.
#
# Before this, `--scan-csv` defaulted to the census file, the supervisor read `best_ticker` from it,
# and passed it straight to `kalshi_live_mm --ticker … --i-understand-real-money`. So ADDING A LINE TO
# THE SCANNER'S SERIES LIST WIDENED WHAT AN AUTOMATED REAL-MONEY RUN COULD QUOTE — the scanner's own
# header says so, and says to sever this coupling BEFORE re-widening. The maker re-validates fee,
# band, event and phase, but none of those is series-aware, so nothing downstream could catch
# "this series should not be in the universe."
#
# That coupling is now the wrong shape for what we need next: the census must widen to hundreds of
# maker-free series to answer "which markets should we even be on", and a discovery scan must be free
# to look at a market without thereby authorising money to be sent at it. Discovery and authorisation
# are different decisions and this is the line between them.
#
# Keep this list SMALL and deliberate. Widening the census does not touch it.
# The first 11 columns of `maker_market_scan`'s header, in order. `_read_scan` maps by index, so this
# is the contract that makes that safe — see the assert there.
_EXPECTED_SCAN_HDR = ["ts", "n_makerfree", "n_with_book", "n_live", "n_live_contested",
                      "n_live_wide", "n_pregame_contested", "best_ticker", "best_phase",
                      "best_mid", "best_halfspread"]

# ⛔ DELIBERATELY EMPTY — NO SERIES IS AUTHORISED FOR AN UNATTENDED REAL-MONEY RUN.
#
# It previously held a hand-picked handful of series. A whole-venue census then measured every one
# of them on the queue a size-1 quote must actually clear (median contracts resting at the touch,
# over markets that are BOTH wide enough to quote and actually trading, sampled hourly across the
# hours a run would occupy) — and **every one failed**. The ranking it produced had this shape:
#
#     SERIES-A   median touch    ~50   (quotable in every sampled cycle)
#     SERIES-B   median touch   ~200   (quotable in fewer than half of them)
#     SERIES-C   median touch  ~1,000
#     SERIES-D   median touch  ~4,000  (quotable in a single cycle)
#     SERIES-E   — never has a wide AND traded market at all, in any cycle
#
# ⚠️ THAT TABLE IS ILLUSTRATIVE. Both the ranking and the pass/fail threshold are venue-, size- and
# hour-specific CONFIGURATION, not universal constants — re-measure them for your own run before
# they mean anything. What generalises is the method: a size-1 quote is decided by the QUEUE it must
# clear, not by the series' raw volume, and PRESENCE matters as much as depth (a series that is only
# occasionally quotable cannot be the target of an unattended run).
#
# A list whose every entry is known-bad is WORSE than an empty one: it reads as "these are vetted"
# and confers assurance nothing earned. Emptying it is the safe direction by construction —
# this list can only ever PREVENT a trade, never cause one — and it is the honest description of the
# current state, which is that nothing has been vetted for unattended real money.
#
# ⚠️ THIS DOES NOT GATE AN ATTENDED RUN, and that distinction is the reason emptying it costs nothing.
# It gates THIS supervisor, which launches runs unattended. An attended run invokes
# `scripts.kalshi_live_mm` directly with `--i-understand-real-money` and never passes through here.
# So "what may run unattended" (nothing) and "what should the next ATTENDED run target" are separate
# questions, and this answers only the first. Do NOT promote an attended run's target into this list
# on the strength of a written plan: a day of TRADE-TAPE evidence with no fill data of our own is
# nowhere near enough to authorise an unattended real-money run.
#
# To re-add a series: measure its gated touch across the hours a run would occupy, and show it is
# STABLE rather than momentarily shallow — the failure mode is a series that reads rank 1 at single-
# digit depth in one window and two orders of magnitude deeper six hours later. Then put it here
# deliberately with the evidence — in its own commit, with a money-path review. Widening the census
# still does not touch this list.
_SUPERVISOR_ALLOWED_SERIES: frozenset[str] = frozenset()


def _unauthorised_series(ticker: str) -> list[str]:
    """Every series in `ticker` that is NOT authorised. Empty ⇒ safe to launch.

    ⚠️ `--ticker` IS A COMMA-SEPARATED LIST, and the first version of this guard forgot it. The maker
    does `{t.strip() for t in args.ticker.split(",")}` and then ranks the resulting markets by
    `(maker_mult, -volume_24h)` — so `"KXNPBGAME-…,KXMLBHR-…"` passed a first-segment-only check and
    the maker would then have picked the HIGHEST-VOLUME one, i.e. the unvetted `KXMLBHR`, which is
    the single highest-volume book on the venue. The guard written to make that impossible would have
    waved it through. Every segment is validated now, and a blank/garbage segment counts as
    unauthorised rather than being skipped."""
    parts = [p.strip() for p in (ticker or "").split(",")]
    bad = []
    for p in parts:
        s = p.split("-", 1)[0]           # canonical series extraction, matches maker.py:_series_of
        if s not in _SUPERVISOR_ALLOWED_SERIES:
            bad.append(s or "<empty>")
    return bad


def _launch_maker(ticker: str, run_seconds: int, live: bool) -> subprocess.Popen:
    # ⛔ FAIL CLOSED — the BACKSTOP. Selection already skips an unauthorised ticker (see main()), so
    # reaching this raise means something bypassed that path; it is deliberately unrecoverable rather
    # than a skip, because at this point the next statement spawns a real-money process.
    bad = _unauthorised_series(ticker)
    if bad:
        raise RuntimeError(
            f"REFUSING to launch the maker on {ticker!r}: series {bad} not in "
            f"_SUPERVISOR_ALLOWED_SERIES. The census may surface any maker-free series; this "
            f"supervisor may only ACT on a vetted one. Add it here deliberately, not by widening "
            f"the scan.")
    cmd = [sys.executable, "-m", "scripts.kalshi_live_mm", "--ticker", ticker, *_MAKER_CFG,
           "--seconds", str(run_seconds)]
    if live:
        cmd.append("--i-understand-real-money")
    log = open("logs/maker_supervisor_run.log", "a")
    log.write(f"\n===== supervisor launch {time.time():.0f} ticker={ticker} live={live} =====\n")
    log.flush()
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)


async def main() -> None:
    # ⛔ NO ABBREVIATIONS. argparse defaults to `allow_abbrev=True`, so `--liv`, `--li` and even
    # `--l` all set `live=True` — a fully armed real-money supervisor whose argv the census's
    # stand-down regex (`maker_supervisor.*--live`) cannot see. The census would then fire its
    # long /series burst into a live run. An argv-regex interlock is best-effort by nature;
    # the least it can get is an argv that says what it means.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--scan-csv", default="logs/maker_market_scan.csv")
    ap.add_argument("--scan-max-age-s", type=float, default=180.0)
    ap.add_argument("--poll-s", type=float, default=45.0)
    ap.add_argument("--run-seconds", type=int, default=300, help="bounded maker run length per opportunity")
    ap.add_argument("--min-mid", type=Decimal, default=Decimal("0.35"),
                    help="fire only if the surfaced widest book's mid ≥ this (balanced — not a converging longshot)")
    ap.add_argument("--max-mid", type=Decimal, default=Decimal("0.65"),
                    help="fire only if the surfaced widest book's mid ≤ this")
    ap.add_argument("--min-halfspread", type=Decimal, default=Decimal("0.03"),
                    help="fire only if the surfaced best half-spread ≥ this (wide enough to clear costs); scanner flags ≥0.02")
    ap.add_argument("--min-wide-streak", type=int, default=2,
                    help="require the SAME market to read balanced-wide for this many consecutive ~60s samples "
                         "before firing (2 ≈ a 60s span). Biases away from whipsaws; the loss cap is the real "
                         "backstop for a swing between samples")
    ap.add_argument("--live", action="store_true", help="FIRE REAL runs (else DRY: log would-start only)")
    ap.add_argument("--max-runs", type=int, default=3, help="cap on real runs fired (0 = unlimited)")
    ap.add_argument("--seconds", type=float, default=0.0, help="supervisor lifetime; 0 = forever")
    args = ap.parse_args()
    if args.min_wide_streak < 1:
        # A streak count is always ≥0, so `streak >= 0` is unconditionally true → the balanced-wide gate
        # collapses to "latest best_ticker non-empty", firing on ANY wide-contested book (tight/lopsided
        # included) — WIDER than the pre-gate behavior. Refuse rather than silently disable a real-money gate.
        ap.error("--min-wide-streak must be ≥ 1 (0 or negative would disable the balanced-wide gate)")

    mode = "🔴 LIVE (real orders)" if args.live else "🟢 DRY (logs only, fires NOTHING)"
    print(f"maker supervisor: {mode} | scan={args.scan_csv} poll={args.poll_s:.0f}s "
          f"run={args.run_seconds}s max_runs={args.max_runs}", flush=True)
    t0 = time.time()
    runs = 0
    while args.seconds == 0 or time.time() - t0 < args.seconds:
        fresh, best_t, best_hs, best_mid, streak = _read_scan(
            args.scan_csv, args.scan_max_age_s,
            min_mid=args.min_mid, max_mid=args.max_mid, min_hs=args.min_halfspread)
        if not fresh:
            print(f"[{time.time():.0f}] scan STALE/missing (>{args.scan_max_age_s:.0f}s) — no action", flush=True)
        elif streak < args.min_wide_streak or not best_t:
            # Gate: fire only on a SUSTAINED balanced-wide window, not any wide book. The scanner's
            # widest-first best_ticker is usually a lopsided converging longshot mid-game; the mid-band +
            # streak filter keeps us out of those (the whipsaw regime), which is the whole point of the gate.
            # An empty best_ticker also lands here (never fires an empty --ticker → no series-wide fallback).
            print(f"[{time.time():.0f}] ⚪ no sustained balanced-wide window "
                  f"(streak {streak}/{args.min_wide_streak}; latest best {best_t or '—'} "
                  f"mid={best_mid or '—'} half-spread={best_hs or '—'}) — waiting", flush=True)
        elif _unauthorised_series(best_t):
            # ⚠️ SKIP, DON'T DIE — and check it BEFORE the DRY branch, not just before the live one.
            # Two things this fixes. (1) The census is widening to hundreds of maker-free series and
            # `best_ticker` is widest-first over all of them, so the sustained window will USUALLY be
            # an unvetted series; raising here would kill the supervisor on its first opportunity and
            # stop it watching for the vetted one ten minutes later — a tool that breaks exactly when
            # it starts being useful. (2) The DRY branch never calls `_launch_maker`, so before this
            # the preview was MORE PERMISSIVE than the live path: it printed an encouraging "WOULD
            # start maker --ticker KXMLBHR-…" for a series the live path refuses. A rehearsal that is
            # wider than the real thing is the wrong direction for a preview.
            # The raise in `_launch_maker` stays as the unbypassable backstop.
            print(f"[{time.time():.0f}] ⚪ surfaced {best_t} (mid {best_mid}, half-spread {best_hs}) "
                  f"but series {_unauthorised_series(best_t)} is NOT AUTHORISED — skipping. "
                  f"Discovery may surface it; only _SUPERVISOR_ALLOWED_SERIES may be acted on.",
                  flush=True)
        else:
            if not args.live:
                print(f"[{time.time():.0f}] 🟢 sustained balanced-wide (streak {streak}); best {best_t} "
                      f"(mid {best_mid}, half-spread {best_hs}). WOULD start maker --ticker {best_t} [DRY — firing nothing]",
                      flush=True)
            elif args.max_runs and runs >= args.max_runs:
                print(f"[{time.time():.0f}] 🟢 opportunity but --max-runs {args.max_runs} reached — stopping",
                      flush=True)
                break
            else:
                flat = await _is_flat()
                if flat is not True:
                    print(f"[{time.time():.0f}] 🟢 opportunity but NOT flat/verifiable "
                          f"({'open position' if flat is False else 'read failed'}) — REFUSING to start",
                          flush=True)
                else:
                    print(f"[{time.time():.0f}] 🟢 STARTING attended maker run: --ticker {best_t} "
                          f"(mid {best_mid}, half-spread {best_hs}, streak {streak}) run={args.run_seconds}s [LIVE]",
                          flush=True)
                    proc = _launch_maker(best_t, args.run_seconds, live=True)
                    runs += 1
                    deadline = time.time() + args.run_seconds + 180   # its --seconds + flatten-wait + margin
                    while proc.poll() is None:
                        if time.time() > deadline:
                            print(f"[{time.time():.0f}] ⚠️ maker run OVERRAN its bounds — SIGINT (teardown)",
                                  flush=True)
                            proc.send_signal(signal.SIGINT)
                            try:
                                proc.wait(timeout=90)
                            except subprocess.TimeoutExpired:
                                print(f"[{time.time():.0f}] ⚠️ still alive after SIGINT — terminating; "
                                      f"CHECK THE KALSHI UI for resting orders", flush=True)
                                proc.terminate()
                            break
                        await asyncio.sleep(15)
                    print(f"[{time.time():.0f}] maker run ended rc={proc.returncode} (run {runs}"
                          f"{'/' + str(args.max_runs) if args.max_runs else ''}) — see "
                          f"logs/maker_supervisor_run.log", flush=True)
        await asyncio.sleep(args.poll_s)


if __name__ == "__main__":
    asyncio.run(main())
