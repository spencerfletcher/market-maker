"""Two-stage market screener: which books on the venue can a size-N maker actually earn on?

The venue lists thousands of active markets. A maker can quote a handful. Everything between
those two numbers is a selection problem, and hand-picking is not a way to solve it — a
hand-picked set can quote flawlessly and still earn nothing, because a market's ability to pay a
maker at all is an arithmetic property of its PRICE and the order SIZE, and that property is
invisible in every operational metric (fill counts, uptime, cancel rates all look healthy while
the rebate rounds to zero).

This tool answers the mechanical half of that question for the whole universe, and hands the
answer to the operator. It RANKS; it does not authorize.

THE TWO STAGES, and why the ordering is the point
-------------------------------------------------
The expensive signal is not the scarce one — request budget is.

  Stage 1 — STRUCTURAL VALIDITY, free. The market listing already carries best bid, best ask and
    the minimum tick inline, so price, spread, tick class and rebate-clearance are computable for
    every market in the universe without a single book fetch. Almost everything dies here.

  Stage 2 — LIQUIDITY, one book read per market. Touch depth (the queue a joined order would sit
    behind) and recent flow are NOT in the listing; each one costs a request against a per-second
    budget shared with the live maker.

Running validity first is what makes the screen affordable: it turns "read the book of every
market" into "read the book of the few hundred that could pay". Do it the other way round and the
rate budget is spent almost entirely on markets that stage 1 would have rejected for free. That
ordering — cheap structural gates before any paid measurement — is the whole design.

WHAT STAGE 1 GATES ON, and why each is arithmetic rather than taste
------------------------------------------------------------------
  - **The rebate must survive rounding.** The maker credit is `coef * p * (1-p) * size`, paid per
    fill and rounded to the cent. Below that boundary the market pays exactly zero no matter how
    well it fills. A hard gate, not a score: a longshot at p=0.02 at small size is unearnable, and
    no amount of execution quality changes that.
  - **Price near 0.5 is worth more.** The credit is quadratic in `p*(1-p)` while improving the
    quote by one tick costs a flat tick, so the same tick surrenders a much larger share of the
    credit at extreme prices than at the middle. `improve_cost_frac` is that share, and it is the
    whole join-vs-improve trade in one number.
  - **A spread ceiling.** Rebate arithmetic alone will happily rank a book quoting 0.001 against
    0.49 at the top — that is not a wide market, it is an empty book with two stray orders in it.
    The credit is only collected on a FILL, so spread with no flow behind it scores nothing.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It reads no settlement rules and no per-market fee schedule. A high rank is not a claim that a
market is safe to quote — only that it is not disqualified by the rebate floor. Selection is the
operator's: this produces the candidate set and the measurements, and the launch-side gates decide
what gets seated.

    python -m scripts.poly_market_screen --size 20 --top 25
    python -m scripts.poly_market_screen --size 20 --depth --csv passes/screen_20260101T0000.csv

⚠️ `--size` decides WHICH MARKETS EXIST downstream: the rebate floor is monotonic in it, so a
`--size N` pass is a superset of what any size <= N admits and BLIND above N. Screening smaller
than you intend to quote silently deletes books the maker could seat. Every row records the size
that built it in `screen_size` — read it before comparing two passes.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import glob
import os
import re
import time
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

from bot.poly_us.client import PolyUSClient, touch_from_md, _parse_book_stats

#: |Θ| for the venue's maker rebate: the credit is Θ·size·p·(1−p) per fill.
REBATE_COEF = Decimal("0.0125")
# A `fillable` Y/N verdict briefly lived here — a queue-depth ceiling above which a joined order
# was declared unfillable — and was WITHDRAWN, because every constant in it was argued from a
# summary statistic that did not survive contact with the fill tape. Two traps, both worth
# stating because both are easy to walk back into:
#   · a queue-depth percentile pooled over IMPROVED and JOINED fills is meaningless — an improved
#     order's queue-ahead is 0 BY CONSTRUCTION, so pooling drags the percentile toward zero and
#     makes joining look far safer than it is. Split by action before quoting any such number.
#   · "no fill was ever recorded from behind depth D" is a statement about the books that were
#     sampled, not about depth. Fills do get recorded from very deep in the queue, on the books
#     that actually trade.
# Fillability is queue-relative-to-FLOW and book-specific. Until a rule can earn its constants
# from a tape, the screen reports BOTH touch sides and the flow ranking, and leaves the verdict
# to the operator.
CENT = Decimal("0.01")
HALF_CENT = Decimal("0.005")
#: Seconds between stage-2 book reads. This is a POLITENESS pace, not the venue's limit — it sits
#: well under the documented per-second budget, and that is deliberate: the screen shares an IP
#: and an API key with a live maker, and throttling the maker's own book reads is a money-path
#: risk, not a data one.
#:
#: It is also the binding constraint on the whole pipeline. A book needs a MEASURED queue to rank,
#: and this pace decides how many books a pass can measure — so the size of the rankable universe
#: is set here, not by the venue. If you raise it, raise it from a machine and key that no live
#: maker is using, step down gradually (1.0 → 0.5 → 0.3 → …) and watch for 429s and CDN challenge
#: pages: an edge provider may sit in front of the documented app limit with its own opaque burst
#: rules, and bursts alongside a live maker are exactly what trips them.
DEPTH_PACE_S = 0.5
PAGE = 100
# A bound, not a belief about the universe size — the loop stops on an empty page. This only keeps
# a venue-side pagination bug from spinning forever.
MAX_PAGES = 200

#: A screen pass file, by name. Pass identity is the timestamp in the filename, and several
#: things key off it: the sticky panel, the coverage scan, and the refusal that keeps a reject
#: roster from ever being mistaken for a pass.
PASS_NAME_RE = re.compile(r"screen_(\d{8}T\d{4})\.csv$")


def _dec(raw: Any) -> Optional[Decimal]:
    """Parse a venue value to Decimal from its STRING form, accepting the {value,currency} wrapper.

    Never Decimal(float): that launders the float's error into the Decimal and defeats the point.
    """
    if isinstance(raw, dict):
        raw = raw.get("value")
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except Exception:  # noqa: BLE001 - a malformed quote is data, not a crash
        return None


def rebate(price: Decimal, size: int) -> Decimal:
    """Un-rounded maker rebate for `size` contracts at `price`."""
    return REBATE_COEF * price * (Decimal(1) - price) * Decimal(size)


def rebate_cents(price: Decimal, size: int) -> Decimal:
    return rebate(price, size).quantize(CENT, rounding=ROUND_HALF_UP)


def clears_floor(price: Decimal, size: int) -> bool:
    """Does a filled order actually pay anything, after nearest-cent rounding?"""
    return rebate(price, size) >= HALF_CENT


def min_clearing_size(price: Decimal, cap: int = 500) -> Optional[int]:
    """Smallest size whose rebate survives rounding at this price — None if none up to `cap`."""
    for n in range(1, cap + 1):
        if clears_floor(price, n):
            return n
    return None


@dataclass
class Candidate:
    slug: str
    category: str
    bid: Decimal
    ask: Decimal
    mid: Decimal
    tick: Decimal
    spread_ticks: int
    rebate_usd: Decimal
    min_size: Optional[int]
    # improving costs one tick against a rebate that scales with p(1-p); this is what fraction of
    # the rebate that tick eats, and it is the whole join-vs-improve economics in one number.
    improve_cost_frac: Optional[Decimal]
    touch_qty: Optional[Decimal] = None
    # ⛔ BOTH SIDES, always. Roughly half of a two-sided maker's fills are ask-side, and the two
    # sides of the same book can diverge by three orders of magnitude — a book screened on its bid
    # touch alone can look like the best queue on the bench while a wall rests on the ask. Any
    # gate built on this data must read max(touch_qty, touch_qty_ask), never the bid alone.
    touch_qty_ask: Optional[Decimal] = None
    shares_traded: Optional[float] = None
    last_trade_age_s: Optional[float] = None
    open_interest: Optional[float] = None
    # The map is a --size-N universe (the rebate floor and the min-size gate are both
    # size-dependent), so a reader must know which N produced the row. Appended LAST so the CSV
    # schema change is add-only — downstream consumers read by DictReader field name; a
    # positional reader is on its own.
    screen_size: Optional[int] = None
    # Which tier measured this row's depth: "fresh" = cache-busted origin read (the panel —
    # decisions read these), "cached" = un-nonced read, up to the CDN's max-age stale — fine for
    # coverage screening, and an analysis comparing queue depths must not mix the two blindly.
    depth_src: str = ""
    # Which DISCOVERY produced this row: "markets" (the market listing) or "events" (the event
    # listing). ⛔ A THIRD POPULATION AXIS, and without it a later read cannot tell "this book
    # stopped trading" from "this book left the universe": the two listings do not enumerate the
    # same set, so a flow series spanning a change of discovery mixes populations silently. Any
    # per-slug time series must be keyed on (producer, screen_size, discovery) for that reason.
    # A blank value means a row written before this column existed.
    discovery: str = ""
    # ⛔ WHEN THIS ROW'S BOOK WAS ACTUALLY READ — unix seconds, stamped inside `add_depth` at the
    # moment the read returns. Blank on a row whose depth was never read.
    #
    # THE DEFECT IT EXISTS TO FIX, because it generalises to any batch producer. A consumer with
    # no per-row time derives row age from the PASS FILENAME — which is stamped when the pass
    # STARTS, while the CSV is written in one shot only after thousands of paced reads. A pass
    # whose publish lag approaches its own cadence therefore emits rows that are already "older"
    # than any sane staleness ceiling the instant they exist, and the consumer refuses the entire
    # addressable universe as stale for most of every cycle — printing a zero score beside a gate
    # name that means "we do not know", not "worthless". It is not stale data: the timestamp
    # described when we started looking, not when we looked.
    #
    # It is also the PREREQUISITE for any bound on how long ago a book last traded: true time
    # since a print is `last_trade_age_s + row_age`, and without this column that sum is not
    # merely uncalibrated, it is UNCOMPUTABLE.
    #
    # ⛔⛔ WHAT THIS CERTIFIES, AND WHAT IT DOES NOT. `add_depth` sets six columns — `touch_qty`,
    # `touch_qty_ask`, `depth_src`, `shares_traded`, `last_trade_age_s`, `open_interest` — plus
    # the price columns, but only when `_refresh_price` accepts the fresh touch. Read `price_src`
    # to know which happened. `read_ts` is the read time of the DEPTH and FLOW columns
    # unconditionally; it certifies the PRICE columns only when `price_src == "fresh"`.
    #
    # This matters because the rebate floor is evaluated on the price columns, and listing quotes
    # move materially over the span of a long sweep — enough of them to flip a book's
    # `clears_floor` verdict. Any consumer seating real size must re-read the book live rather
    # than trust a screen row's price.
    #
    # Appended LAST, the `screen_size`/`discovery` precedent. Readers must treat blank as UNKNOWN
    # and fall back to the pass stamp, never to "fresh".
    read_ts: Optional[float] = None
    # ⛔ WHERE THIS ROW'S PRICE CAME FROM. "fresh" = re-derived from the depth read, so
    # `bid`/`ask`/`mid`/`spread_ticks`/`rebate_usd`/`min_size`/`improve_cost_frac` share the
    # observation `read_ts` stamps. "listing" = the depth read happened but its touch was
    # one-sided or crossed, so the price is still the pass-start crawl's and is as old as the
    # pass. Blank = no depth read at all.
    #
    # A reader judging PRICE freshness must check this, not `read_ts` alone: the two can
    # legitimately disagree by a full sweep.
    price_src: str = ""

    def band(self) -> Decimal:
        """The tick-FREE rebate band: what a filled contract pays, before any improve-cost
        opinion. This — never `score()` — ranks who gets MEASURED (see select_depth_panel)."""
        return REBATE_COEF * self.mid * (Decimal(1) - self.mid)

    def score(self) -> Decimal:
        """Rank key: rebate per contract, discounted CONTINUOUSLY by improvement cost.

        ⛔ An earlier form multiplied by max(0, 1 − improve_cost_frac), which is exactly 0 for
        every book whose tick exceeds its per-contract rebate — i.e. for the overwhelming
        majority of the universe. Nearly every candidate tied at zero, the stable sort preserved
        venue listing order, and "ranking" was silently tick-size-then-listing-position for
        months. The 1/(1+icf) form keeps the same intent (improving-friendly books first) but
        stays strictly ordered by band inside every tick class instead of collapsing to a tie.

        The general lesson: a rank key with a clamp in it can degenerate to a constant over most
        of its domain, and a stable sort will hide that behind plausible-looking output. Check the
        DISTRIBUTION of any score, not just its top rows.
        """
        per_contract = self.band()
        if self.improve_cost_frac is None:
            return per_contract
        return per_contract / (Decimal(1) + self.improve_cost_frac)


def prior_depth_slugs(out_csv: Optional[str]) -> set[str]:
    """Sticky panel: the slugs depth-read in the NEWEST previous pass in the output dir.

    Adjacent passes that read disjoint panels produce almost no flow DELTAS — the map grows but
    measures nothing, because a change in traded volume needs the same book read twice. Carrying
    the previous panel forward is what creates that overlap. A row counts as depth-read iff its
    flow column is populated. Reads the directory of the OUTPUT path so a frozen scheduled loop
    needs no new flag. Only TIMESTAMPED pass files count — a one-off `screen_manual.csv` sorts
    after every timestamped name and would otherwise become "the previous pass" permanently. Any
    unreadable prior degrades to no-sticky, never a crashed pass."""
    if not out_csv:
        return set()
    directory = os.path.dirname(out_csv) or "."
    mine = os.path.abspath(out_csv)
    prior = sorted(f for f in glob.glob(os.path.join(directory, "screen_*.csv"))
                   if os.path.abspath(f) != mine and PASS_NAME_RE.search(f))
    if not prior:
        return set()
    try:
        with open(prior[-1]) as fh:
            return {r["slug"] for r in csv.DictReader(fh) if r.get("shares_traded")}
    except (OSError, KeyError, UnicodeDecodeError, csv.Error):
        return set()


# ⚙️ CONFIGURATION, not a finding. Slug prefixes whose family should be depth-read regardless of
# where it ranks — the escape hatch for "measure this because I asked, not because the arithmetic
# likes it". THE REAL LIST IS THE OPERATOR'S: which market families are worth measuring is exactly
# the selection judgement this tool exists to inform, and it is venue-, season- and
# strategy-specific. The two entries below are illustrative placeholders showing the shape (a slug
# prefix, and a note on why it was requested and when to prune it).
#
# Two rules that DO generalise:
#   · absence from depth history means UNMEASURED, not dead — a family that has never been read
#     cannot have been ruled out, and forcing a seat is how it earns a verdict;
#   · the set must be CAPPED. An uncapped privileged set swallows the whole per-pass read budget
#     within a few passes and freezes the panel — the same starvation failure the sticky cap
#     below exists to prevent. Prune a prefix once its family has accrued enough cells to judge.
FORCED_DEPTH_PREFIXES: tuple[str, ...] = (
    "example-daily-",   # a family whose strikes ROTATE DAILY: forced seats are what keep fresh
                        # cells on each day's live books, since yesterday's slug is gone.
    "example-event-",   # a family measured for a dated event ramp rather than as a seat
                        # candidate; prune once the event resolves.
)
FORCED_DEPTH_CAP = 12

#: A sports book whose slug's latest date is at least this many days out AND whose family is not a
#: known single-GAME family is a FUTURE (season wins, championships, awards) rather than a live
#: game. The distinction is a MEASUREMENT LANE, not a permission: lane membership stratifies the
#: depth panel so one lane cannot starve another, and says nothing about whether a book is safe to
#: quote. Whether any given lane is worth seating is a selection decision made downstream, on
#: measured results.
SPORTS_FUTURES_MIN_DAYS = 7.0
#: Slug families that are single-GAME markets regardless of date. A date test ALONE misreads these
#: badly, because leagues list full season slates months ahead — so a game months out looks like a
#: future by date and is nothing of the kind. Shape first, date second.
#:
#: ⚙️ CONFIGURATION, not a finding. THE REAL LIST IS THE OPERATOR'S: which slug prefixes denote a
#: per-game family is venue-specific, shifts as the venue lists new leagues, and is part of the
#: market taxonomy this tool exists to INFORM rather than to assert. The entries below are
#: illustrative placeholders showing the shape. An EMPTY tuple is a legitimate configuration —
#: `sports_lane` then falls back to the date test alone, which is the weaker half, and says so.
LIVE_GAME_SLUG_PREFIXES: tuple[str, ...] = ("example-game-", "example-match-")
_SLUG_DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_SLUG_DATE_MDY = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\b")


def sports_lane(slug: str, category: Optional[str], now_ts: Optional[float] = None) -> str:
    """'nonsports' | 'sports_futures' | 'sports_live' — the measurement lane for panel
    stratification and discovery eligibility. Shape first (a two-team game family is a game
    at any horizon), date second; the residual error rate of the date half on non-prefixed
    families is UNMEASURED — stated, not hidden."""
    if (category or "").lower() != "sports":
        return "nonsports"
    if slug.startswith(LIVE_GAME_SLUG_PREFIXES):
        return "sports_live"
    now = time.time() if now_ts is None else now_ts
    dates = _SLUG_DATE.findall(slug)
    dates += [f"{y}-{m}-{d}" for m, d, y in _SLUG_DATE_MDY.findall(slug)]
    if not dates:
        return "sports_futures"
    try:
        import datetime as _dt
        latest = max(_dt.datetime.strptime(d, "%Y-%m-%d").replace(
            tzinfo=_dt.timezone.utc).timestamp() for d in dates)
    except ValueError:
        return "sports_futures"
    days_out = (latest - now) / 86400.0
    return "sports_futures" if days_out >= SPORTS_FUTURES_MIN_DAYS else "sports_live"


def select_depth_panel(cands: list[Candidate], top: int,
                       sticky_slugs: set[str],
                       coverage: Optional[dict[str, float]] = None) -> list[Candidate]:
    """Choose who gets a stage-2 DEPTH/FLOW read. The scarcest resource in the tool, so the
    selection rule matters more than the ranking does.

    ⛔ NEVER selected by `score()`. The improve-cost discount caps every coarse-tick book below
    the top-N cutoff structurally, so selecting the panel by score means an entire tick class can
    never be measured — the CHEAP signal deciding what the EXPENSIVE signal is allowed to see,
    which is how a screen ends up confidently ranking the small slice it happened to look at.
    Selection instead:

      (1) FORCED seats (above), capped.
      (2) STICKY, CAPPED at 2/3 of the budget. Every book depth-read last pass is a re-read
          candidate so adjacent passes overlap and flow deltas exist — but ⛔ an UNCAPPED sticky
          set swallows the whole budget within a few passes and freezes the panel forever: zero
          new books admitted, the unmeasured tail never measured, the original defect back in
          steady state. The cap guarantees ≥⌈top/3⌉ of EXPLORATION every pass. Sticky books
          beyond the cap compete in the strata like everyone else and can re-enter.
      (3) The exploration budget is split as per-stratum FLAT quotas over (tick class × lane), so
          no single stratum can crowd out another. Inside a stratum, exploration is
          STARVATION-FIRST: never-depth-read books lead, then oldest-read (`coverage` =
          `depth_coverage()`'s slug→last-read-ts map; band breaks ties).

          ⛔ The coverage gain comes from that SORT ORDER, not from quota shape. Size-weighting
          the quotas was tried and reverted on a decomposition replay: starvation-first delivered
          effectively all of the never-read admissions on its own, the weighting delivered none of
          them and removed books from the lane most likely to be seatable. If you are tempted to
          tune the quota, decompose the arms first and check which one is actually doing the work.
      (4) Leftover budget falls to global band order.

    `score()` still ranks the display and the post-depth flow re-rank — the improve-cost term is
    economically real, it just must not gate measurement. The panel never exceeds `top`: each
    member is a paced origin GET, and a stale oversized sticky set must not inflate every later
    pass's request bill."""
    # Dedupe by slug (offset pagination can re-serve a market; a duplicate is a wasted GET).
    seen: set[str] = set()
    uniq: list[Candidate] = []
    for c in cands:
        if c.slug not in seen:
            seen.add(c.slug)
            uniq.append(c)

    # (0) FORCED seats first — an explicit measurement request outranks ranking, capped.
    forced = sorted((c for c in uniq if c.slug.startswith(FORCED_DEPTH_PREFIXES)),
                    key=lambda c: c.band(), reverse=True)
    panel: list[Candidate] = forced[:FORCED_DEPTH_CAP]
    chosen = {c.slug for c in panel}

    sticky_cap = (2 * top) // 3
    sticky_cands = sorted((c for c in uniq if c.slug in sticky_slugs
                           and c.slug not in chosen),
                          key=lambda c: c.band(), reverse=True)
    panel += sticky_cands[:sticky_cap]
    chosen = {c.slug for c in panel}
    budget = max(0, top - len(panel))

    cov = coverage or {}
    strata: dict[tuple[str, str], list[Candidate]] = {}
    for c in uniq:
        if c.slug in chosen:
            continue
        strata.setdefault((str(c.tick), sports_lane(c.slug, c.category)), []).append(c)
    for members in strata.values():
        # Starvation-first: never-read (ts 0) leads, then oldest-read; band breaks ties.
        members.sort(key=lambda c: (cov.get(c.slug, 0.0), -c.band()))
    # FLAT quota per stratum — deliberately NOT size-weighted; see the docstring.
    quota = budget // max(1, len(strata))
    for members in strata.values():
        for c in members[:quota]:
            panel.append(c)
            chosen.add(c.slug)
    leftovers = sorted((c for c in uniq if c.slug not in chosen),
                       key=lambda c: c.band(), reverse=True)
    for c in leftovers[: max(0, top - len(panel))]:
        panel.append(c)
    return panel[:top]


def build_candidate(m: dict, size: int, max_spread_ticks: int) -> tuple[Optional[Candidate], str]:
    """Stage 1. Return (candidate, reason); `reason` names the gate that rejected it, or 'ok'."""
    slug = m.get("slug") or ""
    bid, ask = _dec(m.get("bestBidQuote")), _dec(m.get("bestAskQuote"))
    if bid is None or ask is None:
        return None, "no_two_sided_quote"
    if bid <= 0 or ask <= 0:
        return None, "no_two_sided_quote"
    if ask <= bid:
        # A crossed or locked listing quote is not tradeable as read. Accepting one manufactures a
        # phantom-cheap side and a phantom edge that evaporates on contact; do not rank it.
        return None, "crossed_or_locked"

    tick = _dec(m.get("orderPriceMinTickSize"))
    if tick is None or tick <= 0:
        return None, "no_tick"

    mid = (bid + ask) / Decimal(2)
    if not clears_floor(mid, size):
        return None, "rebate_rounds_to_zero"

    spread_ticks = int(((ask - bid) / tick).to_integral_value(rounding=ROUND_HALF_UP))
    if spread_ticks > max_spread_ticks:
        # A book quoting a fraction of a cent against a near-even price is not a market with a
        # wide spread, it is an empty book with two stray orders in it. Left ungated, pure rebate
        # arithmetic ranks such books at the very top — hundreds of ticks wide, no flow at all.
        # The rebate is only collected on a FILL, so spread with nothing behind it scores nothing.
        return None, "spread_too_wide"

    per_contract = REBATE_COEF * mid * (Decimal(1) - mid)
    improve_cost = (tick / per_contract) if per_contract > 0 else None

    return Candidate(
        slug=slug,
        category=str(m.get("category") or ""),
        bid=bid,
        ask=ask,
        mid=mid,
        tick=tick,
        spread_ticks=spread_ticks,
        rebate_usd=rebate_cents(mid, size),
        min_size=min_clearing_size(mid),
        improve_cost_frac=improve_cost,
    ), "ok"


# Per-PAGE retry backoff. A scheduled pass that dies on ONE transient error leaves the whole map
# stale until the next run — and a downstream staleness gate that fails CLOSED will then silently
# block work for hours on what was a single timeout. Retrying a page is not "screening a partial
# universe": the abort below still fires when a page fails every attempt.
_RETRY_BACKOFF_S: tuple[float, ...] = (2.0, 5.0)


#: Events per page. ⛔ MEASURE A PAGINATION LIMIT, DO NOT INFER IT. This constant was long
#: accompanied by a comment asserting the venue capped a page at this size regardless of the
#: `limit` asked for. The claim was SELF-FULFILLING — the code asked for 50, received 50, and
#: concluded the venue capped at 50 — and a live probe against larger limits refuted it outright.
#:
#: ⚠️ The cost was not the extra pages. The wrong comment was later cited AS A VENUE FACT by a
#: review of a DIFFERENT module, to statically "confirm" that a fail-closed gate elsewhere broke
#: after one page. That finding was escalated as critical and a live probe refuted it. A wrong
#: comment in one file produced a false critical finding in another; venue behaviour belongs in a
#: probe, not in prose.
#:
#: 50 is therefore a CHOICE, not a venue limit — raising it changes the crawl's page count and
#: truncation behaviour and deserves its own reviewed change rather than a drive-by edit.
_EVENTS_PAGE = 50
#: Runaway backstop, roughly an order of magnitude above the observed listing. Not a budget.
_EVENTS_MAX_OFFSET = 20_000


async def crawl_events_markets(client: PolyUSClient, pace_s: float = 0.2, *,
                               retries: bool = True, empty_probe: int = 6
                               ) -> tuple[list[tuple[dict, dict]], bool]:
    """((market, event) pairs, complete) from the active-events listing.

    ⛔ A ZERO-LENGTH PAGE IS NOT A TERMINATOR. This venue throttles by serving a stale 200 rather
    than rejecting, so one transient empty response mid-crawl would hand back a partial universe
    labelled complete — the silent-truncation class, which is the worst failure a discovery layer
    has, because every downstream conclusion inherits it and nothing looks wrong. The last page is
    a SHORT page; a zero-length page arriving before any short page is an error, not an end.

    ⚠️ Paging is `limit` + `offset` with NO cursor, and the DEFAULT ordering serves long-closed
    events — the filters are what make this the active universe.

    ⛔ `retries` AND `empty_probe` EXIST BECAUSE A SCHEDULED PASS AND A ONE-SHOT CRAWL WANT
    OPPOSITE POLICIES. The scheduled screen must survive a transient (a lost pass leaves the map
    stale, and a fail-closed staleness gate downstream turns that into blocked work) and must probe
    PAST a lone empty page (a sweep once stopped at exactly a round number of candidates while the
    true end was three times further out). A one-shot operator crawl wants the opposite — hard-fail
    loudly rather than paper over anything — so it passes `empty_probe=1`. The defaults are the
    SAFE-FOR-THE-SCHEDULER values.
    """
    pairs: list[tuple[dict, dict]] = []
    offset = 0
    last_was_short = False
    empty_streak = 0
    backoff = _RETRY_BACKOFF_S if retries else ()
    while offset < _EVENTS_MAX_OFFSET:
        last_exc: Exception | None = None
        r = None
        for attempt in range(len(backoff) + 1):
            try:
                r = await client._sdk.get(
                    f"/v1/events?limit={_EVENTS_PAGE}&active=true&closed=false&offset={offset}")
                last_exc = None
                break
            except Exception as exc:  # noqa: BLE001 — retried; the raise below stays fail-closed
                last_exc = exc
                if attempt < len(backoff):
                    print(f"  events offset {offset}: {type(exc).__name__} — retry "
                          f"{attempt + 1}/{len(backoff)} in {backoff[attempt]:.0f}s", flush=True)
                    await asyncio.sleep(backoff[attempt])
        if last_exc is not None:
            raise SystemExit(
                f"events discovery FAILED at offset {offset} after {len(backoff) + 1} attempt(s) "
                f"({type(last_exc).__name__}): {last_exc}\nRefusing to screen a partial universe."
            ) from last_exc
        if not isinstance(r, dict) or "events" not in r:
            # An envelope rename must not read as "empty page" — that diagnoses a schema change as
            # a pagination end and sends the operator to raise the offset bound.
            raise SystemExit(
                f"UNRECOGNISED /v1/events shape at offset {offset}: {str(r)[:200]}")
        events = r.get("events", [])
        if not events:
            # ⛔ RE-REQUEST THE SAME OFFSET — do NOT advance past it. An empty 200 from a venue
            # that throttles-to-stale is the same transient class as the timeout handled above, so
            # it deserves the same treatment: ask again. Skipping forward "probes past" the
            # transient by DISCARDING it — one transient empty page then drops a contiguous
            # venue-ordered slice (i.e. a whole family) and still returns complete=True, under the
            # docstring above promising exactly that cannot happen.
            empty_streak += 1
            if empty_streak >= empty_probe:
                # N consecutive empties AT THE SAME OFFSET is the genuine end of the listing.
                # Completeness therefore does NOT require a short page: a universe that is an
                # exact multiple of the page size never produces one, and requiring one makes
                # `complete` UNSATISFIABLE in that case — a hard failure on a complete universe.
                return pairs, offset > 0
            # ⛔ BACK OFF LIKE THE EXCEPTION RETRY DOES — not `pace_s`. The comment above calls an
            # empty 200 "the same transient class" as the timeout, but sleeping the crawl pace
            # gives it a fraction of that retry's tolerance, so a throttle lasting a second walks
            # straight through every probe and returns a TRUNCATED map labelled complete. Before
            # this, the same sequence failed loudly; a fix that turns a loud failure into a silent
            # one is a regression however good the intent.
            await asyncio.sleep(backoff[min(empty_streak - 1, len(backoff) - 1)]
                                if backoff else pace_s)
            continue
        empty_streak = 0
        last_was_short = len(events) < _EVENTS_PAGE
        for e in events:
            for m in (e.get("markets") or []):
                if m.get("slug"):
                    pairs.append((m, e))
        offset += _EVENTS_PAGE
        if last_was_short:
            return pairs, True
        await asyncio.sleep(pace_s)
    return pairs, False


async def fetch_all_events(client: PolyUSClient, pace_s: float = 0.2) -> list[dict]:
    """Discovery through the EVENT listing — the universe the market listing does not show.

    ⛔ WHY BOTH EXIST. `fetch_all` below crawls the market listing. An independent crawl of the
    active-event listing, taken the same minute, enumerates a MATERIALLY DIFFERENT SET: a
    meaningful number of markets appear only in the events crawl, and a smaller number only in the
    market listing (markets whose parent event is filtered out). Neither is a superset. Any
    ranking built on one listing silently inherits its blind spot.

    ⚠️ A cautionary note on how that was first measured. The gap was initially reported as several
    times larger, because the events crawl was compared against POST-GATE pass files — which
    contain only the markets that survived `build_candidate`. The screen's own rejects therefore
    counted as "the listing never served it". The direction held; the magnitude did not. Compare
    like against like: raw listing vs raw listing, same minute.

    Drop-in: an events market carries `slug`, `bestBidQuote`, `bestAskQuote`,
    `orderPriceMinTickSize` and `category` — every field `build_candidate`/`add_depth` read. The
    event's category is used only as a FALLBACK, since the market's own is the finer label.

    ⚠️ OPERATIONALLY ADDITIVE, AND THAT IS THE RISK. New slugs simply begin accumulating their own
    flow series, so no existing estimate breaks. But depth reads are capped by `--top`/`--discover`
    rather than by universe size, so a wider net does NOT buy more measurement — it starves the
    tail further, and the tail is where the unmeasured books live. Widen the budget with the net,
    or accept that coverage now takes more passes; do NOT read the first wider pass as complete.

    ⛔ THE PAGINATION LIVES HERE. Any thin CLI that wants a universe crawl should import it from
    this module, not the other way round: this module already owns venue discovery, and a
    read-only consumer that imports a network-calling module has quietly widened its own import
    graph to reach the network."""
    pairs, complete = await crawl_events_markets(client, pace_s)
    if not complete:
        raise SystemExit(
            "events discovery did NOT terminate cleanly — refusing to screen a partial universe "
            "(the same fail-closed rule as the listing path below)")
    out: list[dict] = []
    for m, e in pairs:
        if not m.get("category"):
            m = {**m, "category": e.get("category", "")}
        out.append(m)
    return out


async def fetch_all(client: PolyUSClient) -> list[dict]:
    out: list[dict] = []
    empty_streak = 0
    for page in range(MAX_PAGES):
        last_exc: Exception | None = None
        resp = None
        for attempt in range(len(_RETRY_BACKOFF_S) + 1):
            try:
                resp = await client._sdk.markets.list(
                    {"active": True, "closed": False, "limit": PAGE, "offset": page * PAGE},
                )
                last_exc = None
                break
            except Exception as exc:  # noqa: BLE001 - retried; the abort below stays fail-closed
                last_exc = exc
                if attempt < len(_RETRY_BACKOFF_S):
                    print(f"  listing offset {page * PAGE}: {type(exc).__name__} — "
                          f"retry {attempt + 1}/{len(_RETRY_BACKOFF_S)} in "
                          f"{_RETRY_BACKOFF_S[attempt]:.0f}s", flush=True)
                    await asyncio.sleep(_RETRY_BACKOFF_S[attempt])
        if last_exc is not None:
            raise SystemExit(
                f"listing FAILED at offset {page * PAGE} after "
                f"{len(_RETRY_BACKOFF_S) + 1} attempts "
                f"({type(last_exc).__name__}): {last_exc}\n"
                f"Refusing to screen a partial universe."
            ) from last_exc
        ms = resp.get("markets") if isinstance(resp, dict) else None
        if ms is None:
            raise SystemExit(f"UNRECOGNISED listing shape at offset {page * PAGE}: {str(resp)[:200]}")
        if not ms:
            # ⛔ PROBE PAST an empty page before believing it is the end. A full sweep once stopped
            # at exactly a round number of candidates while a direct probe minutes later showed
            # full pages beyond that offset and the true end far further out — a single transient
            # empty page mid-pagination reads as end-of-universe and silently truncates the tail.
            # Three consecutive empties = the real end (the genuine end serves them consistently).
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue
        empty_streak = 0
        out.extend(ms)
    else:
        print(f"⚠️  stopped at the {MAX_PAGES}-page bound ({len(out)} markets) — universe may be larger")
    return out


def measured_ever_slugs(out_csv: Optional[str]) -> set[str]:
    """Every slug that has EVER carried a depth reading, across ALL prior screen passes.

    Distinct from `prior_depth_slugs` (newest pass only — the sticky-panel semantics): the
    discovery tier must not re-crawl a book measured days ago, so its exclusion set is the union.
    Same timestamped-name discipline.

    ⛔ ANY depth read counts — do NOT narrow this to `depth_src == "fresh"`. That was tried and
    REVERTED the same day; the reasoning is preserved here because it is persuasive and wrong, and
    someone will propose it again.

    The argument was: a downstream ranker discards a `depth_src=cached` row, so counting one here
    creates a ratchet — a book gets one cached read, is excluded from discovery forever, and the
    ranker throws that reading away. True as far as it goes.

    What it misses: **`run_discovery` calls `add_depth(fresh=False)`, so EVERY row the discovery
    tier writes is `depth_src="cached"`.** Requiring "fresh" therefore gives the exclusion set no
    writer reachable from the tier it governs — the crawl can never advance. Replayed on a real
    archive, the narrowed predicate re-read over 90% of the same books every pass and never
    reached the rest of the lane at all: once the lane exceeds the per-pass cap it walks the same
    slug-sorted prefix forever, for zero coverage gain.

    The two functions are not disagreeing by accident. They answer DIFFERENT questions: the crawl
    is the wide net (has this been READ — the crawl-position marker this set exists to be), the
    panel's fresh read is the confirmation (does it have a FRESH queue — the ranking question).
    One set cannot answer both. If the goal is to get cached-only books a fresh reading, route
    them to the PANEL explicitly; do not express it by breaking the crawl's terminator.

    ⚠️ Also unmeasured: "a cached touch is stale and stale is wrong in the direction the ranking
    selects for" is REASONED, not measured. Measuring fresh-vs-cached touch divergence over a few
    hundred books would settle it and has not been done."""
    return set(depth_coverage(out_csv))


#: The coverage scan reads only passes newer than this — it bounds a scan that otherwise grows
#: without limit as the archive does, and it changes the SEMANTICS deliberately: a book unread for
#: this long reads as never-read again, so it re-enters both the starvation queue and the
#: discovery crawl. A periodic refresh, in the safe direction (more reads), never a starvation.
#: ⚠️ Watch the producer's own timeout margin as the archive grows: `main()` writes the pass CSV
#: LAST, so a pass that times out publishes nothing at all.
COVERAGE_WINDOW_DAYS = 30.0


def depth_coverage(out_csv: Optional[str], *, now: Optional[float] = None) -> dict[str, float]:
    """slug → NEWEST pass ts (epoch) that carried a depth READ for it — the recency map behind
    both `measured_ever_slugs` (its key set) and the panel's starvation-first exploration order.
    Scan bounded to COVERAGE_WINDOW_DAYS (see above). The ts is the pass FILENAME's, which is what
    defines pass identity everywhere else (row `read_ts` is the true read time — a roughly
    constant offset, so the ordering this function feeds is unaffected).

    ⛔ "READ" includes an EMPTY book: `add_depth` stamps `read_ts` and leaves both touch columns
    blank when there is no book to read. Counting only the touch columns leaves every read-but-
    empty book at coverage 0 FOREVER — permanent front-of-queue ghosts that consume the
    exploration budget every pass and also floor the never-read counter, so the acceptance metric
    can never be scored down."""
    out: dict[str, float] = {}
    if not out_csv:
        return out
    folder = os.path.dirname(os.path.abspath(out_csv))
    try:
        names = os.listdir(folder)
    except OSError:
        return out
    cutoff = (time.time() if now is None else now) - COVERAGE_WINDOW_DAYS * 86400.0
    for name in sorted(names):
        m = PASS_NAME_RE.fullmatch(name)
        if not m:
            continue
        try:
            import datetime as _dt
            ts = _dt.datetime.strptime(m.group(1), "%Y%m%dT%H%M").replace(
                tzinfo=_dt.timezone.utc).timestamp()
        except ValueError:
            continue
        if ts < cutoff:
            continue
        try:
            with open(os.path.join(folder, name), newline="") as fh:
                for row in csv.DictReader(fh):
                    if row.get("slug") and (row.get("touch_qty") or row.get("touch_qty_ask")
                                            or row.get("read_ts")):
                        if ts > out.get(row["slug"], 0.0):
                            out[row["slug"]] = ts
                _drop_page_cache(fh)
        except (OSError, KeyError, UnicodeDecodeError, csv.Error):
            continue            # one unreadable pass must not blind the crawl
    return out


def _drop_page_cache(fh) -> None:
    """Tell the kernel we will not re-read this file — releases its page-cache charge. This scan
    streams the whole pass archive several times an hour, and on a small host that cache charge
    dominates the process's measured peak RSS while being entirely reclaimable. Linux-only
    (`posix_fadvise` is absent on macOS); best-effort, byte-identical output either way."""
    try:
        if hasattr(os, "posix_fadvise"):
            os.posix_fadvise(fh.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    except OSError:
        pass


def select_discovery(cands: list["Candidate"], measured: set[str],
                     panel_slugs: set[str], cap: int) -> list["Candidate"]:
    """The discovery tier's read list: UNMEASURED books in the lanes the panel under-serves,
    excluding the panel itself (which reads them fresh anyway). Slug-sorted for a stable crawl —
    the measured set grows each pass, so consecutive passes walk disjoint slices without needing
    any crawl-position state."""
    lane = [c for c in cands
            if c.slug not in measured and c.slug not in panel_slugs
            and ((c.tick == Decimal("0.01")
                  and (c.category or "").lower() != "sports")
                 # Sports FUTURES are eligible at ANY tick: a blanket non-sports requirement
                 # starves them behind a refusal aimed at live single-game books, which are a
                 # different lane with different dynamics.
                 or sports_lane(c.slug, c.category) == "sports_futures")]
    lane.sort(key=lambda c: c.slug)
    return lane[:max(0, cap)]


def _real_maker_live() -> bool:
    """Is a REAL-money maker quoting right now?

    The discovery crawl's cold reads are hundreds of origin MISSes, and origin bursts are what
    trips an edge provider's burst rules. A rate-limit block costs nothing when no maker runs and
    costs a live session its quoting time when one does — so the crawl waits an hour rather than
    share the maker's gateway. A scan FAILURE reads as LIVE: when we cannot tell, we do not burst.

    STALE counts as live too, with one exception. A stale heartbeat is either a dead process
    (crawling is safe) or a live maker whose heartbeat WRITER broke — and a heartbeat writer that
    swallows its own failures by design makes that ambiguous. ⛔ But a stale record whose PID is
    CONFIRMED GONE is NOT live, and treating it as live is not a one-time cost: the record persists
    until deleted, so one hard-killed maker suppresses the crawl FOREVER rather than for one
    staleness window. The deadman check already resolves exactly that ambiguity; consult it.

        pid GONE    = dead      → safe to crawl
        pid ALIVE but not beating = hung → still treat as live
        pid UNKNOWN = still live
    """
    try:
        from bot.core import heartbeat
        for d in heartbeat.scan():
            if (d.fields or {}).get("mode") != "real":
                continue
            if d.state == "ok":
                return True
            if d.state == "stale" and getattr(d, "pid_running", None) is not False:
                return True
        return False
    except Exception:
        return True


async def run_discovery(client: PolyUSClient, cands: list["Candidate"],
                        panel_slugs: set[str], out_csv: Optional[str], cap: int,
                        pace_s: float = DEPTH_PACE_S,
                        measured: Optional[set[str]] = None) -> int:
    """The discovery tier, as one seam so its UN-NONCED read mode is pinnable — the panel's fresh
    reads and this tier's cached reads must never swap (a cache-busted several-hundred-book crawl
    is the origin-burst pattern; a cached panel feeds decisions stale data)."""
    if _real_maker_live():
        print("discovery tier: SKIPPED — a real-money maker is live, and the crawl's cold "
              "origin reads wait an hour rather than share its gateway.", flush=True)
        return 0
    if measured is None:
        measured = measured_ever_slugs(out_csv)
    discovery = select_discovery(cands, measured, panel_slugs, cap)
    if discovery:
        print(f"discovery tier: {len(discovery)} never-measured book(s), "
              f"un-nonced reads…", flush=True)
        await add_depth(client, discovery, fresh=False, pace_s=pace_s)
    return len(discovery)


def _ask_touch(md: dict) -> Optional[Decimal]:
    """Qty resting AT the best ask, `touch_from_md`'s discipline mirrored for the other side:
    min over offer prices (the list order is undocumented), SUM of qty at that price, Decimal
    from the wire strings, and None — never a guess — when the side is empty or unparseable.
    Lives here rather than widening `touch_from_md` because that helper rides the placement
    guard's money path and its 3-tuple shape is pinned by callers."""
    try:
        offers = md.get("offers")
        if not isinstance(offers, list) or not offers:
            return None
        levels: list[tuple[Decimal, Decimal]] = []
        for lv in offers:
            px = lv.get("px")
            if isinstance(px, dict):
                px = px.get("value")
            levels.append((Decimal(str(px)), Decimal(str(lv.get("qty")))))
        best = min(px for px, _ in levels)
        return sum(q for px, q in levels if px == best)
    except Exception:  # noqa: BLE001 — a screen column fails to None, never to a crash
        return None


def _refresh_price(c: Candidate, bid: Optional[Decimal], ask: Optional[Decimal]) -> None:
    """Re-derive a candidate's PRICE columns from the touch we just read.

    Sets `price_src = "fresh"` and rewrites `bid`/`ask`/`mid`/`spread_ticks`/`rebate_usd`/
    `min_size`/`improve_cost_frac` — every field `build_candidate` derives from price — so a row's
    price and its `read_ts` describe the same observation.

    ⛔ REFUSES A CROSSED OR ONE-SIDED TOUCH, leaving the listing values and `price_src =
    "listing"`. `build_candidate` rejects crossed books at admission because a crossed quote is
    not tradeable as read; accepting one HERE would put the same phantom-cheap side into a row
    that already passed admission. A momentarily-crossed book is a real thing and it must not
    become a cheap price.

    ⛔ It does NOT re-gate. A book whose spread widened past the screen's ceiling since the listing
    crawl keeps its row and carries the true, wider `spread_ticks` — because a row that disappears
    cannot print a gate name, and the file's own promise is that absence from the ranking never
    means dead. Downstream width gates see the honest number instead.

    ⛔ It does NOT touch the panel. `select_depth_panel` runs on `build_candidate` output before
    any read, `prior_depth_slugs` carries SLUGS not prices, and the next pass rebuilds every
    candidate from its own listing crawl — so a refreshed price cannot reach band-based selection
    in this pass or the next.
    """
    if bid is None or ask is None or ask <= bid or bid <= 0:
        c.price_src = "listing"
        return
    c.bid, c.ask = bid, ask
    c.mid = (bid + ask) / Decimal(2)
    if c.tick and c.tick > 0:
        c.spread_ticks = int(((ask - bid) / c.tick).to_integral_value(rounding=ROUND_HALF_UP))
        per_contract = REBATE_COEF * c.mid * (Decimal(1) - c.mid)
        c.improve_cost_frac = (c.tick / per_contract) if per_contract > 0 else None
    # `rebate_usd`/`min_size` are SIZE-dependent, and the size is whatever this producer screened
    # at. Left alone when it is unknown rather than recomputed at a guessed size — a rebate
    # stamped at the wrong size is the defect `screen_size` exists to prevent.
    if c.screen_size:
        c.rebate_usd = rebate_cents(c.mid, c.screen_size)
        c.min_size = min_clearing_size(c.mid)
    c.price_src = "fresh"


def write_pass_rows(fh, cands: list[Candidate]) -> None:
    """Write a pass: header from the DATACLASS, one row per candidate.

    ⛔ FIELDNAMES ARE DERIVED, NEVER LISTED. Every column this schema has gained appeared in the
    CSV with no writer change precisely because the header comes from `asdict()`. A hardcoded list
    is how a column ships DEAD: the producer stamps it, the consumer reads it by name, both halves
    are green under test, and the column is simply never written. That mutant survives a whole
    suite — it only became testable once this function was extracted from `main()`.
    """
    w = csv.DictWriter(fh, fieldnames=list(asdict(cands[0]).keys()))
    w.writeheader()
    for c in cands:
        w.writerow(asdict(c))


async def add_depth(client: PolyUSClient, cands: list[Candidate], *,
                    fresh: bool = True, pace_s: float = DEPTH_PACE_S) -> None:
    """Stage 2: fill in touch depth and FLOW. One book read each.

    `fresh=True` = the PANEL: cache-busted origin reads, decisions read these.
    `fresh=False` = the DISCOVERY tier: un-nonced reads, up to the CDN's max-age stale — fine for
    coverage screening. ⚠️ A first-touch cold book MISSES to the origin either way, so the
    politeness argument is the PACE, never the caching; un-nonced is simply the politer traffic
    shape. `depth_src` records the tier per row.

    Flow has to come from here because the venue gives no other route: the market listing accepts
    a minimum-volume parameter and SILENTLY IGNORES IT (measured: an absurdly large value returns
    the entire active universe, byte-identical to a zero value), and the listing carries no volume
    field at all. A screen that trusted that parameter would believe it had filtered to liquid
    markets while looking at everything.
    """
    for c in cands:
        # Paced: an unpaced burst of origin reads next to a live maker on the same IP is exactly
        # the shape that trips an edge provider's burst protection.
        await asyncio.sleep(pace_s)
        try:
            book = await client._fetch_book(c.slug, fresh=fresh)
        except Exception as exc:  # noqa: BLE001
            print(f"   depth read failed for {c.slug}: {type(exc).__name__}")
            continue
        # ⚠️ Both parsers take `marketData`, NOT the raw book response. Passing the book straight in
        # does not raise — every field simply parses to None — so the depth and flow columns come
        # back empty and the screen looks like it ran. It has to be unwrapped.
        md = book.get("marketData") if isinstance(book, dict) else None
        if md is None:
            print(f"   no marketData for {c.slug} (book keys "
                  f"{sorted(book) if isinstance(book, dict) else type(book).__name__})")
            continue
        try:
            # THE touch parser — a 3-tuple (best_bid, best_ask, qty AT best_bid). Unpack it rather
            # than index: an earlier version guarded `len(touch) >= 4` and read touch[1], which is
            # the ASK. The guard could never fire on a 3-tuple, so the column silently stayed empty.
            _bid, _ask, qty_at_bid = touch_from_md(md)
        except Exception as exc:  # noqa: BLE001
            print(f"   touch parse failed for {c.slug}: {type(exc).__name__}")
            continue
        c.touch_qty = qty_at_bid
        # Ask side from the SAME payload — zero extra requests.
        c.touch_qty_ask = _ask_touch(md)
        # ⛔ REFRESH THE PRICE FROM THE READ WE JUST DID. `_bid`/`_ask` were parsed above and
        # previously thrown away, so `bid`/`ask`/`mid`/`spread_ticks` stayed at whatever the
        # LISTING CRAWL saw at pass start — while `read_ts` beside them said "age ≈ 0". On a book
        # read late in a long sweep that price is an hour old, and the rebate floor is evaluated
        # on exactly those columns; enough listing quotes move over that span to flip a book's
        # verdict.
        #
        # Only when the fresh touch is TWO-SIDED AND UNCROSSED. An empty or crossed book keeps the
        # listing price and says so via `price_src` — writing a crossed price would manufacture
        # exactly the phantom-cheap side `build_candidate` refuses at admission.
        _refresh_price(c, _bid, _ask)
        # Stamped only once a touch was actually parsed — a read that returned no marketData
        # is not a measurement and must not claim a tier.
        c.depth_src = "fresh" if fresh else "cached"
        # ⛔ THE READ TIME, on the same condition as `depth_src`: a row that failed to parse a
        # touch gets neither. Stamped HERE and not at pass start — see `Candidate.read_ts` for why
        # a filename stamp makes every row read as an hour old the instant it is written.
        read_at = time.time()
        c.read_ts = read_at

        stats = _parse_book_stats(md, read_at)
        c.shares_traded = stats.get("shares_traded")
        c.last_trade_age_s = stats.get("last_trade_age_s")
        c.open_interest = stats.get("open_interest")


def build_screen_parser() -> argparse.ArgumentParser:
    """Extracted from main() so the flag DEFAULTS can be pinned without running it — main() opens
    a venue client on its second line, so a test that drives it cannot assert on argument
    defaults, and a source-text assertion is not a pin."""
    ap = argparse.ArgumentParser(
        description="Two-stage market screener: stage 1 gates the whole universe on structural "
                    "validity for free (price, tick, spread, rebate-clearance at --size); stage 2 "
                    "spends one paced book read each on the survivors to measure touch depth and "
                    "flow. Output is a ranked candidate set for the operator to select from — it "
                    "RANKS, it does not authorize.")
    ap.add_argument("--size", type=int, default=5,
                    help="maker order size to screen for. Decides which markets EXIST downstream: "
                         "the rebate floor is monotonic in size, so a pass is a superset of any "
                         "smaller size and blind above it")
    ap.add_argument("--top", type=int, default=25,
                    help="stage-2 budget: how many books get a depth read per pass")
    # ⛔ A SEPARATE FILE, NOT EXTRA ROWS IN THE MAIN CSV. Emitting rejected books into the pass
    # itself was measured and is the wrong trade: it inflates the pass CSV by a fifth, and the pass
    # CSVs are what the ranking path globs and holds in memory — so a reject roster inside them
    # buys a modest recovery of books another producer already covers at the cost of the ranking
    # path's memory budget. A separately-named file matches no pass glob, so it costs the ranking
    # path NOTHING, and it gives the reject histogram a durable home instead of the console.
    ap.add_argument("--reject-csv", default=None,
                    help="write one row per REJECTED market (slug, gate, prices, and the smallest "
                         "size at which it WOULD clear). Separate file by design: it must not "
                         "inflate the pass CSV that the ranking path loads.")
    # ⛔ The spread ceiling is a MEASUREMENT gate, and it was long set far too tight — rejecting
    # more of the universe than every other gate combined, on an anecdote about two pathological
    # books hundreds of ticks wide. A real slice of wide books does trade. Widening it costs zero
    # venue reads (it filters rows already fetched; the depth panel is still --top).
    #
    # ⚠️ Widening what is MEASURABLE is not widening what is LAUNCHABLE — keep a tighter ceiling
    # on the launch side, and keep every producer's ceiling identical, or a per-slug statistic
    # pooled across producers depends on which producer happened to run.
    ap.add_argument("--max-spread-ticks", type=int, default=60,
                    help="reject books wider than this (default 60). Widening this widens what "
                         "gets MEASURED; keep the launch-side gates on their own tighter ceiling")
    ap.add_argument("--depth", action="store_true",
                    help="run stage 2: fetch touch depth for the shortlist (one book read each)")
    ap.add_argument("--discover", type=int, default=300,
                    help="additionally depth-read up to N never-measured books per pass in the "
                         "lanes the panel under-serves (un-nonced, same pace; 0 disables). "
                         "Without a dedicated budget the coverage crawl competes with the panel "
                         "for read slots and takes weeks to cover a lane a dedicated budget "
                         "covers in a few passes")
    ap.add_argument("--csv", default=None,
                    help="write the pass CSV here. Name it screen_YYYYmmddTHHMM.csv: pass "
                         "identity, the sticky panel and the coverage window all key off it")
    ap.add_argument("--depth-pace-s", type=float, default=DEPTH_PACE_S,
                    help=f"seconds between stage-2 book reads (default {DEPTH_PACE_S}). This — "
                         f"not the venue — is what caps how much of the universe is ever "
                         f"rankable, since ranking needs a MEASURED queue. ⚠️ Raise it only from "
                         f"a machine and key no live maker is using: an edge provider may sit in "
                         f"front of the documented app limit with opaque burst rules. Step down "
                         f"gradually (1.0 → 0.5 → 0.3 → …) watching for 429s and challenge pages.")
    ap.add_argument("--discovery", choices=("events", "markets"), default="events",
                    help="where the universe comes from. `events` (default) crawls the "
                         "active-event listing, which enumerates a materially different set from "
                         "the market listing taken the same minute — neither is a superset. "
                         "`markets` is the legacy market-listing path, kept so a pass can be "
                         "reproduced against the OLD universe when comparing tapes across the "
                         "cutover: the two populations are not interchangeable, and a flow series "
                         "spanning both must say which it used (see the `discovery` column). "
                         "The default was HELD at `markets` until the events crawl gained the "
                         "market listing's two incident-derived properties — per-page RETRY and "
                         "PROBE-PAST-A-LONE-EMPTY-PAGE. A wider net is not an upgrade until it is "
                         "at least as hard to silently truncate.")
    return ap


REJECT_COLUMNS = ["slug", "category", "why", "bid", "ask", "mid", "tick", "spread_ticks",
                  "screen_size", "min_size", "min_size_worse_touch"]


def _min_size_worse_touch(bid: Optional[Decimal], ask: Optional[Decimal]) -> str:
    """The seating question's version of `min_size`: the smallest size clearing at the WORSE of
    the two touch prices, because fills happen at OUR QUOTE rather than at the mid.

    Crediting the mid on a permission gate is a real error, not a rounding one — on wide books the
    mid-derived answer understates the required size by a large multiple. `max` of the two
    per-side answers, since the worse price needs the LARGER size.
    """
    if bid is None or ask is None:
        return ""
    sizes = [min_clearing_size(p) for p in (bid, ask)]
    if any(s is None for s in sizes):
        return ""          # one side cannot clear at any size ≤ cap — "" is honest, 500 is not
    return str(max(sizes))


def _reject_row(m: dict, why: str, size: int) -> list[str]:
    """One rejected market, with `min_size` — the smallest size clearing THIS SCREEN'S floor.

    ⚠️ NOT "the smallest size at which it would pay". `min_clearing_size` is the inverse of
    `clears_floor(mid, size)`, i.e. of THIS gate, evaluated at the MID — and a seating gate should
    credit the WORSE of the two touch prices, because fills happen at our quote, not at the mid.
    The gap between the two is negligible on tight books and a large multiple on wide ones, which
    is why both columns are emitted: `min_size` (this screen's answer, kept for continuity with
    `Candidate.min_size`) and `min_size_worse_touch` (the seating answer).

    ⛔ AND NEITHER LICENSES A RESIZE. The rebate's cent-rounding step makes clearance NON-MONOTONE
    across a budget screen that sums several per-book terms — a book can pass at one size and fail
    at both a smaller and a larger one — so "re-size a book until it clears its screen" is
    forbidden. The monotonicity that makes `min_clearing_size` well-defined is a property of
    `clears_floor` ALONE. These columns say "this book was never eligible at the size we happened
    to pass"; they do not say "quote it bigger".
    """

    bid, ask = _dec(m.get("bestBidQuote")), _dec(m.get("bestAskQuote"))
    tick = _dec(m.get("orderPriceMinTickSize"))
    mid = (bid + ask) / Decimal(2) if (bid is not None and ask is not None) else None
    spread_ticks = ""
    if bid is not None and ask is not None and tick and tick > 0:
        spread_ticks = str(int(((ask - bid) / tick).to_integral_value(rounding=ROUND_HALF_UP)))
    return [str(m.get("slug") or ""), str(m.get("category") or ""), why,
            "" if bid is None else str(bid), "" if ask is None else str(ask),
            "" if mid is None else str(mid), "" if tick is None else str(tick),
            spread_ticks, str(size),
            "" if mid is None else str(min_clearing_size(mid) or ""),
            _min_size_worse_touch(bid, ask)]


def _refuse_pass_directory(path: str, what: str) -> None:
    """⛔ Refuse to write a NON-PASS file where a pass file lives, or under a pass-like name.

    A reject roster carries `slug` and `category` columns, so anything that consumes passes by
    globbing a directory — rather than by matching the pass-name pattern — will happily read it as
    a pass and union thousands of REJECTED slugs into the live candidate set. A `<pass>_rejects`
    style name is worse still, since it matches the pattern's suffix and can become a slug's
    "newest row" with no touch and no trade age, firing every unmeasured-data gate map-wide.

    The general rule: a directory whose contents are consumed by a bare glob has a SCHEMA, and
    writing anything else into it is a silent data-poisoning bug. Fail closed rather than document
    a footgun.
    """
    resolved = os.path.abspath(path)
    directory = os.path.dirname(resolved) or "."
    if PASS_NAME_RE.search(os.path.basename(resolved)):
        raise SystemExit(
            f"REFUSING to write {what} to {path}: the name matches the screen-pass pattern and "
            f"would be loaded as a pass.")
    try:
        siblings = os.listdir(directory)
    except OSError:
        return
    if any(PASS_NAME_RE.fullmatch(n) for n in siblings):
        raise SystemExit(
            f"REFUSING to write {what} to {path}: {directory} holds screen passes and is globbed "
            f"bare by pass consumers. Write it to a separate directory.")


def write_reject_roster(path: str, rows: list[list[str]]) -> None:
    """Write the reject roster, refusing any location a pass consumer would read (see above)."""
    _refuse_pass_directory(path, "the reject roster")
    resolved = os.path.abspath(path)
    os.makedirs(os.path.dirname(resolved) or ".", exist_ok=True)
    with open(resolved, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(REJECT_COLUMNS)
        w.writerows(rows)


_UNIVERSE_COUNT_GATES = ("no_two_sided_quote", "crossed_or_locked", "no_tick",
                         "rebate_rounds_to_zero", "spread_too_wide")


def append_universe_count(reject_csv: str, universe: int, surviving: int,
                          rejects: dict[str, int], *, now: float | None = None) -> str:
    """One cumulative row per pass beside the reject roster — the UNIVERSE DENOMINATOR, which the
    pipeline could not otherwise produce: the crawl listing is discarded and the roster is
    overwritten each pass, so "how many books existed" survived only inside that hour's console
    output. Counts, not names — names for the LATEST pass stay reunion-able from the pass CSV plus
    the roster; this file makes the denominator itself durable at a few dozen bytes per pass.

    Applies the SAME location refusal as the roster writer ITSELF — "the caller happened to call
    `write_reject_roster` first" is a property of the caller, not of this function."""
    path = os.path.join(os.path.dirname(os.path.abspath(reject_csv)), "universe_counts.csv")
    _refuse_pass_directory(path, "the universe-count file")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["ts", "universe", "surviving", *_UNIVERSE_COUNT_GATES])
        w.writerow([f"{time.time() if now is None else now:.0f}", universe, surviving,
                    *[rejects.get(g, 0) for g in _UNIVERSE_COUNT_GATES]])
    return path


async def main() -> None:
    args = build_screen_parser().parse_args()

    client = PolyUSClient()
    markets = (await fetch_all_events(client) if args.discovery == "events"
               else await fetch_all(client))
    print(f"universe: {len(markets)} active markets (discovery={args.discovery})\n")

    cands: list[Candidate] = []
    rejects: dict[str, int] = {}
    rejected_rows: list[list[str]] = []
    for m in markets:
        c, why = build_candidate(m, args.size, args.max_spread_ticks)
        if c is None:
            rejects[why] = rejects.get(why, 0) + 1
            if args.reject_csv:
                rejected_rows.append(_reject_row(m, why, args.size))
        else:
            cands.append(c)

    print(f"{'gate':26s} {'dropped':>8s}")
    for why, n in sorted(rejects.items(), key=lambda kv: -kv[1]):
        print(f"{why:26s} {n:>8d}")
    print(f"{'SURVIVING':26s} {len(cands):>8d}   ({100*len(cands)/max(1,len(markets)):.1f}% of universe)\n")

    if args.reject_csv:
        write_reject_roster(args.reject_csv, rejected_rows)
        counts = append_universe_count(args.reject_csv, len(markets), len(cands), rejects)
        print(f"reject roster → {args.reject_csv} ({len(rejected_rows)} row(s)); "
              f"denominator appended → {counts}\n")

    if not cands:
        print(f"Nothing clears the rebate floor at size {args.size}.")
        smallest = min((c for c in (min_clearing_size(_dec(m.get('bestBidQuote')) or Decimal(1))
                                    for m in markets) if c), default=None)
        if smallest:
            print(f"Smallest size that would clear anywhere: {smallest}")
        return

    for c in cands:
        c.screen_size = args.size
        c.discovery = args.discovery
    cands.sort(key=lambda c: c.score(), reverse=True)
    if args.depth:
        # The panel is NOT the score() top-N — see select_depth_panel for why that starves a whole
        # tick class of measurement forever.
        sticky = prior_depth_slugs(args.csv)
        # One archive scan serves both consumers: the discovery tier's exclusion set is this map's
        # key set, and the panel's starvation-first order is its values.
        coverage = depth_coverage(args.csv)
        never_read = sum(1 for c in cands if c.slug not in coverage)
        print(f"depth coverage (last {COVERAGE_WINDOW_DAYS:.0f}d): {len(coverage)} book(s) "
              f"read; {never_read} of {len(cands)} current candidate(s) unread in-window "
              f"(read-but-EMPTY books count as read)")
        shortlist = select_depth_panel(cands, args.top, sticky, coverage=coverage)
        n_sticky = len(sticky & {c.slug for c in shortlist})
        print(f"reading books for {len(shortlist)} panel member(s) "
              f"({n_sticky} sticky from the previous pass)…\n")
        await add_depth(client, shortlist, pace_s=args.depth_pace_s)
        if args.discover > 0:
            await run_discovery(client, cands, {c.slug for c in shortlist},
                                args.csv, args.discover, pace_s=args.depth_pace_s,
                                measured=set(coverage))
    else:
        shortlist = cands[: args.top]

    if args.depth:
        # Re-rank the shortlist now that flow is known: an untraded book pays nothing regardless of
        # how good its rebate arithmetic looks, so a market with no recent trade sorts to the back.
        def flow_key(c: Candidate) -> tuple:
            traded = c.shares_traded or 0.0
            fresh = c.last_trade_age_s is not None and c.last_trade_age_s < 3600
            return (fresh, traded, c.score())
        shortlist.sort(key=flow_key, reverse=True)

    hdr = f"{'slug':42s} {'mid':>6s} {'sprd':>5s} {'reb$':>6s} {'tick%reb':>9s} {'minsz':>6s}"
    if args.depth:
        hdr += f" {'touch':>8s} {'traded':>9s} {'lastTrd':>8s}"
    print(hdr)
    for c in shortlist:
        line = (f"{c.slug[:42]:42s} {c.mid:>6.3f} {c.spread_ticks:>4d}t {c.rebate_usd:>6} "
                f"{(c.improve_cost_frac or 0) * 100:>8.0f}% {c.min_size or '-':>6}")
        if args.depth:
            age = ("-" if c.last_trade_age_s is None
                   else f"{c.last_trade_age_s / 60:.0f}m" if c.last_trade_age_s < 86400 else ">1d")
            line += (f" {c.touch_qty if c.touch_qty is not None else '-':>8}"
                     f" {c.shares_traded if c.shares_traded is not None else '-':>9}"
                     f" {age:>8}")
        print(line)

    print(f"\n`tick%reb` = what one tick of price improvement costs as a share of the rebate.")
    print(f"Over 100% means improving is worse than not filling at all at that price.")
    # In the OUTPUT, not just the module docstring: a rank is not a permission, and a screen that
    # only says so in its source will have its top row seated by someone who never read it.
    print(f"⚠️  This screen RANKS, it does not AUTHORIZE — selection is yours.")
    if args.depth:
        print(f"`touch` = size resting at the best bid — the queue a joined order sits behind.")
    else:
        print(f"⚠️  No flow data: this ranking is rebate arithmetic ONLY and cannot tell a busy")
        print(f"   book from an abandoned one. Re-run with --depth before quoting anything.")

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            write_pass_rows(fh, cands)
        print(f"\nwrote {len(cands)} rows -> {args.csv}")


if __name__ == "__main__":
    asyncio.run(main())
