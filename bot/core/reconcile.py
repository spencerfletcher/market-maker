"""Venue-vs-tracker position reconciliation — the "am I stranded without knowing?" guard.

Moved here from `bot/runner/reconcile.py` 2026-09-04 when the cross-arb bot was removed
(archive branch `archive/arb-bot-2026-09-04`). `ReconcileMixin` was the arb runner's loop and now
has no live mixer; the position-parsing helpers below are what the Poly probes import. The
cannot-verify-is-not-flat principle stated here is cited across the maker code and stands.

WHY THIS EXISTS
───────────────
`PositionTracker` is a LOCAL BELIEF, assembled purely from order responses. Everything keys off
it: the strand global-pause, the strand alert, the exposure caps. Nothing ever asked either venue
what we ACTUALLY hold (`KalshiClient.get_positions()` existed but had ZERO callers; Poly's
`portfolio.positions()` was never called). So when the belief is wrong we are stranded and BLIND —
`_strand_control_loop` reports all-clear forever, because it only inspects `self.tracker`.

The belief can be wrong. A timeout, a 502, or a connection reset AFTER the engine filled leaves us
holding a real position we never recorded — as does a crash between the fill and `add_position`.
Low probability per order; unbounded and SILENT when it happens.

Both order clients now RAISE on that ambiguity instead of returning None (2026-07-16), and the exec
helpers catch it: when a leg we KNOW we hold is at risk they strand it and pause immediately, and
when nothing else fired they log CRITICAL and stop rather than guess a quantity. So the ambiguous
order is no longer silent at the source. This loop remains the only thing that resolves it — the
crash-between-fill-and-record case has no source-side handler at all, and the first-leg case
deliberately records nothing, which is precisely a bet on this loop working. ⚠️ Its KALSHI half did
not, from the day it shipped until 2026-07-16: `get_positions` read a key the venue does not send
and returned `[]` — "confirmed flat" — forever.

WHAT IT CHECKS (v1 — deliberately one direction)
────────────────────────────────────────────────
Only **venue > known** — "we hold MORE than we know about" = unknown exposure. That is the failure
this exists to catch.

The other direction (venue < known: a position we think we hold but the venue doesn't) is logged,
NOT paused: it is dominated by ordinary settlement (games settle constantly and the tracker keeps
its rows), so pauseing on it would be a false-positive storm. It IS a real signal — a hedge that
isn't there means the opposite leg is naked — but it needs settlement-awareness to read, which is
v2. See the private design notes.

THE WHITELIST IS NOT AN IGNORE-LIST
───────────────────────────────────
An "ignore market X" list would hide a GENUINE future strand on X — it silences the alarm. Instead
each entry acknowledges an EXACT (venue, market, qty): "this is also mine and I know about it". So
the whitelist ADDS to what we know we hold, keeping the invariant intact (venue == everything I
know about). If an acknowledged position later CHANGES — 255 becomes 300 — it is no longer what was
acknowledged, and the delta alerts. You only ever lose sight of exactly what you signed off on.

FAIL DIRECTION
──────────────
A read/parse failure is "CANNOT VERIFY", never "we're flat" (the blank≠zero rule, project-wide).
Cannot-verify warns and alerts; it does NOT pause, because we have no evidence of a position and a
transient API error must not halt trading. A DIVERGENCE pauses (via add_stranded → the proven alert
+ _strand_control_loop + logs/resume flow) but only after RECONCILE_CONFIRM_POLLS consecutive
sightings — Kalshi's portfolio GET is eventually consistent and can 404 right after a create.

It NEVER trades. Detect and stop; unwinding a discrepancy is an operator decision.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass

import math

from decimal import Decimal

from bot.core import config
from bot.core.money import CENTICENT, from_float, is_zero
from bot.core.alerts import send_reconcile_alert
from bot.core.logger import get_logger
from bot.core.redact import safe_exc
from bot.poly_us.sides import parse_token

log = get_logger(__name__)


@dataclass(frozen=True)
class VenuePosition:
    """A position the VENUE says we hold. `market` is the venue's own id (Kalshi ticker /
    Poly market slug); `qty` is absolute size (side is irrelevant to "do we hold something")."""
    venue: str
    market: str
    qty: float


@dataclass(frozen=True)
class Divergence:
    """The venue holds `venue_qty` of `market`; we only know about `known_qty`."""
    venue: str
    market: str
    venue_qty: float
    known_qty: float

    @property
    def unknown(self) -> float:
        return self.venue_qty - self.known_qty


_EPS = 0.5   # sub-share noise; venue qtys are whole contracts



# A venue position below wire resolution is NOT a position. Both venues report holdings to 2 dp
# ("position_fp": "-8.00", Poly netPosition likewise), so anything under 1e-4 is residue, not size.
#
# WHY THIS MATTERS HERE SPECIFICALLY: a truthy `qty > 0` makes this module emit a VenuePosition,
# which `find_divergences` escalates through `add_stranded` — a GLOBAL PAUSE of all trading that
# needs a manual `logs/resume` to clear. So a single dust value halts the bot. This is the same
# compare-to-zero class as the crossed-book bug (a ~1e-13 residue read as real depth), and the
# fractional `position_fp` values observed on the 2026-07-19 live MM run (-4.65, -2.17 — still
# unexplained) are direct evidence this input is not always clean.
#
# Direction: fail-SAFE either way. Dust reads as flat (no false halt); anything at or above wire
# resolution still reports, so a real position can never be missed — which is this module's whole
# purpose.
_POSITION_STEP = CENTICENT   # same value as the fee grid, kept named for its own meaning


def _is_real_position(qty: float) -> bool:
    """True if `qty` is a genuine holding at venue resolution (dust-safe replacement for qty > 0).

    A NON-FINITE qty reports True — this module's stated principle is "fail toward alerting, never
    silently drop a position", so an uninterpretable holding must surface as a divergence for an
    operator rather than vanish. (It also avoids from_float(NaN), which raises InvalidOperation.)"""
    if not math.isfinite(qty):
        return True
    return not is_zero(from_float(qty), _POSITION_STEP)


# Canonical home is bot.core.qty_parse (moved 2026-08-13, public-repo decoupling); re-exported
# here so this module's callers and tests are unchanged.
from bot.core.qty_parse import _first_qty  # noqa: E402  (re-export)


def poly_slug(token: str) -> str:
    """Tracker Poly tokens are `<slug>` or `<slug>::short`; the venue reports per-SLUG (the short
    side is a sign on netPosition, not a separate market). Normalize to the slug so both tracker
    rows for a game aggregate against the one venue row."""
    return parse_token(token)[0]


def load_whitelist(path: str) -> dict[tuple[str, str], float]:
    """(venue, market) → acknowledged qty. Missing/……corrupt file → {} (an empty whitelist is the
    SAFE default: it acknowledges nothing, so everything still alerts)."""
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as e:
        log.error(f"reconcile: whitelist {path} unreadable ({e!r}) — treating as EMPTY "
                  f"(nothing acknowledged; everything will alert). Fix the file.")
        return {}
    out: dict[tuple[str, str], float] = {}
    for e in (raw.get("entries") or []):
        try:
            out[(str(e["venue"]), str(e["market"]))] = abs(float(e["qty"]))
        except (KeyError, TypeError, ValueError):
            log.error(f"reconcile: skipping malformed whitelist entry {e!r}")
    return out


def save_whitelist(path: str, entries: dict[tuple[str, str], float], note: str = "") -> None:
    """Rewrite the whitelist. Keeps a human-readable note + timestamp per entry."""
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = {
        "_comment": "Operator-ACKNOWLEDGED positions: 'this is also mine and I know about it'. "
                    "NOT an ignore-list — qty is exact, so if the position CHANGES it alerts "
                    "again. Delete an entry to un-acknowledge.",
        "entries": [
            {"venue": v, "market": m, "qty": q, "note": note, "acknowledged_at": stamp}
            for (v, m), q in sorted(entries.items())
        ],
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


RECON_PREFIX = "reconcile::"


def recon_token(venue: str, market: str) -> str:
    """Synthetic token_id for a strand row WE raise.

    MUST NOT collide with a real Poly token / Kalshi ticker: PositionTracker.add_stranded does
    `self._positions[token_id] = OpenPosition(...)` — a plain dict OVERWRITE. Keying a synthetic
    row by the real market id would DESTROY a live hedged position row (losing its cost basis and
    corrupting the exposure caps). Namespacing makes that impossible."""
    return f"{RECON_PREFIX}{venue}::{market}"


def tracker_keys(token_id: str) -> list[tuple[str, str]]:
    """(venue, market) keys a tracker row credits toward `known`.

    A real row's token_id is a Poly token OR a Kalshi ticker, and we cannot tell which from the
    token alone — so credit BOTH venues. Over-crediting is the SAFE direction for a venue>known
    check: it can only suppress a false alarm, never hide a venue position that exceeds everything
    we know about. A synthetic `reconcile::` row credits only its own (venue, market), so once a
    divergence is RECORDED it stops re-escalating every poll — while its strand keeps the pause."""
    if token_id.startswith(RECON_PREFIX):
        parts = token_id.split("::", 2)
        return [(parts[1], parts[2])] if len(parts) == 3 else []
    return [("kalshi", token_id), ("poly", poly_slug(token_id))]


def known_quantities(tracker_positions, whitelist: dict[tuple[str, str], float]) -> dict:
    """(venue, market) → qty we KNOW we hold = tracker belief + operator acknowledgements."""
    known: dict[tuple[str, str], float] = {}
    for p in tracker_positions:
        for key in tracker_keys(p.token_id):
            known[key] = known.get(key, 0.0) + abs(float(p.shares))
    for key, qty in whitelist.items():
        known[key] = known.get(key, 0.0) + qty
    return known


def find_divergences(venue_positions, known: dict[tuple[str, str], float]) -> list[Divergence]:
    """Venue positions exceeding what we know about. ONE direction only (venue > known) — see
    the module docstring on why venue < known is logged, not paused."""
    out: list[Divergence] = []
    for vp in venue_positions:
        k = known.get((vp.venue, vp.market), 0.0)
        if vp.qty > k + _EPS:
            out.append(Divergence(vp.venue, vp.market, vp.qty, k))
    return out


class ReconcileMixin:
    """Periodic venue-vs-tracker reconciliation. Mixed into BotRunner."""

    async def _fetch_kalshi_positions(self) -> list[VenuePosition] | None:
        """Kalshi's real open positions, or None = CANNOT VERIFY (never [] on failure — an empty
        list means 'confirmed flat' and would mask exactly what we're hunting)."""
        if not config.KALSHI_API_KEY:
            return []
        try:
            raw = await self.kalshi_client.get_positions()
        except Exception as e:
            log.warning(f"reconcile: Kalshi positions read failed: {safe_exc(e)} — CANNOT VERIFY")
            return None
        if not isinstance(raw, list):
            log.error(f"reconcile: Kalshi positions shape unexpected ({type(raw).__name__}) "
                      f"— CANNOT VERIFY")
            return None
        out: list[VenuePosition] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            ticker = item.get("ticker") or item.get("market_ticker")
            qty = _first_qty(item, "position_fp", "position")
            if ticker is None or qty is None:
                log.error(f"reconcile: unparseable Kalshi position {item!r} — CANNOT VERIFY")
                return None          # fail toward alerting, never silently drop a position
            if _is_real_position(qty):
                out.append(VenuePosition("kalshi", str(ticker), qty))
        return out

    async def _fetch_poly_positions(self) -> list[VenuePosition] | None:
        """Poly's real open positions, or None = CANNOT VERIFY.

        Paginates to `eof` — the response is {positions: {slug: {...}}, nextCursor, eof} and
        stopping at page 1 would MISS positions, i.e. reproduce the bug this guards against."""
        if not getattr(self, "_poly_us", False):
            return []
        out: list[VenuePosition] = []
        cursor, pages = "", 0
        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await self.client._sdk.portfolio.positions(params)
            except Exception as e:
                log.warning(f"reconcile: Poly positions read failed: {safe_exc(e)} — CANNOT VERIFY")
                return None
            if not isinstance(resp, dict):
                log.error(f"reconcile: Poly positions shape unexpected — CANNOT VERIFY")
                return None
            positions = resp.get("positions")
            if not isinstance(positions, dict):
                log.error(f"reconcile: Poly `positions` not a dict ({type(positions).__name__}) "
                          f"— CANNOT VERIFY")
                return None
            for slug, item in positions.items():
                if not isinstance(item, dict):
                    continue
                # netPositionDecimal FIRST [box probe 2026-08-06: the exact holding beside
                # the rounded netPosition display]. The rounded fallback stays HERE only
                # because the honest rationale is modest: the arb bot is
                # stopped, this reader tolerated rounded values its whole life, and
                # prepend-only preserves that behaviour when the field is absent. (Not
                # "errs loud" — venue<known is deliberately logged-not-escalated here, so
                # a round-DOWN read errs quiet in that direction.)
                qty = _first_qty(item, "netPositionDecimal", "netPosition", "qtyAvailable")
                if qty is None:
                    log.error(f"reconcile: unparseable Poly position {slug}={item!r} "
                              f"— CANNOT VERIFY")
                    return None
                if _is_real_position(qty):
                    out.append(VenuePosition("poly", poly_slug(str(slug)), qty))
            cursor, pages = resp.get("nextCursor") or "", pages + 1
            if resp.get("eof", True) or not cursor or pages >= 20:
                if pages >= 20 and not resp.get("eof", True):
                    log.error("reconcile: Poly positions pagination hit the 20-page stop with "
                              "eof=false — CANNOT VERIFY (positions may be unread)")
                    return None
                break
        return out

    async def _reconcile_once(self) -> tuple[list[Divergence], bool]:
        """One pass. Returns (divergences, verified) — `verified` False = a venue could not be
        read, so an EMPTY divergence list must NOT be read as 'all clear'."""
        kalshi = await self._fetch_kalshi_positions()
        poly = await self._fetch_poly_positions()
        verified = kalshi is not None and poly is not None
        venue_positions = (kalshi or []) + (poly or [])
        known = known_quantities(
            self.tracker.list_positions(), load_whitelist(config.RECONCILE_WHITELIST_FILE)
        )
        return find_divergences(venue_positions, known), verified

    def _ack_divergences(self, divs: list[Divergence]) -> None:
        """Operator touched the ack flag: acknowledge these EXACT positions into the whitelist."""
        wl = load_whitelist(config.RECONCILE_WHITELIST_FILE)
        for d in divs:
            wl[(d.venue, d.market)] = d.venue_qty      # exact qty — a later change re-alerts
        save_whitelist(config.RECONCILE_WHITELIST_FILE, wl, note="acknowledged via reconcile_ack")
        log.warning(
            "reconcile: ACKNOWLEDGED "
            + "; ".join(f"{d.venue} {d.market} qty={d.venue_qty:.0f}" for d in divs)
            + f" → {config.RECONCILE_WHITELIST_FILE}. These no longer alert AT THIS EXACT SIZE."
        )

    async def _reconcile_loop(self) -> None:
        """Periodically ask both venues what we ACTUALLY hold; alert + pause on unknown exposure.

        Never trades. Escalation is via add_stranded → the existing alert + _strand_control_loop +
        `logs/resume` operator flow, rather than a parallel halt mechanism."""
        if not config.RECONCILE_ENABLED:
            log.warning("reconcile: DISABLED — an unrecorded venue position will NOT be detected")
            return
        log.info(
            f"reconcile: watching venue-vs-tracker every {config.RECONCILE_INTERVAL_S}s "
            f"(confirm x{config.RECONCILE_CONFIRM_POLLS}); ack via "
            f"`touch {config.RECONCILE_ACK_FLAG}`"
        )
        seen, unverified = 0, 0
        while True:
            await asyncio.sleep(config.RECONCILE_INTERVAL_S)
            try:
                divs, verified = await self._reconcile_once()
            except Exception as e:                    # a guard must never kill its own loop
                log.error(f"reconcile: pass failed: {e!r}")
                continue

            if not verified:
                # CANNOT VERIFY ≠ all clear. Warn, and alert if it persists — but do NOT pause:
                # we have no evidence of a position, and a transient API blip must not halt.
                unverified += 1
                if unverified == config.RECONCILE_CONFIRM_POLLS:
                    log.critical(
                        "🚨 reconcile: CANNOT VERIFY venue positions for "
                        f"{unverified} consecutive polls — flying blind on unrecorded exposure"
                    )
                    self._reconcile_alert(
                        "⚠️ Position reconciliation is FAILING — cannot read venue positions. "
                        "An unrecorded position would go undetected. Check API health."
                    )
                continue
            unverified = 0

            if not divs:
                seen = 0
                continue

            # Operator acknowledged: fold these EXACT positions into the whitelist and clear.
            if os.path.exists(config.RECONCILE_ACK_FLAG):
                try:
                    os.remove(config.RECONCILE_ACK_FLAG)
                except OSError:
                    pass
                self._ack_divergences(divs)
                seen = 0
                continue

            seen += 1
            detail = "; ".join(
                f"{d.venue} {d.market}: venue={d.venue_qty:.0f} known={d.known_qty:.0f} "
                f"(unknown {d.unknown:.0f})" for d in divs
            )
            if seen < config.RECONCILE_CONFIRM_POLLS:
                # Kalshi's portfolio GET is eventually consistent — one sighting proves nothing.
                log.warning(f"reconcile: divergence {seen}/{config.RECONCILE_CONFIRM_POLLS} "
                            f"(confirming) — {detail}")
                continue

            log.critical(f"🚨 reconcile: UNRECORDED VENUE POSITION — {detail}")
            existing = {p.token_id: p for p in self.tracker.list_positions()}
            for d in divs:
                tok = recon_token(d.venue, d.market)
                prev = existing.get(tok)
                if prev is not None and abs(prev.shares - d.unknown) < _EPS:
                    continue                          # already recorded this exact divergence
                self.tracker.add_stranded(
                    f"reconcile:{d.venue}:{d.market}",
                    f"UNRECORDED {d.venue} position ({d.market})",
                    tok, d.unknown, 0.0,               # cost basis unknown — we never saw the fill
                )
            self._reconcile_alert(
                f"🚨 UNRECORDED POSITION — the venue holds more than the bot knows about.\n"
                f"{detail}\n"
                f"Trading is PAUSED.\n"
                f"  • If this is yours (manual trade / probe): `touch {config.RECONCILE_ACK_FLAG}` "
                f"to acknowledge it at this exact size, then `touch logs/resume`.\n"
                f"  • Otherwise investigate before resuming — a lost order response can leave a "
                f"real unhedged leg."
            )

    def _reconcile_alert(self, msg: str) -> None:
        """Alert without ever raising into the loop (an alert failure must not blind the guard)."""
        try:
            send_reconcile_alert(msg)
        except Exception as e:
            log.error(f"reconcile: alert failed: {e!r} — {msg}")
