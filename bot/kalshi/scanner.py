"""
bot/kalshi/scanner.py
─────────────────────
Discovers open Kalshi markets for a set of series tickers.

Mirrors the role of bot/scanner.py for Polymarket — returns a list of
typed KalshiMarket objects ready for cross-platform matching.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from bot.core.logger import get_logger

log = get_logger(__name__)


@dataclass
class KalshiMarket:
    ticker: str                      # e.g. "KXNBAGAME-LAKCEL-JUN14"
    event_ticker: str                # e.g. "KXNBAGAME-LAKCEL"
    title: str                       # e.g. "Lakers vs Celtics"
    subtitle: str                    # e.g. "Jun 14, 2026"
    yes_side_label: str              # team/outcome for YES, e.g. "Lakers"
    no_side_label: str               # team/outcome for NO, e.g. "Celtics"
    close_time: str                  # ISO8601 — postponement buffer, NOT game end
    status: str                      # "open", "closed", etc.
    expected_expiration_time: str = ""  # ISO8601 — actual expected game-end + settlement
    yes_bid: float = 0.0             # current YES bid (from REST, for cache priming)
    yes_ask: float = 0.0             # current YES ask (from REST, for cache priming)
    price_tick: float = 0.01         # min price increment (price_ranges[].step; fallback <n>)
    volume_24h: float = 0.0          # contracts traded in 24h — flow proxy for MM target ranking


# The price we read the tick AT. Kalshi's tick is a FUNCTION OF PRICE, so a market has no single
# tick and any scalar is a choice of reference point. 0.50 is the middle of the band we quote in
# PRACTICE — 449 logged would_fire rows span kalshi_limit 0.12–0.86, with 7 below 0.15 and 2 above
# 0.85 — but nothing ENFORCES that band: `_kalshi_breakeven_ask` clamps only to [0.01, 0.99] and the
# unwind sell is `max(0.01, ...)`, so a Poly ask of 0.90+ puts the breakeven ask under 0.10 by
# arithmetic. Prefer `price_tick_at(market, price)` at the price you are about to send; this scalar
# exists for the per-market field and is the weaker of the two.
_TICK_REFERENCE_PRICE = 0.50


def price_tick_at(market: dict, price: float) -> float:
    """The market's min price increment AT `price`.

    ⚠️ KALSHI'S TICK IS PRICE-DEPENDENT. There is no per-market tick field; each market carries a
    `price_ranges` STEP LADDER plus a `price_level_structure` label. Two shapes exist
    [VERIFIED 2026-07-19 live vs `/markets/{t}`, 15 series]:
      · `linear_cent`       → one range, step 0.01 throughout.
      · `tapered_deci_cent` → step 0.001 below 0.10 AND above 0.90, but **0.01 in between**
                              (elections/politics: SENATEIA, SENATETX, CONTROLH/S, KXPRESNOMR).

    Fallback 0.01, and NEVER 0/None — a zero tick divides-by-zero in kalshi_tick_floor."""
    ranges = market.get("price_ranges") or []
    for rg in ranges:
        try:
            if float(rg["start"]) <= price < float(rg["end"]):
                step = float(rg["step"])
                return step if step > 0 else 0.01
        except (TypeError, ValueError, AttributeError, KeyError):
            continue
    return 0.01


def _parse_price_tick(market: dict) -> float:
    """The market's tick in the CONTESTED BAND — the one scalar tick worth carrying per market.

    ⚠️ This read `price_ranges[0].step` and asserted "every binary market is `linear_cent` today".
    That is FALSE as of 2026-07-19: on a `tapered_deci_cent` market `price_ranges[0]` is the
    **0.00–0.10 tail**, so it returned **0.001** for a market whose tick at any tradeable price is
    **0.01**. Consequence, had one ever reached the fire path: `kalshi_tick_floor` would floor onto a
    0.001 grid and Kalshi rejects an off-tick price with HTTP 400 — the order simply never rests.

    LATENT, never live — but state the evidence precisely, because it is thinner than it looks. What
    is MEASURED: all 182 `would_fire` rows carrying the column logged `kalshi_tick=0.0100`, and of
    the 12 series in `matcher._SETTLEMENT_EQUIVALENT` exactly ONE (KXNPBGAME) was in the 2026-07-19
    ladder scan. The other 11 are UNMEASURED — inferred `linear_cent` from the logged ticks, which
    cannot distinguish "read the ladder correctly" from "took the 0.01 fallback".

    Safe-direction in BOTH directions the tick feeds, for two different reasons — the "it's cheaper"
    argument covers only the buy leg:
      · BUY (`kalshi_arb:_execute_kalshi_arb`) — a coarser tick floors the limit further DOWN, so we
        pay less; worst case is a missed fill.
      · UNWIND (`_unwind_kalshi_excess`) — a coarser tick LOWERS the sell limit, which is not
        "cheaper" for us. It is safe because that order is an IOC crossing into resting bids: fills
        land at the book's bid prices, so a lower limit cannot worsen the price per share, it only
        lets us sweep deeper. A more-complete unwind is the favourable direction here, since a failed
        unwind means `add_stranded` and a global pause.

    ⚠️ RESIDUAL: a scalar tick read at 0.50 is wrong for any ladder whose TAIL step is coarser than
    its band step. No such shape is known, but none is ruled out either. On the buy leg that costs a
    fill; on the UNWIND leg an off-tick sell is rejected → strand → global pause. Closing it means
    calling `price_tick_at(market, price)` at the send site — see the private design notes."""
    return price_tick_at(market, _TICK_REFERENCE_PRICE)


def _parse_volume_24h(market: dict) -> float:
    """Contracts traded in 24h. `volume_24h_fp` is a fixed-point STRING, not dollars.

    Defensive for the same reason as _parse_price_tick: this parser runs on the LIVE cross-arb scan
    path, and `fetch_markets` catches per-SERIES, so one unexpected shape here would drop every market
    in that series from arb detection every ~3s — blind, with only an ERROR log. Kalshi has changed a
    field's shape under us twice (`orderbook`→`orderbook_fp`, `positions`→`market_positions`), and this
    field's only consumer is an offline ranking heuristic, so it must never be able to raise.
    0.0 on anything unreadable — an unranked market sorts last, it does not disappear."""
    for key in ("volume_24h_fp", "volume_24h"):
        try:
            v = float(market.get(key))
        except (TypeError, ValueError):
            continue
        if v >= 0:
            return v
    return 0.0


def _parse_event(event: dict) -> list[KalshiMarket]:
    """Extract open KalshiMarket objects from a Kalshi Events API event dict."""
    event_ticker = event.get("event_ticker", "")
    title = event.get("title", "")
    markets = []
    for m in event.get("markets", []):
        if m.get("status") not in ("open", "active"):
            continue
        markets.append(KalshiMarket(
            ticker=m.get("ticker", ""),
            event_ticker=event_ticker,
            title=title,
            subtitle=m.get("subtitle", ""),
            yes_side_label=m.get("yes_sub_title", ""),
            no_side_label=m.get("no_sub_title", ""),
            close_time=m.get("close_time", ""),
            status=m.get("status", ""),
            expected_expiration_time=(
                m.get("expected_expiration_time") or m.get("occurrence_datetime") or ""
            ),
            yes_bid=float(m.get("yes_bid_dollars") or 0),
            yes_ask=float(m.get("yes_ask_dollars") or 0),
            price_tick=_parse_price_tick(m),
            volume_24h=_parse_volume_24h(m),
        ))
    return markets


class KalshiScanner:
    """Fetches open Kalshi markets for a list of series tickers."""

    def __init__(self, client) -> None:
        self._client = client
        # Last per-series breakdown logged at INFO. The scanner runs every ~3s, so
        # we re-log only when the mix changes (e.g. MLB 0→14 when games open) —
        # the old unconditional summary line was removed for being too noisy.
        self._last_breakdown: dict[str, int] | None = None

    async def fetch_markets(self, series_tickers: list[str]) -> list[KalshiMarket]:
        """Fetch open markets across the given series tickers."""
        all_markets: list[KalshiMarket] = []
        breakdown: dict[str, int] = {}
        for series in series_tickers:
            try:
                events = await self._client.get_events(series)
                series_markets = [m for event in events for m in _parse_event(event)]
                all_markets.extend(series_markets)
                breakdown[series] = len(series_markets)
                log.debug(f"KalshiScanner: {series} → {len(events)} events")
            except Exception as e:
                log.error(f"KalshiScanner: error fetching {series}: {e}")
        self._log_breakdown(len(all_markets), breakdown)
        return all_markets

    def _log_breakdown(self, total: int, breakdown: dict[str, int]) -> None:
        """Log per-series market counts — INFO when the mix changes, else DEBUG.

        Only successfully-fetched series appear in ``breakdown``; a series that
        errored out is omitted (an error ≠ "0 markets") and so won't flip the mix.
        """
        summary = " ".join(f"{s}={n}" for s, n in breakdown.items())
        msg = (
            f"KalshiScanner: {total} open markets across "
            f"{len(breakdown)} series\n({summary})"
        )
        if breakdown != self._last_breakdown:
            log.info(msg)
            self._last_breakdown = breakdown
        else:
            log.debug(msg)
