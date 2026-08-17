"""Kalshi TRADE-tape subscriber — a second WS on the `trade` channel.

WHY A SEPARATE FEED. `KalshiOrderBookCache` subscribes to `orderbook_delta`/`ticker`, which tell you
what the book LOOKS like, never what actually TRADED. Every question about adverse selection needs
the tape: whether a price move was a real print or a quote flicker, and where a resting quote would
have been hit. Kalshi delivers that on its own channel, so it needs its own connection.

Extracted from `scripts/kalshi_markout_probe._trade_ws`, which proved the mechanics on real flow.
It lives in `bot/` because two callers need it — the live MM tool (adverse-move context around our
own fills) and the Kalshi shadow-MM port (the trade flow a simulated quote is filled against).
Copy-pasting it a third time is how the two drift apart and stop being comparable.

⚠️ TWO INVERSION TRAPS, both of which have already cost real measurements here:

1. **`taker_book_side` — UNVERIFIED, and the codebase contradicts itself about it.** One place calls
   it richer than the Poly tape — an EXPLICIT aggressor field, no inference needed — while the code
   that actually shipped says the field follows a YES/NO complementary-matching convention that
   inverts if read naively, and uses price-vs-mid instead. Neither claim was ever measured. Do not
   treat the inversion as established; it is repeated here only because the working code chose that
   path.
   `aggressor_from_mid()` is a DERIVATION from a caller-supplied book snapshot; `taker_book_side` is
   a VENUE FIELD, and the standing rule is to read the field. Right move when this is next touched:
   log BOTH and measure the disagreement rate against known own-fills (`raw` is on every event, so
   it costs one line). Meanwhile mid-inference at least matches `passive_markout_probe` on Poly, so
   cross-venue markout stays comparable — which is a reason to compute it, not to discard the field.
   ⚠️ `aggressor_from_mid` needs a mid from BEFORE the print. If the caller reads the book after the
   WS has applied the resulting delta, the inference inverts.

2. **`get_depth(side)` is the OPPOSITE ladder.** It means "size available to BUY that side", so
   `get_depth(t, "no")` is the YES-BID depth and `get_depth(t, "yes")` is the YES-ASK depth — see
   `feed.get_depth`. Mapping them the obvious way inverts OBI. It did, and produced a false null in
   the order-book-imbalance work before anyone noticed.

READ-ONLY. This places nothing and holds no position; it only listens.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable

import websockets

log = logging.getLogger(__name__)

_RECONNECT_S = 3.0
# Bounded: a long run against an unrecognised schema would otherwise retain every message it saw.
# Small on purpose — the control frames arrive at the head of the connection, so a handful is enough
# to identify the envelope, and this is a diagnostic, not a tape.
_UNKNOWN_FRAME_CAP = 8


def _epoch_s(v) -> float | None:
    """Venue timestamp → epoch SECONDS, or None.

    Mirrors `bot.kalshi.maker._fill_ts`'s hardening, deliberately: a millisecond epoch taken at face
    value lands ~55,000 years out, which makes every markout horizon instantly "due" and produces a
    full set of mk=0 rows that look like real measurements. A shared module that two markout callers
    depend on is exactly where that guard has to live."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f > 1e11:
        f /= 1000.0
    return f if 1e9 < f < 4e9 else None


def _dec(v) -> Decimal | None:
    """Venue string → exact Decimal. Parsed from the string form: `Decimal(0.47)` would launder a
    float's representation error into the Decimal and defeat the point."""
    if v is None:
        return None
    try:
        d = Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


class KalshiTradeFeed:
    """Subscribes to `trade` for `tickers` and calls `on_trade(event)` per print.

    `event` keys: `ticker`, `ts` (venue epoch seconds, or None), `yes_price` (Decimal), `qty`
    (Decimal), `raw` (the untouched venue message, so a caller can reach a field this does not
    normalise without needing a change here).

    The callback is invoked inline. Keep it cheap — appending to a list or writing a CSV row is
    fine; anything slow blocks the socket and you will silently drop tape.
    """

    def __init__(self, client, tickers: list[str],
                 on_trade: Callable[[dict], None | Awaitable[None]]) -> None:
        self._client = client
        self._tickers = list(tickers)
        self._on_trade = on_trade
        self.n_trades = 0
        self.n_reconnects = 0
        # ⚠️ A REJECTED SUBSCRIPTION MUST NOT LOOK LIKE A QUIET TAPE. Logging "subscribed" on SEND
        # rather than on ACK is how this repo has already lost two real-money runs: the WS logged
        # connected + subscribed while a filter dropped every message, and both runs ended
        # `quotes=0 fills=0` with no error anywhere. Kalshi's sparse tape (~1 print/40s across 60
        # tickers) makes zero trades entirely plausible, so silence here is indistinguishable from
        # success unless the ack is captured. `subscribed` stays False until the venue confirms.
        self.subscribed = False
        self.n_dropped = 0        # messages seen but not usable as trades — the other silent failure
        # ⚠️ SPLIT OUT OF `n_dropped` ON PURPOSE. `n_dropped` counts every frame we do not recognise,
        # and the ack names above are a GUESS — so a venue that emits any periodic control frame
        # would make it climb forever. A consumer that treats that as "we lost prints" (the maker's
        # queue attribution does) would then mark every order every second and return a run that is
        # 100% unreadable: a measurement dying quietly, which is the failure this file exists to
        # prevent. This counter is the narrow one — frames that WERE trades and could not be used,
        # i.e. actual lost prints.
        self.n_bad_trades = 0
        self.last_error: str | None = None
        # Errors are COUNTED, never swallowed silently. A tap that raises on every message and says
        # nothing turns a total failure into a clean-looking "no trades" — the exact way a shadow
        # probe once printed an all-zero table with no error anywhere.
        self.errors: dict[str, int] = {}
        # ⚠️ THE ACK TYPES BELOW ARE A GUESS: `subscribed`/`ok`/`error` were never captured from
        # Kalshi, only assumed. So keep the first few frames we do NOT recognise — verbatim, bounded
        # — and the next real run turns the guess into a captured envelope. If `subscribed` stays
        # False while trades flow, the real ack type is sitting in here.
        self.unknown_frames: list[str] = []
        # Prints the tape DELIVERED and the consumer then threw away — a raising `on_trade`. Kept
        # separate from `errors`, which also accrues reconnect exceptions and venue `error` frames:
        # a consumer feeding those into a lost-print signal would mark every live order on every
        # poll during an error storm and return a run that is 100% unreadable.
        self.n_callback_errors = 0

    def _note(self, exc: Exception) -> None:
        key = f"{type(exc).__name__}: {str(exc)[:120]}"
        self.errors[key] = self.errors.get(key, 0) + 1

    async def run_forever(self) -> None:
        while True:
            try:
                # ⚠️ 5s/5s, not the library's 20/20. A server-side stall raises only after
                # ping_interval + ping_timeout, and until it does `n_reconnects` does not move,
                # `subscribed` stays True, and every order resting in that window accrues traded=0
                # behind a clean flag. At 20/20 that blind window is ~40s, which spans 1-6 whole
                # order lifetimes at --requote-s 6-30 — and because Kalshi's tape is SPARSE,
                # silence is genuinely indistinguishable from a quiet market, so nothing downstream
                # can recover it. 5/5 cuts the blind window to ~10s. Pings are a few bytes; this is
                # not a meaningful cost.
                async with websockets.connect(
                    self._client.ws_url, additional_headers=self._client.ws_headers(),
                    ping_interval=5, ping_timeout=5, max_size=None,
                ) as ws:
                    # ⚠️ RESET BEFORE EVERY SUBSCRIBE. Carrying `subscribed=True` across a reconnect
                    # defeats the entire guard: the feed acks at t=0, the socket drops an hour later,
                    # the RE-subscribe is rejected (a ticker closed, creds rotated), the tape goes
                    # silent — and the flag still reads True. Reconnects are the normal case on a
                    # long run, which is why `n_reconnects` exists, so this is the expected path and
                    # not an edge case.
                    self.subscribed = False
                    await ws.send(json.dumps({
                        "id": 1, "cmd": "subscribe",
                        "params": {"channels": ["trade"], "market_tickers": self._tickers},
                    }))
                    log.info(f"kalshi trade feed: subscribe SENT ({len(self._tickers)} tickers) "
                             f"— awaiting ack")
                    while True:
                        await self._handle(await ws.recv())
            except asyncio.CancelledError:
                raise
            except Exception as exc:                       # noqa: BLE001 — must survive to reconnect
                self._note(exc)
                self.n_reconnects += 1
                log.warning(f"kalshi trade feed: reconnecting after {exc!r}")
                await asyncio.sleep(_RECONNECT_S)

    async def _handle(self, raw) -> None:
        try:
            m = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        mtype = m.get("type")
        if mtype in ("subscribed", "ok"):
            self.subscribed = True
            log.info(f"kalshi trade feed: subscription ACKED ({len(self._tickers)} tickers)")
            return
        if mtype == "error":
            # Loud, and it does NOT set `subscribed` — a rejected subscribe leaves the socket open
            # forever, so the only symptom would otherwise be a tape that never prints.
            self.last_error = str(m.get("msg") or m)[:200]
            self._note(RuntimeError(f"subscribe/stream error: {self.last_error}"))
            log.error(f"kalshi trade feed: venue ERROR — {self.last_error}")
            return
        if mtype != "trade":
            self.n_dropped += 1
            if len(self.unknown_frames) < _UNKNOWN_FRAME_CAP:
                self.unknown_frames.append(str(raw)[:400])
            return
        msg = m.get("msg") or {}
        ticker = msg.get("market_ticker")
        px = _dec(msg.get("yes_price_dollars"))
        if not ticker or px is None or px <= 0:
            # A renamed venue field lands here. Counted, so `n_trades=0, n_dropped=large` reads as
            # "the schema moved" rather than "the market was quiet". Also counted NARROWLY: this
            # frame really was a trade we could not use, i.e. a lost print.
            self.n_dropped += 1
            self.n_bad_trades += 1
            return
        qty = _dec(msg.get("count_fp"))
        if qty is None:
            # ⚠️ COUNTED AS A LOST PRINT, not just kept as None. This frame WAS a trade and cannot be
            # used — the definition of `n_bad_trades` — and every consumer drops it. Uncounted, a
            # `count_fp` rename gives `subscribed=True, n_reconnects=0, n_bad_trades=0` and a
            # climbing `n_trades` while the maker's queue attribution accrues nothing: all health
            # flags clean, every fill scoring as a queue that never traded. The total-rename case is
            # backstopped by the own-fill completeness guard; an INTERMITTENT parse failure is not.
            self.n_bad_trades += 1
        event = {
            "ticker": ticker,
            "ts": _epoch_s(msg.get("ts")),
            "yes_price": px,
            # None (unparseable) is kept DISTINCT from Decimal(0) (a genuine zero print):
            # collapsing them makes any volume-weighted statistic silently read a parse failure as
            # no size.
            "qty": qty,
            "raw": msg,
        }
        self.n_trades += 1
        try:
            r = self._on_trade(event)
            if inspect.isawaitable(r):        # not iscoroutine: a returned Task/Future would be
                await r                        # dropped rather than awaited

        except Exception as exc:                           # noqa: BLE001 — a bad callback must not
            self._note(exc)                                # kill the tape for everyone else
            self.n_callback_errors += 1                    # ...but the print IS lost — say so


def aggressor_from_mid(yes_price: Decimal, mid: Decimal) -> str:
    """Which side was the aggressor — `offer_hit` or `bid_hit` — inferred from price vs mid.

    Deliberately NOT `taker_book_side` (see the module docstring: it inverts). This is also the
    inference `passive_markout_probe` uses on Poly, which is what keeps cross-venue markout numbers
    comparable rather than differing by convention.

      · print at/above the mid → the offer was lifted → a maker's ASK filled → maker is SHORT
      · print below the mid    → the bid was hit      → a maker's BID filled → maker is LONG
    """
    # Coerce the mid: callers hold it as a float (`maker.MakerSession.mid` returns one), and
    # `Decimal("0.43") == 0.43` is False — so the tie branch below would essentially never fire and
    # the 5.3% sign flip it exists to prevent would come straight back, with the tests still green
    # because they pass Decimals. Same class as `0.47/0.01 == 46.99…`.
    if not isinstance(mid, Decimal):
        mid = _dec(mid)
        if mid is None:
            return "unknown"
    if yes_price == mid:
        # UNKNOWN, not a guess. On the existing 30,894-row Kalshi tape 1,646 prints (5.3%) sit
        # exactly at the mid, and `>=` assigned every one of them to offer_hit. That does not add
        # noise — it flips the SIGN of those observations' markout, in one direction, on 5% of the
        # population feeding the number that decides maker viability. Let the caller drop them.
        return "unknown"
    return "offer_hit" if yes_price > mid else "bid_hit"


@contextlib.asynccontextmanager
async def trade_feed(client, tickers: list[str], on_trade):
    """Run a `KalshiTradeFeed` for the duration of the block, cancelling it cleanly on exit.

    The manual version of this leaks a task on every early return, which in a long-lived MM process
    means a slow accumulation of live sockets nobody is reading."""
    feed = KalshiTradeFeed(client, tickers, on_trade)
    task = asyncio.create_task(feed.run_forever())
    try:
        yield feed
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
