"""NOTIFY-ONLY probe-stats channel [operator-approved 2026-08-26].

The probe lane is the one lane whose whole point is EVIDENCE, and its evidence arrives
overnight while nobody is reading a log file. This posts what the probe is doing — per fill,
per completed round trip, every latch / mark-tripwire event, and the end-of-run EV-bar verdict
block — to a Discord channel of its own.

⛔ **IT IS NOT ON THE MONEY PATH AND MUST NEVER BECOME ONE.** Three properties, all pinned by
tests in `tests/test_probe_notify.py`:

  1. **Absent env var = silently off.** `DISCORD_PROBE_WEBHOOK_URL` unset ⇒ every entry point
     is a no-op that returns None. Not an error, not a warning, not a startup refusal — an
     operator who never configures this must never see a difference.
  2. **A webhook failure is SWALLOWED with ONE log line.** `event()` / `tick()` / `flush()`
     cannot raise: their bodies are wrapped, and the post itself runs off the event loop. The
     quote cycle completes whether the channel is healthy, dead, revoked or a black hole.
     (`bot/core/alerts.py` learned this the hard way — an un-timed-out post hung a live process
     for good; the timeout here is mandatory for the same reason.)
  3. **NO FALLBACK to `DISCORD_WEBHOOK_URL`.** This is a stats firehose. An unset var must
     never dump per-fill chatter onto the alerts channel the operator's phone mirrors — the
     same rule the significant-edge channel already carries in `bot/core/config.py`.

⚠️ **PROBE LANE ONLY.** `for_lane()` returns a DISABLED notifier for any lane but `probe`, so
the main-lane maker is untouched by construction rather than by a caller remembering to check.

**Rate sanity.** A burst of fills posts ONE message, not one per fill: events are queued and
flushed at most once per `DEBOUNCE_S`. The first event of a quiet period goes out immediately
(a lone fill should not wait), and a queue that has gone quiet is drained by `tick()`, which
the quote loop calls once a cycle — without it a trailing event would sit until teardown.

**Time accuracy.** Debouncing means the instant a message ARRIVES says
nothing about when its events happened, so every queued line carries the UTC wall-clock time it
was QUEUED at (`[HH:MM:SSZ] `). Two ordering rules stand behind that stamp: batches are posted
by a single worker thread so they land strictly FIFO (`_dispatch`), and the maker enqueues a
fill, its round trip and any latch the trip armed in CAUSAL order (`maker._book_fill`) — the
operator's report was latches appearing before the fills that caused them.
"""
from __future__ import annotations

import asyncio
import atexit
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Callable, Optional, Sequence

import requests

from bot.core import config
from bot.core.redact import safe_exc

log = logging.getLogger(__name__)

#: The only lane this channel speaks for, and the ONE definition of the probe lane's name.
#: ⛔ `scripts.poly_probe_night` IMPORTS this rather than spelling it again: two copies mean a
#: lane rename silently disables the channel (the maker's lane stops matching) while every
#: probe tool keeps working, which is the failure nobody would notice until a night produced
#: no messages. The direction is fixed — scripts may import `bot.core`, never the reverse.
PROBE_LANE = "probe"

#: Mandatory. `requests` reads `None` as "wait forever"; see the alerts.py header for the
#: live process that hung on exactly that.
POST_TIMEOUT_S = 10

#: One message per this many seconds, at most. The maker's cycle is ~3-4 s and a burst gate
#: can open several books at once, so an un-debounced per-fill post is a rate-limit machine.
DEBOUNCE_S = 20.0

#: Discord's hard body limit is 2000 characters; leave room for the code fence.
MAX_MESSAGE_CHARS = 1900

#: Backstop for a channel that has been down a long time: keep the NEWEST events, drop the
#: oldest. An unbounded queue beside a live maker is a memory leak on a 1.9 GB box.
MAX_QUEUE = 200

#: `report_verdict`'s own heading for the EV-bar section. The verdict text is reused VERBATIM
#: — no number in this module is ever re-derived from the tapes.
VERDICT_MARKER = "=== TOES-IN EV BAR"


def stamp(now: float) -> str:
    """`[HH:MM:SSZ] ` for a UTC wall-clock instant. The ONE definition of the prefix format."""
    return time.strftime("[%H:%M:%SZ] ", time.gmtime(now))


def _post_webhook(url: str, text: str) -> None:
    """The one place an HTTP request is made. Timeout is not optional."""
    requests.post(url, json={"content": text}, timeout=POST_TIMEOUT_S)


def chunks(text: str, *, limit: int = MAX_MESSAGE_CHARS) -> list[str]:
    """Split on line boundaries into messages of at most `limit` characters.

    A single line longer than the limit is hard-split rather than dropped — a truncated
    diagnostic still beats a missing one.
    """
    out: list[str] = []
    current = ""
    for line in text.splitlines():
        while len(line) > limit:
            if current:
                out.append(current)
                current = ""
            out.append(line[:limit])
            line = line[limit:]
        if not current:
            current = line
        elif len(current) + 1 + len(line) <= limit:
            current = f"{current}\n{line}"
        else:
            out.append(current)
            current = line
    if current:
        out.append(current)
    return out


class ProbeNotifier:
    """Queue events, post at most one batched message per `debounce_s`.

    Every public method is total: it returns None and never raises, whatever the channel does.
    """

    def __init__(self, url: str, *, debounce_s: float = DEBOUNCE_S,
                 clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time,
                 post: Optional[Callable[[str, str], None]] = None) -> None:
        self.url = str(url or "")
        self.debounce_s = max(0.0, float(debounce_s))
        #: ⛔ TWO CLOCKS, deliberately. `clock` is MONOTONIC and owns the debounce window — a
        #: wall clock stepping backwards (NTP) would stall the queue for the step. `wall_clock`
        #: is what an operator reads, and it is only ever used to stamp a queued line, never to
        #: decide anything. Both are injectable so a test can pin either.
        self._clock = clock
        self._wall_clock = wall_clock
        self._post = post if post is not None else _post_webhook
        #: Created on first off-loop dispatch, then reused: ONE worker, so batches post in the
        #: order they were drained. See `_dispatch`.
        self._executor: Optional[ThreadPoolExecutor] = None
        self._queue: list[str] = []
        #: 0.0 so the FIRST event of a run posts immediately — a lone probe fill at 03:00 is
        #: exactly the message that must not wait a debounce window.
        self._next_post_at = 0.0
        #: A dead webhook must not spam the alerts channel through the WARNING Discord
        #: handler: the first failure is loud, repeats are DEBUG until one succeeds.
        self._failing = False

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    # ── entry points — none of these may raise ───────────────────────────────────────────

    def event(self, text: "str | Callable[..., str]", *args: object,
              **kwargs: object) -> None:
        """Queue one line, and post the batch if the debounce window has elapsed.

        ⛔ `text` may be a RENDERER plus its arguments (`event(fill_line, slug=…, …)`) — and on
        the money path it always is. Rendering at the call site would evaluate OUTSIDE this
        method's `try`, so a `Decimal` that came back `None` from a venue read would raise
        straight into the quote cycle and make the "cannot raise into a caller" contract a
        comment rather than a fact. Called here, a broken renderer is one swallowed log line.

        The line is stamped with the wall-clock time it is QUEUED at, not the time its batch
        posts: a debounced batch can be up to `DEBOUNCE_S` old, and a teardown flush much older
        than that, so the arrival instant is not the event's instant.

        ⛔ A MULTI-LINE event (the verdict block) is stamped on its FIRST LINE ONLY, and the
        rest of it stays byte-for-byte the report's own text. The verdict block's rule is that
        nothing in it may be re-derived for the channel; a prefix on line 1 is PRESENTATION —
        it adds no number and changes none — whereas stamping every line would rewrite a
        rendered table the operator compares against their terminal.
        """
        if not self.enabled:
            return
        try:
            rendered = text(*args, **kwargs) if callable(text) else str(text)
            if not rendered:
                return
            self._queue.append(stamp(self._wall_clock()) + rendered)
            if len(self._queue) > MAX_QUEUE:
                del self._queue[:-MAX_QUEUE]
            self._drain(force=False)
        except Exception as exc:                      # never into the caller
            self._complain(exc)

    def tick(self) -> None:
        """Called once per quote cycle: drain a queue that has gone quiet."""
        if not self.enabled or not self._queue:
            return
        try:
            self._drain(force=False)
        except Exception as exc:
            self._complain(exc)

    def flush(self) -> None:
        """Post whatever is queued NOW, ignoring the debounce. Teardown calls this."""
        if not self.enabled or not self._queue:
            return
        try:
            self._drain(force=True)
        except Exception as exc:
            self._complain(exc)

    # ── internals ────────────────────────────────────────────────────────────────────────

    def _drain(self, *, force: bool) -> None:
        if not self._queue:
            return
        now = self._clock()
        if not force and now < self._next_post_at:
            return
        batch, self._queue = self._queue, []
        self._next_post_at = now + self.debounce_s
        self._dispatch(chunks("\n".join(batch)))

    def _dispatch(self, messages: Sequence[str]) -> None:
        """Post OFF the event loop. `requests` is synchronous; called straight from a
        coroutine a slow post stalls the single loop that both the WS feeds and every quote
        timer live on — the alarm becoming the outage (alerts.py `_dispatch`). With no loop
        running (a script, a test) we are already off it, so post inline.

        ⛔ ONE executor job for the WHOLE batch, not one per chunk. Separate jobs run on
        separate threads with no ordering guarantee, so a two-chunk EV-bar block could arrive
        with its second half first — the channel would read as corrupt on exactly the message
        that matters most.

        ⛔ ONE WORKER, notifier-owned, for the same reason ACROSS batches [operator 2026-08-30:
        "batches arrive in random order"]. The default executor `run_in_executor(None, …)` uses
        is a POOL: batch 2 could run on a second thread while batch 1's post is still in
        flight, and a slow first post reordered the channel against the tape. `max_workers=1`
        makes delivery strictly FIFO — the queue order IS the wire order — and it also bounds
        this channel's thread cost to one on a 1.9 GB box.

        ⚠️ THIS CALL CAN RAISE and the inner swallow cannot catch it: `submit` raises
        `RuntimeError` once the executor has been shut down (as `run_in_executor` did on a
        closing loop), which is live precisely during a SIGINT-at-teardown. The callers' `try`
        is the thing that contains it — and since `teardown()` flushes before its phases, an
        escape here would skip cancel-all, flatten and the stray sweep, i.e. strand live orders
        over a notification. Pinned by
        `test_a_raising_DISPATCH_still_lets_teardown_run_all_four_phases`.

        ⚠️ Thread lifetime — the worker is NOT a daemon (`thread_name_prefix` names threads, it
        does not detach them), so the interpreter JOINS it at exit; an `atexit` hook asks for
        `shutdown(wait=False)` so the pool is told to stop as early as we can tell it. Why that
        join cannot hang the process, which is the only property that matters here: a job is a
        finite list of `_post_quietly` calls and every one of them is a `requests.post` with
        `POST_TIMEOUT_S`, so the worst case is bounded by the batch's chunk count × 10 s and
        cannot be unbounded — which is exactly the guarantee `alerts.py` lacked when an
        un-timed-out post hung a live process for good.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._post_batch(messages)
            return
        self._ensure_executor().submit(self._post_batch, messages)

    def _ensure_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1,
                                                thread_name_prefix="probe-notify")
            atexit.register(self._executor.shutdown, wait=False)
        return self._executor

    def _post_batch(self, messages: Sequence[str]) -> None:
        for message in messages:
            self._post_quietly(message)

    def _post_quietly(self, message: str) -> None:
        try:
            self._post(self.url, message)
        except Exception as exc:
            self._complain(exc)
            return
        self._failing = False

    def _complain(self, exc: BaseException) -> None:
        """ONE log line, then quiet until the channel recovers. `safe_exc`, not `{exc!r}` — a
        `requests` exception embeds the request URL, i.e. the webhook secret."""
        if self._failing:
            log.debug(f"probe notify: still failing ({safe_exc(exc)}) — swallowed")
            return
        self._failing = True
        log.warning(f"probe notify: post failed ({safe_exc(exc)}) — swallowed; the probe run "
                    f"is unaffected, only the notify channel is")


#: A disabled singleton is enough for every non-probe caller — it holds no state worth
#: separating and its methods are no-ops.
def for_lane(lane: str, *, url: Optional[str] = None,
             debounce_s: float = DEBOUNCE_S) -> ProbeNotifier:
    """The notifier for `lane`. Probe lane + a configured webhook → live; anything else → off.

    ⛔ Reads `config` at CALL time, never at import, so `tests/conftest.py`'s autouse blanking
    of the webhook URLs actually reaches it.
    """
    if str(lane or "") != PROBE_LANE:
        return ProbeNotifier("")
    return ProbeNotifier(config.DISCORD_PROBE_WEBHOOK_URL if url is None else url,
                         debounce_s=debounce_s)


# ── renderers — the exact text that reaches the channel, testable without a maker ────────

def _num(value: object) -> str:
    return "--" if value is None else str(value)


def _d3(value: Decimal) -> str:
    """DISPLAY ONLY — money/quantity at 3 decimal places.

    ⛔ Formats the `Decimal` directly (`:.3f` needs no float), so nothing here launders a
    price, a size or a P&L through a float. It is the ONE display idiom in this module: every
    money/qty interpolation below goes through it, and no computation, tape row or venue
    payload ever does. The channel used to print a full round-trip quotient
    (`$-2.175384615384615384615384615`), which is unreadable at 03:00 on a phone.
    """
    return f"{value:.3f}"


def fill_line(*, slug: str, side: str, price: Decimal, filled_qty: Decimal,
              cum_filled_qty: Decimal,
              commission_order_total: Optional[Decimal]) -> str:
    """One fill.

    ⛔ The rebate is the venue's READ-BACK (`commissionNotionalTotalCollected`, sign-flipped),
    and it is a PER-ORDER CUMULATIVE total, not this increment's credit — labelled as such,
    because the venue rounds the credit per fill increment and a per-fill split would be a
    number we invented. `--` means the venue did not report one on this row, never zero.
    """
    if commission_order_total is None:
        rebate = "rebate -- (not read back)"
    else:
        # ⛔ NOT `_d3`. The rebate is sub-cent by construction (the venue's own read-back, at
        # its own precision — 0.0138 here, ~0.0018 on a typical increment), so 3dp would round
        # most credits to <n> or $0.000 and make the one number this line exists to carry
        # unreadable. It has no unbounded tail to trim: it is a venue string, not a quotient.
        rebate = f"rebate ${-commission_order_total} (order total, venue read-back)"
    return (f"💵 FILL {slug} {side} {_d3(filled_qty)} @ {_d3(price)} "
            f"(cum {_d3(cum_filled_qty)}) · {rebate}")


def round_trip_line(*, slug: str, realized: Decimal, closed: Decimal) -> str:
    """One COMPLETED round trip's realized P&L. Price-realized only — the same channel the
    loss cap sees, and blind to rebates, rewards and settlement, so it is labelled rather than
    presented as the trip's economics."""
    per_ct = _d3(realized / closed) if closed else "--"
    return (f"🔁 ROUND TRIP {slug} realized ${_d3(realized)} over {_d3(closed)} contract(s) "
            f"= ${per_ct}/ct (PRICE-realized only — no rebate, reward or settlement term)")


def event_line(kind: str, slug: str, detail: str, *, icon: str = "🔒") -> str:
    """A latch / tripwire / rail event, verbatim from the rail that fired it.

    `icon` MIRRORS THE RAIL'S OWN LOG LINE so the channel and the journal read alike: 🔒 for
    the adverse latch, 🛑 for the mark tripwire, 🛡️ for the width shield. The last one is the
    load-bearing distinction — the shield does NOT stand a book down, and reading it as a trip
    is the exact confusion the shield was introduced to end.
    """
    return f"{icon} {kind} {slug}: {detail}"


def verdict_block(rendered: str) -> str:
    """The EV-bar section of an ALREADY-RENDERED verdict report, VERBATIM.

    ⛔ Nothing here re-derives a number. `poly_probe_night.report_verdict` owns every figure in
    the block; this slices its own output at its own heading so the channel and the terminal
    cannot disagree. Returns "" when the marker is absent (an older report, a refusal) — and
    "" posts nothing.
    """
    idx = str(rendered or "").find(VERDICT_MARKER)
    return "" if idx < 0 else rendered[idx:].rstrip()


def post_verdict(rendered: str, *, lane: str = PROBE_LANE,
                 notifier: Optional[ProbeNotifier] = None) -> None:
    """Post the EV-bar block of a rendered verdict report. Silent no-op when off or absent."""
    block = verdict_block(rendered)
    if not block:
        return
    note = notifier if notifier is not None else for_lane(lane)
    note.event(block)
    note.flush()


__all__: Sequence[str] = (
    "DEBOUNCE_S", "MAX_MESSAGE_CHARS", "MAX_QUEUE", "POST_TIMEOUT_S", "PROBE_LANE",
    "VERDICT_MARKER", "ProbeNotifier", "chunks", "event_line", "fill_line", "for_lane",
    "post_verdict", "round_trip_line", "stamp", "verdict_block",
)
