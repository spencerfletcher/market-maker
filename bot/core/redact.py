"""Render an exception for a log line, a CSV cell or an alert without leaking credentials.

⛔ **WHY THIS EXISTS.** `aiohttp.ClientResponseError.__repr__` embeds `RequestInfo`, whose third field
is the OUTGOING HEADER DICT — for this repo that is `KALSHI-ACCESS-KEY` and
`KALSHI-ACCESS-SIGNATURE`. So `f"...: {exc!r}"` on any failed Kalshi call writes the live API key id
into whatever that log line reaches. It already happened: the production key id appeared verbatim,
several times over, in a plain log file. That instance was contained because the file happened to be
untracked and nothing reached git history — but the MECHANISM was not contained by anything.

Three things made it worse than a stray log line:
  · `logs/` is deliberately git-TRACKED, so one `git add -A` writes a permanent copy into history;
  · `bot/core/logger.py` attaches a Discord handler at WARNING to every logger, and the DRY posture
    does NOT suppress it (`DISCORD_NOTIFY_DRY_RUN` is true), so a single 429 posts it off-box;
  · the same `{exc!r}` pattern is live in `reconcile.py` (runs every 60s) and on the fire path.

`__str__` does not carry the headers. `safe_exc` therefore keeps what a reader actually needs — the
exception TYPE and its message — and drops the args payload that carries the credential. The regex is
defence in depth for any other type that renders a header dict into its message; it is deliberately
anchored on the header NAME rather than on a key-shaped pattern, because a rule that tried to
recognise the secret itself would either miss a rotated key or redact unrelated UUIDs.
"""
from __future__ import annotations

import re

# `KALSHI-ACCESS-KEY': 'abc…'` / `KALSHI-ACCESS-SIGNATURE=abc…` in any quoting style.
_AUTH_HEADER = re.compile(
    r"(KALSHI-ACCESS-(?:KEY|SIGNATURE)['\"]?\s*[:=]\s*['\"]?)[^'\",\s)}\]]+",
    re.IGNORECASE,
)


def safe_exc(exc: BaseException) -> str:
    """`repr(exc)` minus the payload that carries our auth headers.

    Returns `TypeName: message`. Prefer this over `{exc!r}` anywhere the result can reach a log file,
    a CSV cell or an alert — which is everywhere, since the Discord handler is attached at WARNING.
    Plain `{exc}` is NOT an adequate substitute: several exception types this code catches have an
    empty `str()` (`TimeoutError`, `CancelledError`), so the type name is the whole diagnostic.
    """
    return _AUTH_HEADER.sub(r"\1<REDACTED>", f"{type(exc).__name__}: {exc}")
