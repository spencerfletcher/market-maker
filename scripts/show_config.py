#!/usr/bin/env python3
"""
scripts/show_config.py — print the bot's RESOLVED config (post-.env override) for the
NON-SECRET operational knobs only. Read-only; it never prints credentials.

Why this exists instead of reading .env directly: .env holds the venue API keys, the Poly US
secret, and the wallet PRIVATE_KEY. Verifying a knob like KALSHI_CONFIRM_SECONDS must never put
those in anyone's context (a transcript persists; "don't print secrets" is unenforceable once a
value is in context). So this imports bot.core.config (which loads .env) and prints ONLY the
explicitly-whitelisted attributes in SAFE_GROUPS — a WHITELIST, so a *new* secret added to .env
can never leak here. `_safe_str` is a second line of defense: any value that looks secret-shaped
is redacted even if a name is mis-whitelisted. `Read(.env)` stays denied in settings.json.

⚠️ ONE DOCUMENTED EXCEPTION: the "Operator files" section prints `KILL_SWITCH_FILE` and goes
through NEITHER guard — not SAFE_GROUPS, not the import-time assert, not `_safe_str`. That is
deliberate, because `_SECRET_SHAPE` matches long path-shaped strings and would redact any ordinary
absolute path, which would make the section useless. The exception is narrow and safe today —
the value is a filesystem path, and `KILL_SWITCH_FILE` is not in SECRET_NAMES.
⛔ BEFORE ADDING A THIRD ENTRY, note the trap: `KALSHI_PRIVATE_KEY_PATH` is literally an operator
file *and* is in SECRET_NAMES, so it is the natural next addition and it would bypass both guards.
Adding one needs a path-aware renderer, not a `_safe_str` call.

Usage:  .venv/bin/python -m scripts.show_config
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root, for direct runs

from bot.core import config

# ── The whitelist: ONLY these (non-secret) attributes are ever printed, grouped. ──────────────
SAFE_GROUPS: dict[str, list[str]] = {
    "Mode": ["DRY_RUN", "LOG_LEVEL"],
    "Risk / exposure": [
        "DAILY_LOSS_LIMIT", "KALSHI_EXEC_COST_BUDGET",
    ],
    "Venue selection / feeds": [
        "POLY_US_FEED_SOURCE", "MAKER_BOOK_SOURCE",
        "KALSHI_ENV", "KALSHI_PRICE_SOURCE", "KALSHI_SERIES",
        "KALSHI_RESNAP_THROTTLE_SECONDS", "KALSHI_BOOK_SUSPECT_SECONDS",
    ],
    "WS loop timing diagnostics": [
        "WS_LOOP_TIMING", "WS_LOOP_TIMING_SAMPLE_N", "WS_LOOP_TIMING_FLUSH_N",
    ],
    "Operational rails (file paths)": [
        "ALERT_HEALTH_FILE", "HEARTBEAT_DIR", "MAKER_STATE_FILE",
    ],
    "Notifications (flags only — NOT the webhook URLs)": [
        "DISCORD_NOTIFY_DRY_RUN",
    ],
    "Memory guard (halt = combined avail+swap <= MIN_AVAIL, OR RAM avail <= HARD_RAM_FLOOR)": [
        "MEMGUARD_ENABLED", "MEMGUARD_WARN_RSS_MB", "MEMGUARD_HALT_RSS_MB",
        "MEMGUARD_MIN_AVAIL_MB", "MEMGUARD_WARN_AVAIL_MB", "MEMGUARD_HARD_RAM_FLOOR_MB",
        "MEMGUARD_TMPFS_WARN_MB",
    ],
}

# ── Known secrets — NEVER printed. Kept explicit so the whitelist can be ASSERTED clean. ──────
SECRET_NAMES: frozenset[str] = frozenset({
    "KALSHI_API_KEY", "KALSHI_PRIVATE_KEY_PATH",
    "POLYMARKET_US_KEY_ID", "POLYMARKET_US_SECRET_KEY",
    "DISCORD_WEBHOOK_URL", "DISCORD_FILLS_WEBHOOK_URL", "DISCORD_REPORTS_WEBHOOK_URL",
    "PUSHOVER_USER_KEY", "PUSHOVER_API_TOKEN",
})

_MISSING = object()
# Redact anything shaped like a URL, a long opaque token, a hex key, or a PEM block.
_SECRET_SHAPE = re.compile(r"https?://|-----BEGIN|0x[0-9a-fA-F]{16,}|[A-Za-z0-9+/=_-]{32,}")


def _all_safe() -> list[str]:
    return [name for group in SAFE_GROUPS.values() for name in group]


# Fail LOUD at import if a secret ever lands on the whitelist (pinned by the test too).
assert not (set(_all_safe()) & SECRET_NAMES), \
    f"secret name(s) on the non-secret whitelist: {set(_all_safe()) & SECRET_NAMES}"


def _safe_str(name: str, value: object) -> str:
    """Render a value, redacting anything secret-shaped (defense in depth)."""
    if value is _MISSING:
        return "<not in config>"
    s = str(value)
    if _SECRET_SHAPE.search(s):
        return "<redacted: value looks secret-shaped>"
    return s


def _operator_files() -> list[tuple[str, str, bool]]:
    """The operator control files, RESOLVED — `(label, absolute path, exists)`.

    ⛔ This section exists because a runbook that spells a path by hand gets it wrong in a direction
    that reads as SAFE: an operator who reads the wrong state file sees "clean" and believes it.
    Read the constants rather than re-spelling them, so a doc cannot drift from the code.

    ⚠️ ASYMMETRY, AND IT MATTERS. The durable state paths are absolute at import and therefore the
    same for every process. The kill-switch path is NOT: `KILL_SWITCH_FILE` is a relative default,
    so `abspath` resolves it against **whatever cwd `show_config` was run from**, which need not be
    the cwd of the process the operator is trying to halt. Printing it alone would recreate, on the
    more dangerous control, exactly the "reads as safe, names the wrong file" shape this section
    exists to kill. `main()` therefore prints the RAW configured value and a cwd caveat beside it
    whenever the value is relative — do not drop that."""
    kill_switch = config.KILL_SWITCH_FILE
    out: list[tuple[str, str, bool]] = [
        ("Durable maker state", config.MAKER_STATE_FILE, os.path.exists(config.MAKER_STATE_FILE)),
    ]
    if kill_switch:
        resolved = os.path.abspath(kill_switch)
        out.append(("Kill switch (pause file)", resolved, os.path.exists(resolved)))
    else:
        # An empty KILL_SWITCH_FILE makes `is_paused()` a constant False with no startup warning —
        # the disabled state is otherwise indistinguishable from the armed one.
        out.append(("Kill switch (pause file)", "⛔ DISABLED (KILL_SWITCH_FILE is empty)", False))
    return out


def main() -> None:
    print("Resolved config (post-.env) — non-secret operational knobs only.")
    print("Secrets (PRIVATE_KEY, *_API_KEY, *_SECRET_KEY, webhook URLs, …) are never shown.\n")
    for group, names in SAFE_GROUPS.items():
        print(f"── {group} " + "─" * max(0, 60 - len(group)))
        for name in names:
            print(f"  {name:34} = {_safe_str(name, getattr(config, name, _MISSING))}")
        print()

    print("── Operator files (resolved absolute paths) " + "─" * 18)
    for label, value, exists in _operator_files():
        print(f"  {label:34} = {value}")
        print(f"  {'':34}   {'EXISTS' if exists else 'absent'}")
    ks = config.KILL_SWITCH_FILE
    if ks and not os.path.isabs(ks):
        # ⛔ THE PATH ABOVE IS RESOLVED AGAINST *THIS* PROCESS'S CWD, and this process is not the one
        # being halted. A maker launched from ~ watches ~/pause.json; an operator who runs this from
        # the repo, touches the path it prints and sees EXISTS would believe a real-money run was
        # stopped while it kept quoting. Say so rather than let the reader assume.
        print(f"  ⚠️ KILL_SWITCH_FILE is RELATIVE ({ks!r}) — the line above resolves it against")
        print(f"     THIS command's cwd ({os.getcwd()}), not the cwd of the process you")
        print( "     want to halt. A maker launched by hand watches the path relative to where")
        print( "     THAT run was started, so create the file there.")
    print("  (kill switch present ⇒ the maker halts new quoting within one --requote-s and tears")
    print("   down. scripts/maker_supervisor.py does NOT read it, so it keeps RELAUNCHING makers —")
    print("   each one halts before quoting, but the supervisor looks alive.)")
    print()


if __name__ == "__main__":
    main()
