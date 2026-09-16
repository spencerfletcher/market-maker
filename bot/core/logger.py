"""
bot/logger.py
─────────────
Core console + file logging infrastructure shared by every module.

Provides:
  - get_logger(name) — coloured-stdout + bot.log file handler + WARNING/ERROR
    Discord webhook handler
  - print_scan_summary() — end-of-scan banner

Discord push embeds (alert-style) live in bot/alerts.py.
Structured arb/trade file logs (trades.log, dry_arb.log, current_arbs.log,
arbs.log) live in bot/arb_logs.py.
"""
import logging
import os
import re
import sys
import time
from datetime import datetime

# ⛔ The webhook poster is imported INSIDE emit(), not here [RAM audit 2026-08-12 D2]: the
# import chain (requests + urllib3 + charset tables) costs ~34 MB of resident heap PER
# PROCESS, and six collectors paid it at startup whether they ever warned or not — on a
# 1.9 GB box that was ~2 dozen MB × the fleet for a handler most processes never fire.
# (The poster moved from `discord_webhook` into bot.core.alerts on 2026-09-04; the chain
# it pulls is the same `requests` one, so the deferral still pays.)
# The import now lands on the FIRST WARNING with a webhook configured (sys.modules caches
# every later one), inside emit's existing swallow-everything try.

from bot.core import config


# ANSI colour codes
_RESET = "\033[0m"
_BOLD = "\033[1m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_CYAN = "\033[36m"
_GREY = "\033[90m"

_LEVEL_COLOURS = {
    "DEBUG":    _GREY,
    "INFO":     _CYAN,
    "WARNING":  _YELLOW,
    "ERROR":    _RED,
    "CRITICAL": _RED + _BOLD,
}


class _ColouredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        colour = _LEVEL_COLOURS.get(record.levelname, _RESET)
        ts = datetime.now().strftime("%H:%M:%S")
        level = f"{colour}{record.levelname:8s}{_RESET}"
        name = f"{_GREY}{record.name}{_RESET}"
        return f"{_GREY}{ts}{_RESET} {level} {name} — {record.getMessage()}"


class _DiscordWebhookHandler(logging.Handler):
    """Sends WARNING, ERROR, and CRITICAL logs to Discord.

    Dedups identical messages within a short window so a per-tick warning burst
    (e.g. a paused loop) can't flood the webhook and trip Discord's 429.

    ⚠️ ONE log.warning() USED TO BE ABLE TO FREEZE THE WHOLE BOT. `emit` runs synchronously on
    whatever thread logged — which here is the event loop — and posted with `DiscordWebhook`'s
    default timeout of None, which `requests` reads as *wait forever*: no connect timeout, no read
    timeout. Proven 2026-07-16 against a black-hole socket (accepts TCP, never replies): a task
    ticking 19x/0.2s emitted one warning and never ticked again, still blocked when killed at 25s.
    The failure is self-amplifying — the feed-freeze *alarm* freezes both feeds, and the
    `websockets` library then cannot answer server pings from the same loop, so the sockets drop
    and the reconnect code can never run. Nothing recovers it: the unit has `Restart=always` but no
    `WatchdogSec=`, so a blocked-but-alive process reports `active (running)` forever.

    The dedup does NOT help — it is checked before the post, and the FIRST post is what hangs.

    Now: bounded by a timeout AND dispatched off the loop (see alerts._dispatch). An alert about
    trouble must not be able to cause more of it than the trouble.
    """

    _last_sent: dict[str, float] = {}
    _DEDUP_WINDOW = 30.0  # seconds

    def emit(self, record: logging.LogRecord) -> None:
        if not config.DISCORD_WEBHOOK_URL:
            return
        if config.DRY_RUN and not config.DISCORD_NOTIFY_DRY_RUN:
            return
        try:
            msg = self.format(record)
            now = time.time()
            last = _DiscordWebhookHandler._last_sent.get(msg)
            if last is not None and now - last < _DiscordWebhookHandler._DEDUP_WINDOW:
                return
            if len(_DiscordWebhookHandler._last_sent) > 500:
                _DiscordWebhookHandler._last_sent.clear()
            _DiscordWebhookHandler._last_sent[msg] = now
            color = "ff0000" if record.levelno >= logging.ERROR else "ffaa00"
            # Imported here, not at module scope: bot.core.alerts imports config, and a top-level
            # import would close a cycle through this module's own logger.
            from bot.core.alerts import (_DISCORD_TIMEOUT_S, DiscordEmbed, DiscordWebhook,
                                         _dispatch)
            webhook = DiscordWebhook(url=config.DISCORD_WEBHOOK_URL,
                                     timeout=_DISCORD_TIMEOUT_S)
            embed = DiscordEmbed(
                title=f"⚠️ Bot Alert: {record.levelname}",
                description=f"```\n{msg}\n```",
                color=color,
            )
            webhook.add_embed(embed)
            _dispatch(webhook)
        except Exception:
            # Drop silently to prevent infinite logging loops
            pass


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger with coloured console + file + Discord handlers."""
    log = logging.getLogger(name)
    if not log.handlers:
        # Force utf-8 for Windows command prompt compatibility
        if sys.stdout.encoding.lower() != "utf-8":
            handler = logging.StreamHandler(sys.stdout.reconfigure(encoding="utf-8"))
        else:
            handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_ColouredFormatter())
        log.addHandler(handler)

        # No file handler: every record already goes to stdout, which systemd captures to
        # logs/bot.stdout.log. A separate bot.log was pure double-logging (and landed in the
        # repo root). Console + Discord only here; get_file_logger files are separate.

        # Never attach the Discord webhook handler under pytest — otherwise tests
        # that exercise error/warning paths would fire real alerts to the channel.
        if config.DISCORD_WEBHOOK_URL and "pytest" not in sys.modules:
            discord_handler = _DiscordWebhookHandler()
            discord_handler.setLevel(logging.WARNING)
            discord_handler.setFormatter(logging.Formatter("%(message)s"))
            log.addHandler(discord_handler)

    log.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    return log


_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


class _StripAnsiFormatter(logging.Formatter):
    """File formatter that strips ANSI colour codes (console-only escapes)."""

    def format(self, record: logging.LogRecord) -> str:
        return _ANSI_RE.sub("", super().format(record))


def get_file_logger(name: str, filename: str) -> logging.Logger:
    """Return a logger that writes ONLY to logs/<filename> — no console, no Discord.

    Used for high-frequency diagnostics (e.g. Kalshi TIGHTEST) that would flood
    the console during a live game. propagate=False keeps it off the root/console
    handlers; ANSI colour codes from the call site are stripped for clean files.
    """
    log = logging.getLogger(name)
    if not log.handlers:
        os.makedirs("logs", exist_ok=True)
        # delay=True: the file is opened on the FIRST RECORD, not at import. These loggers are
        # module-level singletons (`bot/runner/common.py`), so without it merely importing the
        # runner creates three files in `logs/` — the LIVE tape tree on the box, and a tree the
        # test suite must not touch at all.
        handler = logging.FileHandler(os.path.join("logs", filename), encoding="utf-8",
                                      delay=True)
        handler.setFormatter(
            _StripAnsiFormatter("%(asctime)s %(levelname)-8s — %(message)s")
        )
        log.addHandler(handler)
        log.propagate = False
    log.setLevel(logging.INFO)
    return log


def print_scan_summary(
    scan_num: int,
    markets_found: int,
    opps_found: int,
    trades_executed: int,
) -> None:
    """Print a concise banner at the end of each scan cycle."""
    status = (
        f"{_GREEN}✅ {trades_executed} trade(s) executed{_RESET}"
        if trades_executed
        else f"{_GREY}— no arb found{_RESET}"
    )
    print(
        f"\n{_BOLD}─── Scan #{scan_num:04d} ───{_RESET}  "
        f"markets={markets_found}  opportunities={opps_found}  {status}\n"
    )
