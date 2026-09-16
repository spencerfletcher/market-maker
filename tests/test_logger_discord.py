"""Pins for the logger's Discord handler plumbing."""
import subprocess
import sys


def test_importing_the_logger_does_not_import_the_posting_chain():
    """[RAM audit 2026-08-12 D2] the posting chain costs ~34MB resident PER PROCESS and six
    collectors paid it at startup for a handler most never fire. The import is deferred to the
    first emitted WARNING; a revert to a module-top import goes RED here. The sentinel is
    `requests` — since 2026-09-04 the poster is bot.core.alerts, which imports requests at module
    scope, so logger must not import alerts eagerly (`discord_webhook` is no longer a dependency
    and the old sentinel could never fail).
    Subprocess, because this test's own interpreter has sys.modules polluted by the suite."""
    code = ("import sys; import bot.core.logger; "
            "sys.exit(1 if 'requests' in sys.modules else 0)")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True)
    assert r.returncode == 0, "importing the logger eagerly imported the requests posting chain"
