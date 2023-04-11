from ploomber_core.telemetry.telemetry import Telemetry
from ploomber import __version__

POSTHOG_API_KEY = "phc_P9SpSeypyPwxrMdFn2edOOEooQioF2axppyEeDwtMSP"

telemetry = Telemetry(POSTHOG_API_KEY, package_name="ploomber", version=__version__)
