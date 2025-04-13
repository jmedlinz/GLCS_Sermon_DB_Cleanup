import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime

from rich.console import Console
from rich.theme import Theme

from config import APP_ENVIRONMENT, DATA_DIR

USE_UNIQUE_LOG_FILENAME = False

# Either use one log file that is appended to for all runs,
# or use a log file with a unique date/time-based filename for each run.
SUB_LOG_FILE_NAME = ""
# ! Haven't found a way to test this with unit tests, so added "pragma: no cover" to bypass it for now.
if USE_UNIQUE_LOG_FILENAME:  # pragma: no cover
    SUB_LOG_FILE_NAME = "." + datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE_NAME = f"data_processing.{APP_ENVIRONMENT}{SUB_LOG_FILE_NAME}.log"
LOG_FILE = os.path.join(DATA_DIR, LOG_FILE_NAME)

NEW_JOB_RUN_SEPARATOR = "#" * 79
DIVIDER = "-" * 55


# List of colors: https://rich.readthedocs.io/en/stable/appendix/colors.html
CUSTOM_THEME = Theme(
    {
        "header": "bold italic dark_slate_gray1",
        "info": "deep_sky_blue1",
        "ok": "chartreuse3",
        "warning": "yellow1",
        "comment": "grey82",
        "error": "underline bold red",
        "success": "underline bold chartreuse3",
        "failure": "underline bold red",
    }
)

#  Global variables
console = Console(theme=CUSTOM_THEME)
warnings = 0
errors = 0
email_log = []


def _clear_status():
    global warnings, errors
    warnings = 0
    errors = 0


def add_to_log(message=""):
    logging.info(message)


def get_email_log():
    return email_log


def get_warning_count():
    return warnings


def get_error_count():
    return errors


def _add_to_email_log(message=""):
    email_log.append(message)


def _report(prepend="", message="", theme="info", log_only=False):
    message = prepend + str(message)
    add_to_log(message)
    if not log_only:
        _add_to_email_log(" " + message)
        console.print(message, style=theme)


def _header(message="", log_message=""):
    if not log_message:
        log_message = message
    add_to_log(log_message)
    _add_to_email_log(log_message)
    console.print(message, style="header", highlight=False)


def report_header(app_name="", comp_name="NA", app_env="NA", user_name="NA"):
    if app_name:
        _header(f" Application: {app_name}")
    _header(f" Session:     Running on {comp_name} in {app_env} mode as the {user_name} account")
    report_divider()


def report_section(message=""):
    _report(" ", message)


def report_subsection(message="", log_only=False):
    _report("    ", message, log_only=log_only)


def report_info(message="", log_only=False):
    _report("    - ", message, "ok", log_only)


def report_comment(message="", log_only=False):
    _report("    - ", message, "comment", log_only)


def report_error(message=""):
    global errors
    errors += 1
    _report("* ", message, "error")


def report_error_continue(message=""):
    _report("       * ", message, "error")


def report_warning(message=""):
    global warnings
    warnings += 1
    _report("    ! ", message, "warning")


def report_warning_continue(message=""):
    _report("    !    ", message, "warning")


def _add_s(message="", count=0, verb=True):
    """
    Add an 's' to the end of a word if count is not 1.
    Return was/were too unless verb is False.
    Example:
        _add_s("error", 1) returns "1 error was"
        _add_s("error", 2) returns "2 errors were"
        _add_s("error", 1, verb=False) returns "1 error"
        _add_s("warning", 2, verb=False) returns "2 warnings"
    """

    verb_message = "was"
    if count != 1:
        message += "s"
        verb_message = "were"

    if verb:
        return f"{count} {message} {verb_message}"
    else:
        return f"{count} {message}"


def _report_warning_status():
    report_blank()
    warn_cnt = get_warning_count()
    message = f"*** {_add_s('warning', warn_cnt)} found ***"
    _header(f"[warning]{message}[/]", message)
    report_blank()


def _report_failure_status():
    report_blank()
    warn_cnt = get_warning_count()
    err_cnt = get_error_count()
    if warn_cnt:
        message = f"*** {_add_s('error', err_cnt, verb=False)} and {_add_s('warning', warn_cnt)} found ***"
    else:
        message = f"*** {_add_s('error', err_cnt)} found ***"
    _header(f"[error]{message}[/]", message)
    report_blank()


def _report_success_status():
    report_blank()
    message = "Success!"
    _header(f"Status: [success]{message}[/]", f"Status: {message}")
    report_blank()


def report_status():
    if errors:
        _report_failure_status()
    elif warnings:
        _report_warning_status()
    else:
        _report_success_status()


def report_blank():
    _report("", "")


def _report_exception(message=""):
    _report("*** ", message, "error")


def report_exception(message, exception):
    global errors
    errors += 1
    report_blank()
    logging.exception(exception)
    add_to_log("")
    _report_exception(message)
    if exception.args:
        if len(exception.args) > 1:
            _report_exception(exception.args[1])
        else:
            _report_exception(exception.args[0])
    _report_exception("See the log file '" + LOG_FILE + "' for more information")


def report_divider():
    _header(DIVIDER)


def _new_task_header():
    now = datetime.now().strftime("%a, %B %d, %Y %I:%M:%S%p")
    add_to_log()
    _header(NEW_JOB_RUN_SEPARATOR)
    _header(" " + now)

    versions = []

    # Helper function to get version info safely
    def get_version(command, name):
        # Skip if command not found in PATH
        if not shutil.which(command):
            return None

        try:
            result = subprocess.run(
                [command, "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                shell=False,  # More secure than shell=True
            )
            if result.returncode == 0:
                return result.stdout.strip().title()
            return None
        except Exception:  # pragma: no cover
            return None

    # Get Python version directly from sys module instead of subprocess
    python_version = f"Python {sys.version.split()[0]}"
    versions.append(python_version)

    # Check other tools
    for cmd, name in [("pyenv", "Pyenv"), ("poetry", "Poetry")]:
        version = get_version(cmd, name)
        if version:
            versions.append(version)

    # Add versions to log
    add_to_log(" " + ", ".join(versions))
    add_to_log(DIVIDER)


def clear_log_file(filename=LOG_FILE):
    """Set everything back to it's beginning status."""

    global email_log

    _clear_status()
    email_log = []

    # Wipe the log file.  This is useful for testing.
    # Make sure we're not doing this to the 'usual' data_processing.log file though.
    if filename != LOG_FILE:
        open(filename, "w").close()

    # Initiate the log file to the format I want.  When testing, this isn't done when it's initialized so force it here.
    logging.basicConfig(filename=filename, level=logging.DEBUG, force=True)

    # Start the logging with a header.
    _new_task_header()


def initiate_logging():
    """Initiate the log file"""

    global email_log

    # Set the logging level for some libraries to WARNING.
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _clear_status()

    # Clear the email log
    email_log = []

    # Initiate the log file
    logging.basicConfig(
        filename=LOG_FILE,
        # Set the level to INFO instead of DEBUG to avoid the log file getting too big due to the FTP transfers.
        level=logging.INFO,
        # For debugging, consider adding the file path and line number to the format:
        #    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s: %(message)s",
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Start the logging with a header.
    _new_task_header()


# Initiate the log file
initiate_logging()
