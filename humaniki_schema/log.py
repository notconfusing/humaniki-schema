import airbrake
from cloghandler import ConcurrentRotatingFileHandler
# pipenv install ConcurrentLogHandler
import os, sys
import logging

from humaniki_schema.utils import get_ancestor_directory_that_has_xdir_as_child


def get_logger(ENV=None, BASE_DIR=None):
    if ENV is None:
        ENV = os.getenv('HUMANIKI_ENV', 'development')
    if BASE_DIR is None:
        BASE_DIR = get_ancestor_directory_that_has_xdir_as_child(xdir='logs', caller__file__=__file__)
    # use Airbrake in production
    if ENV == "production":
        log = airbrake.getLogger()
        log.setLevel(logging.INFO)
    else:
        log = logging.getLogger(__name__)
        log.setLevel(logging.DEBUG)

    # Return the logger as-is if it has already been initialized
    handlers = [h for h in log.handlers if type(h) != airbrake.AirbrakeHandler]
    if len(handlers) > 0:
        return log

    # print all debug and higher to STDOUT
    # if the environment is development
    if (ENV == "development"):
        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setLevel(logging.DEBUG)
        log.addHandler(stdoutHandler)

    logfile = os.path.abspath(os.path.join(BASE_DIR, "logs", f"humaniki_{ENV}.log"))
    print(f"Logging to {logfile}")
    formatter = logging.Formatter('%(asctime)s - %(name)s({env}) - %(levelname)s - %(message)s'.format(env=ENV))

    rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 32 * 1000 * 1024, 5)
    rotateHandler.setLevel(logging.DEBUG)
    rotateHandler.setFormatter(formatter)
    log.addHandler(rotateHandler)
    return log