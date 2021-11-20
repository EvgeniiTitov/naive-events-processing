import logging
import sys

from config import Config


logging.basicConfig(
    format="'%(asctime)s %(msecs)03d %(levelname)s "
    "- %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.DEBUG if Config.DEBUG else logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


class LoggerMixin:
    @property
    def logger(self) -> logging.Logger:
        name = ".".join([__name__, self.__class__.__name__])
        return logging.getLogger(name)
