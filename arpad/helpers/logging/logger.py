import logging

from arpad.helpers.logging.config import LoggingConfig
from arpad.helpers.logging.handlers import ConsoleHandler


class Logger:
    @staticmethod
    def _set_logger(
        name: str, handlers: list[logging.Handler], level: int = logging.DEBUG
    ):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if logger.hasHandlers():
            logger.handlers.clear()
        for handler in handlers:
            logger.addHandler(handler)
        return logger

    def get(self, logger=LoggingConfig):
        logger = self._set_logger(
            name=logger.name,
            handlers=[
                ConsoleHandler().create(
                    formatter=logging.Formatter(fmt=logger.format), level=logger.level
                )
            ],
            level=logger.level,
        )
        return logger
