import logging

from chaos.helpers.logging.config import LoggerConfig
from chaos.helpers.logging.handlers import ConsoleHandler


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

    def get(self, logger=LoggerConfig):
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
