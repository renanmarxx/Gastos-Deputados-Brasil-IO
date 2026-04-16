import logging
from dataclasses import dataclass


@dataclass
class LoggingConfig:
    name: str = "chaos-logger"
    format: str = (
        "[%(asctime)s] {{%(filename)s:%(funcName)s:%(lineno)d}} %(levelname)s - %(message)s"
    )
    level: logging = logging.DEBUG
