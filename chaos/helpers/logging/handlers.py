import logging


class ConsoleHandler:

    @staticmethod
    def create(formatter: logging.Formatter, level: logging = logging.DEBUG):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        return console_handler
