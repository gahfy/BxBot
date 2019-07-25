from enum import Enum


class LogLevel(Enum):
    NONE = -1
    VERBOSE = 0
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4


current_log_level: LogLevel = LogLevel.VERBOSE


def print_log(log_level: LogLevel, message: str):
    if log_level != LogLevel.NONE:
        colors = [
            "\033[90m",
            "\033[39m",
            "\033[32m",
            "\033[33m",
            "\033[31m"
        ]
        if log_level.value >= current_log_level.value:
            message_to_print = "%s%s%s" % (colors[log_level.value], message, '\033[39m')
            print(message_to_print)


def v(message: str):
    print_log(LogLevel.VERBOSE, message)


def d(message: str):
    print_log(LogLevel.DEBUG, message)


def i(message: str):
    print_log(LogLevel.INFO, message)


def w(message: str):
    print_log(LogLevel.WARNING, message)


def e(message: str):
    print_log(LogLevel.ERROR, message)
