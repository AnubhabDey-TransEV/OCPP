import logging
from logging.config import dictConfig


def get_logging_config():
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": "DEBUG",
            },
        },
        "loggers": {
            "uvicorn": {
                "level": "DEBUG",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn.error": {
                "level": "DEBUG",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "DEBUG",
                "handlers": ["console"],
                "propagate": False,
            },
            "fastapi": {
                "level": "DEBUG",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }


def setup_logging():
    print("Applying logging configuration...")
    dictConfig(get_logging_config())
    print("Logging configuration applied.")
