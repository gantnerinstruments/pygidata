import logging


def setup_module_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger for the given module name.
    Adds a StreamHandler with a simple formatter if no handlers are present.

    Args:
        name: The module name (usually __name__).
        level: Logging level (default: INFO).

    Returns:
        Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def set_global_log_level(level: int):
    """
    Change level of all configured loggers (including existing module loggers).
    """
    logging.getLogger().setLevel(level)  # root logger
    for name, logger in logging.root.manager.loggerDict.items():
        if isinstance(logger, logging.Logger):
            logger.setLevel(level)
