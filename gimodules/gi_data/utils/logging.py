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
