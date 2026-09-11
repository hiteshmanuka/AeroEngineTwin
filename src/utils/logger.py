from pathlib import Path
import os
import logging
from logging import Logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"

logDir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(logDir, exist_ok=True)

def configLogger(loggerName: str) -> Logger:

    try:
        rel_path = os.path.relpath(loggerName, PROJECT_ROOT)
        loggerName = rel_path.replace(os.sep, ".").removesuffix(".py")

    except Exception:
        loggerName = os.path.basename(loggerName).removesuffix(".py")

    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if the logger is imported multiple times
    if logger.hasHandlers():
        return logger

    logFormat = logging.Formatter(
        "[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s"
    )

    filePath = os.path.join(logDir, "application.log")
    
    fileHandler = logging.FileHandler(
        filePath
    )
    fileHandler.setLevel(logging.DEBUG)
    fileHandler.setFormatter(logFormat)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)
    streamHandler.setFormatter(logFormat)

    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)

    return logger