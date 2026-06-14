"""
日志系统
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "aurora",
    level: str = "INFO",
    log_file: Optional[str] = None,
    rotation: str = "1 day",
) -> logging.Logger:
    """
    配置日志器

    Args:
        name: 日志器名称
        level: 日志级别 (DEBUG/INFO/WARNING/ERROR)
        log_file: 日志文件路径
        rotation: 日志轮转周期

    Returns:
        配置好的日志器
    """
    try:
        import colorlog
        use_color = True
    except ImportError:
        use_color = False

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 清除现有处理器
    logger.handlers.clear()

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    if use_color:
        console_format = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    else:
        console_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # 文件处理器
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "aurora") -> logging.Logger:
    """
    获取日志器

    Args:
        name: 日志器名称

    Returns:
        日志器实例
    """
    return logging.getLogger(name)


# 默认日志器
logger = setup_logger()
