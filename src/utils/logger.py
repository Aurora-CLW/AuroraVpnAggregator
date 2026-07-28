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
    try:
        import colorlog
        use_color = True
    except ImportError:
        use_color = False

    log_level = getattr(logging, level.upper(), logging.INFO)

    # 配置根 logger 使所有模块 (src.* 等) 的日志都能输出
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()

    # 配置 aurora 命名空间
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.handlers.clear()

    # 同时配置 src 命名空间 (项目内所有模块)
    src_logger = logging.getLogger("src")
    src_logger.setLevel(log_level)
    src_logger.handlers.clear()
    src_logger.propagate = True

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

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
    root.addHandler(console_handler)

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
        root.addHandler(file_handler)

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
