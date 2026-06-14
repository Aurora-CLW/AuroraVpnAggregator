"""
订阅源处理器
"""

from .base import BaseHandler
from .github_handler import GitHubHandler
from .local_handler import LocalHandler
from .telegram_handler import TelegramHandler

# 处理器注册表
HANDLERS = {
    "github": GitHubHandler,
    "local": LocalHandler,
    "telegram": TelegramHandler,
}


def get_handler(source_type: str, config: dict) -> BaseHandler:
    """
    获取处理器实例

    Args:
        source_type: 订阅源类型
        config: 处理器配置

    Returns:
        处理器实例
    """
    handler_class = HANDLERS.get(source_type)
    if handler_class:
        return handler_class(config)
    raise ValueError(f"未知的订阅源类型: {source_type}")


__all__ = [
    "BaseHandler",
    "GitHubHandler",
    "LocalHandler",
    "TelegramHandler",
    "HANDLERS",
    "get_handler",
]
