"""
订阅源处理器基类
"""

from abc import ABC, abstractmethod
from typing import List
import logging

from ..models.node import Node

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    """订阅源处理器基类"""

    def __init__(self, config: dict):
        """
        初始化处理器

        Args:
            config: 处理器配置
        """
        self.config = config
        self.enabled = config.get("enabled", True)
        self.name = config.get("name", "Unknown")

    @abstractmethod
    async def fetch(self) -> List[Node]:
        """
        抓取节点

        Returns:
            节点列表
        """
        pass

    def mark_source(self, nodes: List[Node], source_name: str = None) -> List[Node]:
        """
        标记节点来源

        Args:
            nodes: 节点列表
            source_name: 来源名称

        Returns:
            标记后的节点列表
        """
        source = source_name or self.name
        for node in nodes:
            node.source = source
            if not node.node_fingerprint:
                node.node_fingerprint = node.generate_fingerprint()
        return nodes
