"""
节点去重器
"""

from typing import List
from collections import defaultdict
import logging

from ..models.node import Node

logger = logging.getLogger(__name__)


class Deduplicator:
    """节点去重器"""

    def __init__(self, method: str = "fingerprint", max_per_server: int = 10):
        """
        初始化去重器

        Args:
            method: 去重方式 (fingerprint/server_port)
            max_per_server: 相同服务器最大节点数
        """
        self.method = method
        self.max_per_server = max_per_server

    def deduplicate(self, nodes: List[Node]) -> List[Node]:
        """
        去重

        Args:
            nodes: 节点列表

        Returns:
            去重后的节点列表
        """
        if not nodes:
            return []

        if self.method == "fingerprint":
            return self._deduplicate_by_fingerprint(nodes)
        elif self.method == "server_port":
            return self._deduplicate_by_server_port(nodes)
        else:
            return self._deduplicate_by_fingerprint(nodes)

    def _deduplicate_by_fingerprint(self, nodes: List[Node]) -> List[Node]:
        """基于指纹去重"""
        seen = {}
        duplicates = 0

        for node in nodes:
            fingerprint = node.node_fingerprint or node.generate_fingerprint()
            if fingerprint not in seen:
                seen[fingerprint] = node
            else:
                duplicates += 1

        logger.info(f"去重完成: {len(nodes)} -> {len(seen)} (移除 {duplicates} 个重复)")
        return list(seen.values())

    def _deduplicate_by_server_port(self, nodes: List[Node]) -> List[Node]:
        """基于服务器+端口去重"""
        server_nodes = defaultdict(list)

        for node in nodes:
            key = f"{node.server}:{node.port}"
            server_nodes[key].append(node)

        result = []
        for key, node_list in server_nodes.items():
            # 每个服务器最多保留 max_per_server 个节点
            sorted_nodes = sorted(node_list, key=lambda n: n.latency or 9999)
            result.extend(sorted_nodes[:self.max_per_server])

        logger.info(f"去重完成: {len(nodes)} -> {len(result)}")
        return result

    def remove_invalid(self, nodes: List[Node]) -> List[Node]:
        """
        移除无效节点

        Args:
            nodes: 节点列表

        Returns:
            有效节点列表
        """
        valid = [n for n in nodes if n.is_valid]
        invalid_count = len(nodes) - len(valid)
        logger.info(f"过滤无效节点: {len(nodes)} -> {len(valid)} (移除 {invalid_count} 个)")
        return valid

    def filter_by_country(
        self,
        nodes: List[Node],
        include: List[str] = None,
        exclude: List[str] = None
    ) -> List[Node]:
        """
        按国家过滤节点

        Args:
            nodes: 节点列表
            include: 只包含的国家列表
            exclude: 排除的国家列表

        Returns:
            过滤后的节点列表
        """
        result = nodes

        if include:
            result = [n for n in result if n.country and n.country in include]

        if exclude:
            result = [n for n in result if not n.country or n.country not in exclude]

        return result

    def filter_by_keywords(self, nodes: List[Node], keywords: List[str]) -> List[Node]:
        """
        按关键词过滤节点名称

        Args:
            nodes: 节点列表
            keywords: 排除的关键词列表

        Returns:
            过滤后的节点列表
        """
        if not keywords:
            return nodes

        result = []
        for node in nodes:
            exclude = False
            for kw in keywords:
                if kw.lower() in node.name.lower():
                    exclude = True
                    break
            if not exclude:
                result.append(node)

        return result

    def limit_nodes(self, nodes: List[Node], max_nodes: int = 0) -> List[Node]:
        """
        限制节点数量

        Args:
            nodes: 节点列表
            max_nodes: 最大节点数（0 为不限制）

        Returns:
            限制后的节点列表
        """
        if max_nodes <= 0 or len(nodes) <= max_nodes:
            return nodes

        # 按延迟排序，保留延迟最低的节点
        sorted_nodes = sorted(nodes, key=lambda n: n.latency or 9999)
        return sorted_nodes[:max_nodes]
