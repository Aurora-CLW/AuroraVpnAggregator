"""
节点测试器
"""

import asyncio
import logging
from typing import List, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from ..models.node import Node
from ..utils.network import check_tcp_port, measure_latency

logger = logging.getLogger(__name__)


class NodeTester:
    """节点测试器"""

    def __init__(self, config: dict):
        """
        初始化测试器

        Args:
            config: 测试配置
        """
        self.tcp_enabled = config.get("tcp", {}).get("enabled", True)
        self.tcp_timeout = config.get("tcp", {}).get("timeout", 3)
        self.tcp_concurrent = config.get("tcp", {}).get("concurrent", 100)

        self.http_enabled = config.get("http", {}).get("enabled", True)
        self.http_timeout = config.get("http", {}).get("timeout", 10)
        self.http_concurrent = config.get("http", {}).get("concurrent", 20)
        self.test_url = config.get("http", {}).get(
            "test_url",
            "http://www.gstatic.com/generate_204"
        )

        self.max_latency = config.get("max_latency", 5000)

    async def test_all(self, nodes: List[Node]) -> List[Node]:
        """
        测试所有节点

        Args:
            nodes: 待测试节点列表

        Returns:
            有效节点列表
        """
        if not nodes:
            return []

        logger.info(f"开始测试 {len(nodes)} 个节点...")

        # Stage 1: TCP 可达性测试
        if self.tcp_enabled:
            tcp_valid = await self._tcp_batch_test(nodes)
            logger.info(f"TCP 测试完成: {len(tcp_valid)}/{len(nodes)} 可达")
        else:
            tcp_valid = nodes

        # Stage 2: HTTP 代理测试
        if self.http_enabled:
            http_valid = await self._http_batch_test(tcp_valid)
            logger.info(f"HTTP 测试完成: {len(http_valid)}/{len(tcp_valid)} 有效")
        else:
            http_valid = tcp_valid

        # 统计
        for node in http_valid:
            node.is_valid = True

        logger.info(f"测试完成: {len(http_valid)} 个有效节点")
        return http_valid

    async def _tcp_batch_test(self, nodes: List[Node]) -> List[Node]:
        """批量 TCP 测试"""
        semaphore = asyncio.Semaphore(self.tcp_concurrent)
        valid_nodes = []

        async def test_with_semaphore(node: Node):
            async with semaphore:
                is_valid = await check_tcp_port(node.server, node.port, self.tcp_timeout)
                node.tcp_valid = is_valid
                if is_valid:
                    # 测量延迟
                    node.latency = await measure_latency(node.server, node.port, self.tcp_timeout)
                return node, is_valid

        tasks = [test_with_semaphore(node) for node in nodes]
        results = await asyncio.gather(*tasks)

        for node, is_valid in results:
            if is_valid:
                valid_nodes.append(node)
            node.tested_at = datetime.now()

        return valid_nodes

    async def _http_batch_test(self, nodes: List[Node]) -> List[Node]:
        """批量 HTTP 代理测试"""
        # 注意: 完整的代理测试需要根据节点类型构建代理客户端
        # 这里简化处理，仅检查延迟是否在阈值内
        valid_nodes = []

        for node in nodes:
            # 简化: 如果 TCP 延迟在阈值内，视为有效
            if node.latency > 0 and node.latency < self.max_latency:
                valid_nodes.append(node)
            elif node.latency == 0:
                # 如果延迟为0，重新测量
                node.latency = await measure_latency(node.server, node.port, self.http_timeout)
                if node.latency > 0 and node.latency < self.max_latency:
                    valid_nodes.append(node)

        return valid_nodes

    async def test_node(self, node: Node) -> bool:
        """
        测试单个节点

        Args:
            node: 节点对象

        Returns:
            是否有效
        """
        # TCP 测试
        is_tcp_valid = await check_tcp_port(node.server, node.port, self.tcp_timeout)
        node.tcp_valid = is_tcp_valid

        if not is_tcp_valid:
            return False

        # 测量延迟
        node.latency = await measure_latency(node.server, node.port, self.tcp_timeout)
        node.tested_at = datetime.now()

        # 检查延迟阈值
        if node.latency > 0 and node.latency < self.max_latency:
            node.is_valid = True
            return True

        return False


async def test_nodes(nodes: List[Node], config: dict = None) -> List[Node]:
    """
    测试节点的便捷函数

    Args:
        nodes: 节点列表
        config: 测试配置

    Returns:
        有效节点列表
    """
    if config is None:
        config = {
            "tcp": {"enabled": True, "timeout": 3, "concurrent": 100},
            "http": {"enabled": True, "timeout": 10, "concurrent": 20},
            "max_latency": 5000,
        }

    tester = NodeTester(config)
    return await tester.test_all(nodes)
