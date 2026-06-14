"""
订阅抓取器
"""

import asyncio
import aiohttp
from typing import List, Optional
from pathlib import Path
import logging

from ..models.node import Node, Source
from .parser import Parser

logger = logging.getLogger(__name__)


class Fetcher:
    """订阅源抓取器"""

    def __init__(self, timeout: int = 30, retry: int = 3):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retry = retry
        self.parser = Parser()

    async def fetch_url(self, url: str, source_name: str = "") -> Optional[str]:
        """
        抓取远程订阅内容

        Args:
            url: 订阅地址
            source_name: 来源名称（用于日志）

        Returns:
            订阅内容字符串，失败返回 None
        """
        for attempt in range(self.retry):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            content = await response.text()
                            logger.info(f"[{source_name}] 抓取成功: {len(content)} 字节")
                            return content
                        else:
                            logger.warning(f"[{source_name}] HTTP {response.status}")
            except asyncio.TimeoutError:
                logger.warning(f"[{source_name}] 超时，重试 {attempt + 1}/{self.retry}")
            except aiohttp.ClientError as e:
                logger.warning(f"[{source_name}] 网络错误: {e}，重试 {attempt + 1}/{self.retry}")
            except Exception as e:
                logger.error(f"[{source_name}] 未知错误: {e}")
                break

            if attempt < self.retry - 1:
                await asyncio.sleep(2 ** attempt)  # 指数退避

        return None

    def fetch_file(self, path: str, source_name: str = "") -> Optional[str]:
        """
        读取本地文件内容

        Args:
            path: 文件路径
            source_name: 来源名称

        Returns:
            文件内容字符串，失败返回 None
        """
        try:
            file_path = Path(path)
            if not file_path.exists():
                logger.warning(f"[{source_name}] 文件不存在: {path}")
                return None

            content = file_path.read_text(encoding="utf-8")
            logger.info(f"[{source_name}] 读取文件成功: {len(content)} 字节")
            return content

        except Exception as e:
            logger.error(f"[{source_name}] 读取文件失败: {e}")
            return None

    async def fetch_source(
        self,
        source: Source,
        parse: bool = True
    ) -> List[Node]:
        """
        抓取并解析订阅源

        Args:
            source: 订阅源配置
            parse: 是否解析节点

        Returns:
            节点列表
        """
        nodes = []

        if not source.enabled:
            logger.info(f"[{source.name}] 已禁用，跳过")
            return nodes

        content = None

        # 根据类型获取内容
        if source.type == "github" and source.url:
            content = await self.fetch_url(source.url, source.name)

        elif source.type == "local" and source.path:
            content = self.fetch_file(source.path, source.name)

        elif source.type == "telegram":
            # Telegram 单独处理
            from ..handlers.telegram_handler import TelegramHandler
            handler = TelegramHandler(source.__dict__)
            try:
                nodes = await handler.fetch()
                source.node_count = len(nodes)
                source.last_update = source.last_update or None
                return nodes
            except Exception as e:
                source.error = str(e)
                logger.error(f"[{source.name}] Telegram 抓取失败: {e}")
                return nodes

        # 解析内容
        if content and parse:
            try:
                nodes = self.parser.parse(content, source.format)
                for node in nodes:
                    node.source = source.name
                source.node_count = len(nodes)
                logger.info(f"[{source.name}] 解析成功: {len(nodes)} 个节点")
            except Exception as e:
                source.error = str(e)
                logger.error(f"[{source.name}] 解析失败: {e}")

        return nodes

    async def fetch_all(
        self,
        sources: List[Source],
        concurrent: int = 5
    ) -> List[Node]:
        """
        并发抓取多个订阅源

        Args:
            sources: 订阅源列表
            concurrent: 并发数

        Returns:
            所有节点列表
        """
        all_nodes = []

        semaphore = asyncio.Semaphore(concurrent)

        async def fetch_with_limit(source: Source):
            async with semaphore:
                return await self.fetch_source(source)

        tasks = [fetch_with_limit(source) for source in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for source, result in zip(sources, results):
            if isinstance(result, Exception):
                source.error = str(result)
                logger.error(f"[{source.name}] 抓取异常: {result}")
            elif isinstance(result, list):
                all_nodes.extend(result)

        logger.info(f"共抓取 {len(all_nodes)} 个节点")
        return all_nodes
