"""
GitHub 订阅处理器
"""

import logging
from typing import List, Optional
import aiohttp

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)


class GitHubHandler(BaseHandler):
    """GitHub 订阅处理器"""

    def __init__(self, config: dict):
        super().__init__(config)
        self.url = config.get("url", "")
        self.format = config.get("format", "auto")
        self.priority = config.get("priority", 5)
        self.parser = Parser()
        self.timeout = 30
        self.retry = 3

    async def fetch(self) -> List[Node]:
        """抓取 GitHub 订阅"""
        if not self.enabled or not self.url:
            return []

        content = await self._fetch_url()

        if not content:
            return []

        # 解析节点
        nodes = self.parser.parse(content, self.format)

        # 标记来源
        self.mark_source(nodes)

        logger.info(f"[{self.name}] 抓取完成: {len(nodes)} 个节点")
        return nodes

    async def _fetch_url(self) -> Optional[str]:
        """抓取 URL 内容"""
        for attempt in range(self.retry):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.url,
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:
                        if response.status == 200:
                            content = await response.text()
                            logger.debug(f"[{self.name}] 抓取成功: {len(content)} 字节")
                            return content
                        else:
                            logger.warning(f"[{self.name}] HTTP {response.status}")
            except Exception as e:
                logger.warning(f"[{self.name}] 抓取失败 (尝试 {attempt + 1}/{self.retry}): {e}")
                if attempt < self.retry - 1:
                    import asyncio
                    await asyncio.sleep(2 ** attempt)

        logger.error(f"[{self.name}] 抓取失败")
        return None
