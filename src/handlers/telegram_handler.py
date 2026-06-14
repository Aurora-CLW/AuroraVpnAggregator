"""
Telegram 频道处理器
"""

import logging
import re
from typing import List, Optional
from datetime import datetime

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)


class TelegramHandler(BaseHandler):
    """Telegram 频道处理器"""

    # 节点 URL 匹配模式
    NODE_PATTERNS = [
        r'(vmess://[A-Za-z0-9+/=]+)',
        r'(vless://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(trojan://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(ss://[A-Za-z0-9+/=\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(ssr://[A-Za-z0-9+/=]+)',
    ]

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_id = config.get("api_id")
        self.api_hash = config.get("api_hash")
        self.channels = config.get("channels", [])
        self.max_messages = config.get("max_messages", 100)
        self.parser = Parser()
        self.client = None

    async def fetch(self) -> List[Node]:
        """从 Telegram 频道抓取节点"""
        if not self.enabled:
            return []

        if not self.api_id or not self.api_hash:
            logger.warning(f"[{self.name}] Telegram API 未配置")
            return []

        try:
            await self._init_client()
            all_nodes = []

            for channel_config in self.channels:
                if not channel_config.get("enabled", True):
                    continue

                nodes = await self._fetch_channel(channel_config)
                all_nodes.extend(nodes)

            await self._close_client()

            logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
            return all_nodes

        except Exception as e:
            logger.error(f"[{self.name}] Telegram 抓取失败: {e}")
            return []

    async def _init_client(self):
        """初始化 Telegram 客户端"""
        try:
            from telethon import TelegramClient
            from pathlib import Path

            # 确保 session 目录存在
            session_path = Path("data/cache/telegram_session")
            session_path.parent.mkdir(parents=True, exist_ok=True)

            self.client = TelegramClient(
                str(session_path),
                self.api_id,
                self.api_hash
            )
            await self.client.start()
            logger.info("Telegram 客户端初始化成功")

        except ImportError:
            logger.error("telethon 未安装，请运行: pip install telethon")
            raise
        except Exception as e:
            logger.error(f"Telegram 客户端初始化失败: {e}")
            raise

    async def _close_client(self):
        """关闭客户端"""
        if self.client:
            await self.client.disconnect()
            self.client = None

    async def _fetch_channel(self, channel_config: dict) -> List[Node]:
        """抓取单个频道"""
        channel_id = channel_config.get("channel_id")
        max_messages = channel_config.get("max_messages", self.max_messages)
        keywords = channel_config.get("keywords", [])
        channel_name = channel_config.get("name", str(channel_id))

        logger.info(f"抓取频道: {channel_name}")

        nodes = []
        try:
            async for message in self.client.iter_messages(
                channel_id,
                limit=max_messages
            ):
                if message.text:
                    extracted = self._extract_nodes(message.text, keywords)
                    nodes.extend(extracted)

        except Exception as e:
            logger.error(f"抓取频道 {channel_name} 失败: {e}")

        # 标记来源
        self.mark_source(nodes, f"telegram:{channel_name}")

        logger.info(f"频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes

    def _extract_nodes(self, text: str, keywords: List[str] = None) -> List[Node]:
        """从文本中提取节点"""
        nodes = []

        for pattern in self.NODE_PATTERNS:
            matches = re.findall(pattern, text)

            for match in matches:
                # 关键词过滤
                if keywords and not any(kw in match for kw in keywords):
                    continue

                try:
                    node = self.parser._parse_node_url(match)
                    if node:
                        nodes.append(node)
                except Exception as e:
                    logger.debug(f"解析节点失败: {e}")

        return nodes
