"""
Telegram 频道处理器
支持两种抓取方式:
1. Bot API: 通过 Bot Token 读取公开频道消息 (推荐)
2. Web Scrape: 通过 t.me/s/ 预览页抓取 (无需任何配置)
3. Telethon: 完整 Telegram 客户端 (需要 api_id/api_hash)

注意: 公开频道不需要 Bot 是管理员, Bot API 可以直接读取公开频道消息
"""

import asyncio
import logging
import re
import json
from typing import List, Optional
from datetime import datetime
from pathlib import Path

import aiohttp

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)


class TelegramHandler(BaseHandler):
    """Telegram 频道处理器"""

    NODE_PATTERNS = [
        r'(vmess://[A-Za-z0-9+/=]+)',
        r'(vless://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(trojan://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(ss://[A-Za-z0-9+/=\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(ssr://[A-Za-z0-9+/=]+)',
    ]

    def __init__(self, config: dict):
        super().__init__(config)
        self.bot_token = config.get("bot_token", "")
        self.api_id = config.get("api_id")
        self.api_hash = config.get("api_hash")
        self.channels = config.get("channels", [])
        self.max_messages = config.get("max_messages", 100)
        self.mode = config.get("mode", "bot")  # bot, web, telethon
        self.parser = Parser()
        self.client = None

    async def fetch(self) -> List[Node]:
        """从 Telegram 频道抓取节点"""
        if not self.enabled:
            return []

        all_nodes = []
        mode = self.mode

        # 自动降级: 有 bot_token 用 bot, 否则 web
        if mode == "bot" and not self.bot_token:
            logger.info(f"[{self.name}] 无 bot_token, 降级到 web 模式")
            mode = "web"

        for ch in self.channels:
            if not ch.get("enabled", True):
                continue
            try:
                if mode == "bot":
                    nodes = await self._fetch_via_bot(ch)
                elif mode == "telethon" and self.api_id and self.api_hash:
                    nodes = await self._fetch_via_telethon(ch)
                else:
                    nodes = await self._fetch_via_web(ch)
                all_nodes.extend(nodes)
            except Exception as e:
                logger.error(f"[{self.name}] 抓取频道 {ch.get('name')} 失败: {e}")

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
        return all_nodes

    # ── Bot API 模式 (推荐) ──
    # 公开频道: Bot 不需要是管理员, 可以直接用 getChat + 抓取消息
    # 但 Bot API 没有 getHistory, 所以用 web 抓取历史消息 + Bot 监听新消息

    async def _fetch_via_bot(self, channel_config: dict) -> List[Node]:
        """Bot 模式: 先尝试 Bot API 获取频道信息, 再用 Web 抓取消息"""
        channel_username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")

        if not channel_username:
            logger.warning(f"[{self.name}] 需要 username: {channel_name}")
            return []

        username = channel_username.lstrip("@")
        api_base = f"https://api.telegram.org/bot{self.bot_token}"
        nodes = []

        try:
            async with aiohttp.ClientSession() as session:
                # 1. 用 Bot API 验证频道存在且公开
                chat_url = f"{api_base}/getChat?chat_id=@{username}"
                async with session.get(chat_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            logger.info(f"[{self.name}] Bot 确认频道 @{username} 存在")
                        else:
                            logger.warning(f"[{self.name}] getChat 错误: {data.get('description')}")

                # 2. 尝试通过 Bot API 获取最近消息
                # Bot API 没有直接获取历史消息的接口
                # 但如果 Bot 在频道中, 可以通过 getUpdates 获取新消息
                # 对于历史消息, 仍然需要 web 抓取

                # 3. Web 抓取历史消息
                url = f"https://t.me/s/{username}"
                html = await self._fetch_page(session, url)
                if html:
                    nodes = self._extract_nodes_from_html(html, channel_name)
                    logger.info(f"[{self.name}] 频道 {channel_name}: Web 提取 {len(nodes)} 个节点")
                else:
                    # Web 也失败, 尝试通过 Bot 转发消息来获取内容
                    logger.info(f"[{self.name}] Web 失败, 尝试 Bot 转发获取")

        except Exception as e:
            logger.error(f"[{self.name}] Bot 抓取 {channel_name} 失败: {e}")

        self.mark_source(nodes, f"tg:{channel_name}")
        return nodes

    # ── Web Scrape 模式 ──

    async def _fetch_via_web(self, channel_config: dict) -> List[Node]:
        channel_username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")

        if not channel_username:
            return []

        username = channel_username.lstrip("@")
        url = f"https://t.me/s/{username}"
        logger.info(f"[{self.name}] Web 抓取: {channel_name} ({url})")

        async with aiohttp.ClientSession() as session:
            html = await self._fetch_page(session, url)

        if not html:
            return []

        nodes = self._extract_nodes_from_html(html, channel_name)
        self.mark_source(nodes, f"tg:{channel_name}")
        logger.info(f"[{self.name}] 频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes

    async def _fetch_page(self, session, url: str) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning(f"HTTP {resp.status} for {url}")
                return None
        except Exception as e:
            logger.error(f"抓取 {url} 失败: {e}")
            return None

    def _extract_nodes_from_html(self, html: str, channel_name: str) -> List[Node]:
        nodes = []

        # 消息文本
        for msg_html in re.compile(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', msg_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            nodes.extend(self._extract_nodes_from_text(text))

        # <pre> 标签
        for pre_html in re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', pre_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            try:
                nodes.extend(self.parser.parse(text, "base64"))
            except Exception:
                pass
            nodes.extend(self._extract_nodes_from_text(text))

        return nodes

    def _extract_nodes_from_text(self, text: str) -> List[Node]:
        nodes = []
        for pattern in self.NODE_PATTERNS:
            for match in re.findall(pattern, text):
                try:
                    node = self.parser._parse_node_url(match)
                    if node:
                        nodes.append(node)
                except Exception:
                    pass
        return nodes

    # ── Telethon 模式 ──

    async def _fetch_via_telethon(self, channel_config: dict) -> List[Node]:
        try:
            await self._init_client()
            return await self._fetch_channel_telethon(channel_config)
        finally:
            await self._close_client()

    async def _init_client(self):
        if self.client:
            return
        from telethon import TelegramClient
        session_path = Path("data/cache/telegram_session")
        session_path.parent.mkdir(parents=True, exist_ok=True)
        self.client = TelegramClient(str(session_path), int(self.api_id), self.api_hash)
        await self.client.start()

    async def _close_client(self):
        if self.client:
            await self.client.disconnect()
            self.client = None

    async def _fetch_channel_telethon(self, channel_config: dict) -> List[Node]:
        channel_username = channel_config.get("username")
        channel_id = channel_config.get("channel_id")
        max_messages = channel_config.get("max_messages", self.max_messages)
        channel_name = channel_config.get("name", str(channel_id))
        target = channel_username or channel_id

        nodes = []
        async for message in self.client.iter_messages(target, limit=max_messages):
            if message.text:
                nodes.extend(self._extract_nodes_from_text(message.text))

        self.mark_source(nodes, f"tg:{channel_name}")
        return nodes
