"""
Telegram 频道处理器
支持两种抓取方式:
1. Web Scrape: 通过预览页抓取 (无需任何配置, 推荐)
   - 优先使用 telegram.dog (t.me 镜像, GitHub Actions 中可用)
   - 降级使用 t.me
2. Telethon: 完整 Telegram 客户端 (需要 api_id/api_hash, 可选)
3. Bot API: 配合 Web 抓取验证频道存在性 (需要 Bot Token, 可选)
"""

import asyncio
import logging
import re
from typing import List, Optional
from pathlib import Path

import aiohttp

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)

# t.me 在 GitHub Actions 和国内网络可能被屏蔽
# telegram.dog 是 t.me 的镜像, 通常不被屏蔽
WEB_MIRRORS = ["https://telegram.dog/s/", "https://t.me/s/"]


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
        self.mode = config.get("mode", "web")
        self.parser = Parser()
        self.client = None

    async def fetch(self) -> List[Node]:
        if not self.enabled:
            return []

        all_nodes = []

        for ch in self.channels:
            if not ch.get("enabled", True):
                continue
            try:
                nodes = await self._fetch_channel(ch)
                all_nodes.extend(nodes)
            except Exception as e:
                logger.error(f"[{self.name}] 抓取频道 {ch.get('name')} 失败: {e}")

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
        return all_nodes

    async def _fetch_channel(self, channel_config: dict) -> List[Node]:
        """抓取单个频道 — 尝试多个镜像, 直到成功"""
        username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")

        if not username:
            logger.warning(f"[{self.name}] 需要 username: {channel_name}")
            return []

        username = username.lstrip("@")
        nodes = []

        async with aiohttp.ClientSession() as session:
            # 1. 尝试所有 Web 镜像
            for mirror in WEB_MIRRORS:
                url = f"{mirror}{username}"
                logger.info(f"[{self.name}] 尝试: {url}")
                html = await self._fetch_page(session, url)
                if html:
                    nodes = self._extract_nodes_from_html(html, channel_name)
                    if nodes:
                        logger.info(f"[{self.name}] {channel_name}: 通过 {mirror} 提取 {len(nodes)} 个节点")
                        break
                    else:
                        logger.info(f"[{self.name}] {channel_name}: 页面获取成功但无节点, 尝试下一镜像")

            # 2. Web 全部失败, 尝试 Bot API 验证
            if not nodes and self.bot_token:
                api_base = f"https://api.telegram.org/bot{self.bot_token}"
                chat_url = f"{api_base}/getChat?chat_id=@{username}"
                try:
                    async with session.get(chat_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("ok"):
                                logger.info(f"[{self.name}] Bot 确认 @{username} 存在, 但 Web 抓取失败")
                except Exception:
                    pass

        if not nodes:
            logger.warning(f"[{self.name}] 频道 {channel_name} (@{username}) 抓取失败")

        self.mark_source(nodes, f"tg:{channel_name}")
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
        except Exception as e:
            logger.debug(f"抓取 {url} 失败: {e}")
        return None

    def _extract_nodes_from_html(self, html: str, channel_name: str) -> List[Node]:
        nodes = []

        # 消息文本
        for msg_html in re.compile(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', msg_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            nodes.extend(self._extract_nodes_from_text(text))

        # <pre> 标签 (Base64 订阅内容)
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

    # ── Telethon 模式 (可选, 需要 api_id/api_hash) ──

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
