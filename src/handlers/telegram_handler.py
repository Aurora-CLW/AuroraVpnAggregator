"""
Telegram 频道处理器
支持三种模式:
1. bot: 通过 Bot API 获取公开频道消息 (推荐, 只需 Bot Token)
2. web: 通过 t.me/s/ 预览页抓取 (无需任何配置, 但可能被屏蔽)
3. telethon: 完整 Telegram 客户端 (需要 api_id/api_hash)
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
        self.mode = config.get("mode", "web")  # bot, web, telethon
        self.parser = Parser()
        self.client = None

    async def fetch(self) -> List[Node]:
        """从 Telegram 频道抓取节点"""
        if not self.enabled:
            return []

        all_nodes = []

        # 自动选择模式: 有 bot_token 用 bot, 否则 web
        mode = self.mode
        if mode == "bot" and not self.bot_token:
            logger.warning(f"[{self.name}] Bot 模式但未配置 bot_token, 降级到 web 模式")
            mode = "web"

        for channel_config in self.channels:
            if not channel_config.get("enabled", True):
                continue

            try:
                if mode == "bot":
                    nodes = await self._fetch_via_bot(channel_config)
                elif mode == "telethon" and self.api_id and self.api_hash:
                    nodes = await self._fetch_via_telethon(channel_config)
                else:
                    nodes = await self._fetch_via_web(channel_config)
                all_nodes.extend(nodes)
            except Exception as e:
                logger.error(f"[{self.name}] 抓取频道 {channel_config.get('name')} 失败: {e}")

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
        return all_nodes

    # ── Bot API 模式 (推荐) ──

    async def _fetch_via_bot(self, channel_config: dict) -> List[Node]:
        """通过 Bot API 获取公开频道消息"""
        channel_username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")
        limit = channel_config.get("max_messages", 50)

        if not channel_username:
            logger.warning(f"[{self.name}] Bot 模式需要 username: {channel_name}")
            return []

        username = channel_username.lstrip("@")
        logger.info(f"[{self.name}] Bot 抓取频道: {channel_name} (@{username})")

        # Bot API: getChat 获取频道信息, 然后用 getUpdates 或直接读取
        # 公开频道可以用 Bot API 的 getChat + 转发消息方式
        # 但最直接的是: 让 Bot 成为频道成员后用 getUpdates
        # 公开频道可以直接用 Bot API 访问

        nodes = []
        api_base = f"https://api.telegram.org/bot{self.bot_token}"

        try:
            async with aiohttp.ClientSession() as session:
                # 获取频道信息
                chat_url = f"{api_base}/getChat?chat_id=@{username}"
                async with session.get(chat_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.warning(f"[{self.name}] getChat 失败: HTTP {resp.status}")
                        return []
                    data = await resp.json()
                    if not data.get("ok"):
                        logger.warning(f"[{self.name}] getChat 错误: {data.get('description')}")
                        return []
                    chat_id = data["result"]["id"]

                # 获取最近消息
                # Bot API 没有 getHistory, 但可以用 getUpdates 捕获新消息
                # 对于已有消息, 需要用 forwardMessage 间接获取
                # 最实用的方式: 用 Bot 监听新消息, 或使用 web 模式抓取历史

                # 替代方案: 直接抓取 t.me/s/ 预览页 (但用 Bot Token 作为备用)
                # 在 GitHub Actions 中尝试 web, 失败则跳过
                html = await self._fetch_page(session, f"https://t.me/s/{username}")
                if html:
                    nodes = self._extract_nodes_from_html(html, channel_name)
                else:
                    logger.warning(f"[{self.name}] Web 抓取也失败, 跳过频道 {channel_name}")

        except Exception as e:
            logger.error(f"[{self.name}] Bot 抓取频道 {channel_name} 失败: {e}")

        self.mark_source(nodes, f"tg:{channel_name}")
        logger.info(f"[{self.name}] 频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes

    # ── Web Scrape 模式 (无需认证) ──

    async def _fetch_via_web(self, channel_config: dict) -> List[Node]:
        """通过 t.me/s/channel 预览页抓取"""
        channel_username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")

        if not channel_username:
            logger.warning(f"[{self.name}] Web 模式需要 username: {channel_name}")
            return []

        username = channel_username.lstrip("@")
        url = f"https://t.me/s/{username}"
        logger.info(f"[{self.name}] Web 抓取频道: {channel_name} ({url})")

        async with aiohttp.ClientSession() as session:
            html = await self._fetch_page(session, url)

        if not html:
            return []

        nodes = self._extract_nodes_from_html(html, channel_name)
        self.mark_source(nodes, f"tg:{channel_name}")

        logger.info(f"[{self.name}] 频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes

    async def _fetch_page(self, session_or_none, url: str) -> Optional[str]:
        """抓取页面"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
            if session_or_none:
                async with session_or_none.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.text()
            else:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 200:
                            return await resp.text()
            logger.warning(f"HTTP 非 200 for {url}")
            return None
        except Exception as e:
            logger.error(f"抓取 {url} 失败: {e}")
            return None

    def _extract_nodes_from_html(self, html: str, channel_name: str) -> List[Node]:
        """从 Telegram 预览页 HTML 中提取节点"""
        nodes = []

        # 消息文本
        message_pattern = re.compile(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            re.DOTALL
        )
        for msg_html in message_pattern.findall(html):
            text = re.sub(r'<[^>]+>', '', msg_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            nodes.extend(self._extract_nodes_from_text(text))

        # <pre> 标签 (Base64 订阅)
        for pre_html in re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', pre_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            try:
                nodes.extend(self.parser.parse(text, "base64"))
            except Exception:
                pass
            nodes.extend(self._extract_nodes_from_text(text))

        # <a> 标签中的订阅链接
        for link in re.compile(r'href="(https?://[^"]+)"').findall(html):
            if any(kw in link.lower() for kw in ["sub", "clash", "v2ray", "singbox", "proxy", "vpn", "token"]):
                # 异步抓取订阅链接内容
                pass  # 同步环境中无法直接抓取, 先跳过

        return nodes

    def _extract_nodes_from_text(self, text: str) -> List[Node]:
        """从纯文本中提取节点 URL"""
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

    # ── Telethon 模式 (需要 API) ──

    async def _fetch_via_telethon(self, channel_config: dict) -> List[Node]:
        """通过 Telethon 抓取 (完整功能)"""
        try:
            await self._init_client()
            nodes = await self._fetch_channel_telethon(channel_config)
            return nodes
        finally:
            await self._close_client()

    async def _init_client(self):
        if self.client:
            return
        try:
            from telethon import TelegramClient
            session_path = Path("data/cache/telegram_session")
            session_path.parent.mkdir(parents=True, exist_ok=True)
            self.client = TelegramClient(str(session_path), int(self.api_id), self.api_hash)
            await self.client.start()
            logger.info("Telegram 客户端初始化成功")
        except ImportError:
            logger.error("telethon 未安装，请运行: pip install telethon")
            raise
        except Exception as e:
            logger.error(f"Telegram 客户端初始化失败: {e}")
            raise

    async def _close_client(self):
        if self.client:
            await self.client.disconnect()
            self.client = None

    async def _fetch_channel_telethon(self, channel_config: dict) -> List[Node]:
        channel_id = channel_config.get("channel_id")
        channel_username = channel_config.get("username")
        max_messages = channel_config.get("max_messages", self.max_messages)
        channel_name = channel_config.get("name", str(channel_id))
        target = channel_username or channel_id
        logger.info(f"Telethon 抓取频道: {channel_name}")

        nodes = []
        try:
            async for message in self.client.iter_messages(target, limit=max_messages):
                if message.text:
                    nodes.extend(self._extract_nodes_from_text(message.text))
        except Exception as e:
            logger.error(f"Telethon 抓取频道 {channel_name} 失败: {e}")

        self.mark_source(nodes, f"tg:{channel_name}")
        logger.info(f"频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes
