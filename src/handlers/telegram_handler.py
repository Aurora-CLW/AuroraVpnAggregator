"""
Telegram 频道处理器
支持两种模式:
1. Telethon (需要 api_id/api_hash, 完整功能)
2. Web Scrape (无需认证, 通过 t.me/s/channel 预览页抓取)
"""

import asyncio
import logging
import re
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
        self.api_id = config.get("api_id")
        self.api_hash = config.get("api_hash")
        self.channels = config.get("channels", [])
        self.max_messages = config.get("max_messages", 100)
        self.mode = config.get("mode", "web")  # web (默认) 或 telethon
        self.parser = Parser()
        self.client = None

    async def fetch(self) -> List[Node]:
        """从 Telegram 频道抓取节点"""
        if not self.enabled:
            return []

        all_nodes = []

        for channel_config in self.channels:
            if not channel_config.get("enabled", True):
                continue

            try:
                if self.mode == "telethon" and self.api_id and self.api_hash:
                    nodes = await self._fetch_via_telethon(channel_config)
                else:
                    nodes = await self._fetch_via_web(channel_config)
                all_nodes.extend(nodes)
            except Exception as e:
                logger.error(f"[{self.name}] 抓取频道 {channel_config.get('name')} 失败: {e}")

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
        return all_nodes

    # ── Web Scrape 模式 (无需认证) ──

    async def _fetch_via_web(self, channel_config: dict) -> List[Node]:
        """通过 t.me/s/channel 预览页抓取"""
        channel_id = channel_config.get("channel_id", "")
        channel_username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")
        max_messages = channel_config.get("max_messages", 50)

        # 构建 URL: t.me/s/channel_username
        if channel_username:
            username = channel_username.lstrip("@")
            url = f"https://t.me/s/{username}"
        elif channel_id:
            # channel_id 需要是 username
            logger.warning(f"[{self.name}] Web 模式需要 username 而非 channel_id: {channel_name}")
            return []
        else:
            logger.warning(f"[{self.name}] 频道 {channel_name} 未配置 username")
            return []

        logger.info(f"[{self.name}] Web 抓取频道: {channel_name} ({url})")

        html = await self._fetch_page(url)
        if not html:
            return []

        # 从 HTML 中提取消息文本
        nodes = self._extract_nodes_from_html(html, channel_name)
        self.mark_source(nodes, f"tg:{channel_name}")

        logger.info(f"[{self.name}] 频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes

    async def _fetch_page(self, url: str) -> Optional[str]:
        """抓取 Telegram 预览页面"""
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    else:
                        logger.warning(f"HTTP {resp.status} for {url}")
                        return None
        except Exception as e:
            logger.error(f"抓取 {url} 失败: {e}")
            return None

    def _extract_nodes_from_html(self, html: str, channel_name: str) -> List[Node]:
        """从 Telegram 预览页 HTML 中提取节点"""
        nodes = []

        # Telegram 预览页消息在 <div class="tgme_widget_message_text"> 中
        # 提取所有消息文本
        message_pattern = re.compile(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            re.DOTALL
        )

        messages = message_pattern.findall(html)

        for msg_html in messages:
            # 清理 HTML 标签
            text = re.sub(r'<[^>]+>', '', msg_html)
            # 解码 HTML 实体
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            # 提取节点
            extracted = self._extract_nodes_from_text(text)
            nodes.extend(extracted)

        # 也检查 <pre> 标签中的内容 (Base64 订阅链接)
        pre_pattern = re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL)
        pre_blocks = pre_pattern.findall(html)
        for pre_html in pre_blocks:
            text = re.sub(r'<[^>]+>', '', pre_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            # 尝试作为 base64 解析
            try:
                parsed = self.parser.parse(text, "base64")
                nodes.extend(parsed)
            except Exception:
                pass
            # 也提取 URL
            extracted = self._extract_nodes_from_text(text)
            nodes.extend(extracted)

        # 检查 <a> 标签中的订阅链接
        link_pattern = re.compile(r'href="(https?://[^"]+)"')
        links = link_pattern.findall(html)
        for link in links:
            # 检查是否是订阅链接
            if any(ext in link.lower() for ext in ["sub", "clash", "v2ray", "singbox", "proxy", "vpn", "token"]):
                try:
                    content = None
                    # 同步请求太慢，跳过外部链接抓取
                    # 只提取 URL 本身包含的节点
                    extracted = self._extract_nodes_from_text(link)
                    nodes.extend(extracted)
                except Exception:
                    pass

        return nodes

    def _extract_nodes_from_text(self, text: str) -> List[Node]:
        """从纯文本中提取节点 URL"""
        nodes = []
        for pattern in self.NODE_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
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
        """初始化 Telegram 客户端"""
        if self.client:
            return

        try:
            from telethon import TelegramClient

            session_path = Path("data/cache/telegram_session")
            session_path.parent.mkdir(parents=True, exist_ok=True)

            self.client = TelegramClient(
                str(session_path),
                int(self.api_id),
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

    async def _fetch_channel_telethon(self, channel_config: dict) -> List[Node]:
        """Telethon 模式抓取单个频道"""
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
                    extracted = self._extract_nodes_from_text(message.text)
                    nodes.extend(extracted)

                # 也检查消息中的文档/文件
                if message.document:
                    # 可能有订阅文件附件
                    pass

        except Exception as e:
            logger.error(f"Telethon 抓取频道 {channel_name} 失败: {e}")

        self.mark_source(nodes, f"tg:{channel_name}")
        logger.info(f"频道 {channel_name}: 提取 {len(nodes)} 个节点")
        return nodes
