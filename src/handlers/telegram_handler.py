"""
Telegram 频道处理器
支持三种抓取方式 (按优先级自动尝试):
1. RSS API: 通过 tg.i-c-a.su 获取 RSS (无需认证, 推荐, CI 可用)
2. Web Scrape: 通过预览页抓取 (无需配置, t.me/telegram.dog 在 CI 可能被屏蔽)
3. Telethon: 完整 Telegram 客户端 (需要 api_id/api_hash, 可选)

支持两种消息格式:
- 直接节点: 消息含 vmess://, vless:// 等协议链接
- 订阅链接: 消息含 https:// 订阅 URL, 自动 fetch 并解析
"""

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from typing import List, Optional
from pathlib import Path

import aiohttp

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)

# RSS API — 无需认证, GitHub Actions 可用
RSS_API_BASE = "https://tg.i-c-a.su/rss/"

# t.me 预览页镜像 (CI 可能被屏蔽, 作为降级)
WEB_MIRRORS = ["https://telegram.dog/s/", "https://t.me/s/"]

# 订阅 URL 匹配 — 匹配 <pre> 中的 https:// 链接
SUB_URL_PATTERN = re.compile(r'https?://[^\s<>"\']+')


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

        for i, ch in enumerate(self.channels):
            if not ch.get("enabled", True):
                continue
            try:
                nodes = await self._fetch_channel(ch)
                all_nodes.extend(nodes)
            except Exception as e:
                logger.error(f"[{self.name}] 抓取频道 {ch.get('name')} 失败: {e}")
            # 频道间延迟, 避免 RSS API 限流
            if i < len(self.channels) - 1:
                await asyncio.sleep(2)

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")
        return all_nodes

    async def _fetch_channel(self, channel_config: dict) -> List[Node]:
        """抓取单个频道 — RSS 优先, Web 降级"""
        username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")

        if not username:
            logger.warning(f"[{self.name}] 需要 username: {channel_name}")
            return []

        username = username.lstrip("@")
        nodes = []
        sub_urls = []

        async with aiohttp.ClientSession() as session:
            # 1. 优先尝试 RSS API (CI 友好)
            rss_url = f"{RSS_API_BASE}{username}"
            logger.info(f"[{self.name}] 尝试 RSS: {rss_url}")
            rss_result = await self._fetch_via_rss(session, username, channel_name)
            if rss_result["nodes"]:
                logger.info(f"[{self.name}] {channel_name}: RSS 获取 {len(rss_result['nodes'])} 个节点")
                nodes = rss_result["nodes"]
            if rss_result["sub_urls"]:
                sub_urls = rss_result["sub_urls"]

            # 2. RSS 失败, 尝试 Web 镜像
            if not nodes and not sub_urls:
                for mirror in WEB_MIRRORS:
                    url = f"{mirror}{username}"
                    logger.info(f"[{self.name}] 尝试 Web: {url}")
                    html = await self._fetch_page(session, url)
                    if html:
                        web_result = self._extract_from_html(html, channel_name)
                        if web_result["nodes"] or web_result["sub_urls"]:
                            logger.info(f"[{self.name}] {channel_name}: Web 获取 {len(web_result['nodes'])} 个节点, {len(web_result['sub_urls'])} 个订阅链接")
                            nodes = web_result["nodes"]
                            sub_urls = web_result["sub_urls"]
                            break
                        else:
                            logger.info(f"[{self.name}] {channel_name}: 页面成功但无节点, 尝试下一镜像")

            # 3. 递归 fetch 订阅链接
            if sub_urls:
                logger.info(f"[{self.name}] {channel_name}: 发现 {len(sub_urls)} 个订阅链接, 开始递归解析")
                sub_nodes = await self._fetch_sub_urls(session, sub_urls, channel_name)
                logger.info(f"[{self.name}] {channel_name}: 订阅链接解析获取 {len(sub_nodes)} 个节点")
                nodes.extend(sub_nodes)

        if not nodes:
            logger.warning(f"[{self.name}] 频道 {channel_name} (@{username}) 抓取失败")

        self.mark_source(nodes, f"tg:{channel_name}")
        return nodes

    async def _fetch_via_rss(self, session, username: str, channel_name: str) -> dict:
        """通过 RSS API 获取频道消息并提取节点和订阅链接"""
        rss_url = f"{RSS_API_BASE}{username}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, text/html",
        }
        try:
            async with session.get(rss_url, headers=headers, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                if resp.status != 200:
                    logger.debug(f"RSS HTTP {resp.status} for {rss_url}")
                    return {"nodes": [], "sub_urls": []}
                xml_content = await resp.text()
                if not xml_content or "<rss" not in xml_content:
                    logger.debug(f"RSS 响应非 XML for {rss_url}")
                    return {"nodes": [], "sub_urls": []}
                return self._extract_from_rss(xml_content, channel_name)
        except Exception as e:
            logger.debug(f"RSS 抓取 {rss_url} 失败: {e}")
            return {"nodes": [], "sub_urls": []}

    def _extract_from_rss(self, xml_content: str, channel_name: str) -> dict:
        """从 RSS XML 中提取节点和订阅链接"""
        nodes = []
        sub_urls = []
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.debug(f"RSS XML 解析失败: {e}")
            return {"nodes": [], "sub_urls": []}

        for item in root.iter("item"):
            # 从 <title> 提取
            title_elem = item.find("title")
            if title_elem is not None and title_elem.text:
                text = title_elem.text
                nodes.extend(self._extract_nodes_from_text(text))
                sub_urls.extend(self._extract_sub_urls(text))

            # 从 <description> 提取 (含 HTML 实体的节点链接)
            desc_elem = item.find("description")
            if desc_elem is not None and desc_elem.text:
                text = desc_elem.text
                # 解码 HTML 实体
                text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
                # 去除 HTML 标签
                clean_text = re.sub(r'<[^>]+>', '', text)
                nodes.extend(self._extract_nodes_from_text(clean_text))
                sub_urls.extend(self._extract_sub_urls(clean_text))

                # 从 <pre> 标签中提取订阅链接 (优先)
                for pre_match in re.finditer(r'<pre>(.*?)</pre>', text, re.DOTALL):
                    pre_content = pre_match.group(1).strip()
                    pre_content = pre_content.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
                    pre_urls = self._extract_sub_urls(pre_content)
                    # <pre> 中的链接优先级最高, 去重添加到前面
                    for url in pre_urls:
                        if url not in sub_urls:
                            sub_urls.insert(0, url)

        # 去重, 保持顺序
        seen = set()
        unique_sub_urls = []
        for url in sub_urls:
            if url not in seen:
                seen.add(url)
                unique_sub_urls.append(url)

        return {"nodes": nodes, "sub_urls": unique_sub_urls}

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

    def _extract_from_html(self, html: str, channel_name: str) -> dict:
        """从 HTML 预览页中提取节点和订阅链接"""
        nodes = []
        sub_urls = []

        # 消息文本
        for msg_html in re.compile(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', msg_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            nodes.extend(self._extract_nodes_from_text(text))
            sub_urls.extend(self._extract_sub_urls(text))

        # <pre> 标签 (Base64 订阅内容 或 订阅链接)
        for pre_html in re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', pre_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            # 先尝试解析为 Base64/Clash 等格式
            try:
                pre_nodes = self.parser.parse(text, "auto")
                if pre_nodes:
                    nodes.extend(pre_nodes)
                    continue
            except Exception:
                pass
            # 不是节点格式, 尝试提取订阅链接
            pre_urls = self._extract_sub_urls(text)
            for url in pre_urls:
                if url not in sub_urls:
                    sub_urls.insert(0, url)
            nodes.extend(self._extract_nodes_from_text(text))

        # 去重
        seen = set()
        unique_sub_urls = []
        for url in sub_urls:
            if url not in seen:
                seen.add(url)
                unique_sub_urls.append(url)

        return {"nodes": nodes, "sub_urls": unique_sub_urls}

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

    # 排除的订阅链接域名 (非订阅链接)
    _EXCLUDED_SUB_DOMAINS = {
        # Telegram 自身
        "t.me", "telegram.me", "telegram.dog", "telegram.org",
        # RSS 镜像
        "tg.i-c-a.su",
        # 社交/视频平台
        "youtu.be", "youtube.com", "twitter.com", "x.com",
        "instagram.com", "facebook.com", "tiktok.com",
        # 代码/文档平台
        "github.com", "gitlab.com", "gist.github.com",
        # 搜索引擎
        "google.com", "bing.com", "baidu.com",
        # 短链接 (可能指向广告)
        "bit.ly", "tinyurl.com", "t.cn",
    }

    # 排除的 URL 路径关键词 (广告/非订阅)
    _EXCLUDED_PATH_KEYWORDS = {
        "ad", "ads", "invite", "register", "signup",
        "download/app", "store", "play.google",
    }

    def _extract_sub_urls(self, text: str) -> List[str]:
        """从文本中提取订阅 URL (https:// 开头, 排除非订阅链接)"""
        urls = []
        for match in SUB_URL_PATTERN.findall(text):
            # 排除社交/广告域名
            if any(match.startswith(f"https://{d}") for d in self._EXCLUDED_SUB_DOMAINS):
                continue
            # 排除图片/文件等非订阅链接
            if any(match.endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".mp4", ".pdf", ".apk", ".exe")):
                continue
            # 排除 tg.i-c-a.su 媒体链接
            if "/media/" in match and "tg.i-c-a.su" in match:
                continue
            urls.append(match)
        return urls

    async def _fetch_sub_urls(self, session, sub_urls: List[str], channel_name: str) -> List[Node]:
        """递归 fetch 订阅链接并解析节点"""
        all_nodes = []
        # 只取最新的几个订阅链接 (避免请求过多)
        max_sub = 3
        for url in sub_urls[:max_sub]:
            try:
                logger.info(f"[{self.name}] {channel_name}: fetch 订阅链接 {url}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug(f"订阅链接 HTTP {resp.status}: {url}")
                        continue
                    content = await resp.text()
                    if not content or len(content) < 20:
                        continue
                    # 用 Parser 自动检测格式并解析
                    nodes = self.parser.parse(content, "auto")
                    if nodes:
                        logger.info(f"[{self.name}] {channel_name}: 订阅链接解析 {len(nodes)} 个节点: {url}")
                        all_nodes.extend(nodes)
                    else:
                        logger.debug(f"订阅链接无有效节点: {url}")
            except Exception as e:
                logger.debug(f"订阅链接 fetch 失败 {url}: {e}")
        return all_nodes

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
