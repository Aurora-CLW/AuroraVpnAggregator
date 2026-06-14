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
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)

# RSS API — 无需认证, GitHub Actions 可用
RSS_API_BASE = "https://tg.i-c-a.su/rss/"

# t.me 预览页镜像 (CI 可能被屏蔽, 作为降级)
WEB_MIRRORS = ["https://telegram.dog/s/", "https://t.me/s/"]

# 订阅 URL 匹配 — 匹配 https:// 链接, 但排除尾部黏附的中文/标点
SUB_URL_PATTERN = re.compile(r'https?://[^\s<>"\']+')

# 文件格式 URL 检测 — 根据扩展名推断订阅格式
FILE_FORMAT_MAP = {
    ".yaml": "clash",
    ".yml": "clash",
    ".json": "singbox",
    ".txt": "auto",
    ".base64": "base64",
}


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
        self.proxy = config.get("proxy") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self.parser = Parser()
        self.client = None
        self.channel_results: Dict[str, dict] = {}  # {channel_name: {nodes, sub_urls, status, updated_at}}

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

        # 跨频道节点去重 (按服务器+端口去重)
        seen = set()
        unique_nodes = []
        for node in all_nodes:
            key = (node.server, node.port) if hasattr(node, 'server') and hasattr(node, 'port') else id(node)
            if key not in seen:
                seen.add(key)
                unique_nodes.append(node)

        if len(unique_nodes) < len(all_nodes):
            logger.info(f"[{self.name}] 跨频道去重: {len(all_nodes)} → {len(unique_nodes)} 个节点")

        return unique_nodes

    async def _fetch_channel(self, channel_config: dict) -> List[Node]:
        """抓取单个频道 — RSS 优先, 无结果则翻页, 再降级 Web"""
        username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")
        pinned_url = channel_config.get("pinned_url", "")
        channel_format = channel_config.get("format", "auto")

        if not username:
            logger.warning(f"[{self.name}] 需要 username: {channel_name}")
            return []

        username = username.lstrip("@")
        nodes = []
        sub_urls: List[dict] = []

        async with aiohttp.ClientSession() as session:
            pending_msg_links: List[str] = []  # 待抓取的消息链接

            # 0. 如果配置了 pinned_url (置顶消息链接), 直接抓取该消息
            if pinned_url:
                logger.info(f"[{self.name}] {channel_name}: 抓取置顶消息 {pinned_url}")
                html = await self._fetch_page(session, pinned_url)
                if html:
                    result = self._extract_from_html(html, channel_name)
                    nodes.extend(result["nodes"])
                    sub_urls.extend(result["sub_urls"])
                    pending_msg_links.extend(result.get("msg_links", []))

            # 1. 尝试 RSS API
            rss_url = f"{RSS_API_BASE}{username}"
            logger.info(f"[{self.name}] 尝试 RSS: {rss_url}")
            rss_result = await self._fetch_via_rss(session, username, channel_name)
            if rss_result["nodes"]:
                logger.info(f"[{self.name}] {channel_name}: RSS 获取 {len(rss_result['nodes'])} 个节点")
                nodes.extend(rss_result["nodes"])
            if rss_result["sub_urls"]:
                sub_urls.extend(rss_result["sub_urls"])
            pending_msg_links.extend(rss_result.get("msg_links", []))

            # 2. RSS 无节点且无订阅链接, 尝试翻页抓取更多历史消息
            if not nodes and not sub_urls:
                sub_urls = await self._fetch_older_messages(session, username, channel_name)

            # 3. 仍无结果, 尝试 Web 镜像 (首页 + 翻页)
            if not nodes and not sub_urls:
                for mirror in WEB_MIRRORS:
                    url = f"{mirror}{username}"
                    logger.info(f"[{self.name}] 尝试 Web: {url}")
                    html = await self._fetch_page(session, url)
                    if html:
                        web_result = self._extract_from_html(html, channel_name)
                        nodes.extend(web_result["nodes"])
                        sub_urls.extend(web_result["sub_urls"])
                        pending_msg_links.extend(web_result.get("msg_links", []))
                        if web_result["nodes"] or web_result["sub_urls"]:
                            logger.info(f"[{self.name}] {channel_name}: Web 获取 {len(web_result['nodes'])} 个节点, {len(web_result['sub_urls'])} 个订阅链接")
                            break
                        # Web 首页无结果, 尝试翻页
                        older = await self._web_paginate(session, mirror, username, html, channel_name)
                        if older:
                            nodes.extend(older["nodes"])
                            sub_urls.extend(older["sub_urls"])
                            break

            # 4. 抓取发现的消息链接 (如"点我传送"指向的置顶消息)
            seen_msg_ids = set()
            # 已通过 pinned_url 抓过的消息不再重复
            if pinned_url:
                mid = re.search(r'/(\d+)/?$', pinned_url)
                if mid:
                    seen_msg_ids.add(mid.group(1))
            msg_link_count = 0
            max_msg_links = 5  # 限制消息链接抓取数量, 避免过度请求
            for link in pending_msg_links:
                if msg_link_count >= max_msg_links:
                    break
                mid = re.search(r'/(\d+)/?$', link)
                if mid and mid.group(1) in seen_msg_ids:
                    continue
                if mid:
                    seen_msg_ids.add(mid.group(1))
                # 跳过 ?single 参数链接 (页面内重复引用)
                if "?single" in link:
                    continue
                logger.info(f"[{self.name}] {channel_name}: 抓取消息链接 {link}")
                html = await self._fetch_page(session, link)
                if html:
                    result = self._extract_from_html(html, channel_name)
                    nodes.extend(result["nodes"])
                    sub_urls.extend(result["sub_urls"])
                    msg_link_count += 1
                await asyncio.sleep(1)

            # 4. 递归 fetch 订阅链接
            if sub_urls:
                # 去重
                seen = set()
                unique: List[dict] = []
                for u in sub_urls:
                    url_key = u["url"] if isinstance(u, dict) else u
                    if url_key not in seen:
                        seen.add(url_key)
                        unique.append(u if isinstance(u, dict) else {"url": u, "format_hint": "auto"})
                logger.info(f"[{self.name}] {channel_name}: 发现 {len(unique)} 个订阅链接, 开始递归解析")
                sub_nodes = await self._fetch_sub_urls(session, unique, channel_name, channel_format)
                logger.info(f"[{self.name}] {channel_name}: 订阅链接解析获取 {len(sub_nodes)} 个节点")
                nodes.extend(sub_nodes)

        if not nodes:
            logger.warning(f"[{self.name}] 频道 {channel_name} (@{username}) 抓取失败")

        # 记录频道抓取结果
        self.channel_results[channel_name] = {
            "nodes": len(nodes),
            "sub_urls": [u["url"] if isinstance(u, dict) else u for u in sub_urls],
            "status": "success" if nodes else "empty",
            "updated_at": datetime.now().isoformat(),
        }

        self.mark_source(nodes, f"tg:{channel_name}")
        return nodes

    async def _fetch_older_messages(self, session, username: str, channel_name: str) -> List[dict]:
        """RSS 无结果时, 尝试 Web 预览页翻页获取更多历史消息"""
        for mirror in WEB_MIRRORS:
            try:
                url = f"{mirror}{username}"
                html = await self._fetch_page(session, url)
                if not html:
                    continue
                result = await self._web_paginate(session, mirror, username, html, channel_name)
                if result and (result["nodes"] or result["sub_urls"]):
                    return result["sub_urls"]
            except Exception:
                continue
        return []

    async def _web_paginate(self, session, mirror: str, username: str, first_html: str, channel_name: str) -> Optional[dict]:
        """Web 预览页翻页 — 通过 ?before= 消息 ID 加载更早的消息"""
        all_nodes = []
        all_sub_urls = []
        html = first_html
        max_pages = 5

        for page in range(max_pages):
            # 从页面提取消息 ID (data-post="channel/12345")
            msg_ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
            if not msg_ids:
                break

            # 提取当前页内容
            result = self._extract_from_html(html, channel_name)
            all_nodes.extend(result["nodes"])
            all_sub_urls.extend(result["sub_urls"])

            # 如果已经找到节点或订阅链接, 停止翻页
            if all_nodes or all_sub_urls:
                break

            # 用最早的消息 ID 翻页
            oldest_id = min(int(mid) for mid in msg_ids)
            next_url = f"{mirror}{username}?before={oldest_id}"
            logger.info(f"[{self.name}] {channel_name}: Web 翻页 {page+1}, before={oldest_id}")
            html = await self._fetch_page(session, next_url)
            if not html:
                break
            await asyncio.sleep(1)

        if not all_nodes and not all_sub_urls:
            return None
        return {"nodes": all_nodes, "sub_urls": all_sub_urls}

    async def _fetch_via_rss(self, session, username: str, channel_name: str) -> dict:
        """通过 RSS API 获取频道消息并提取节点和订阅链接"""
        rss_url = f"{RSS_API_BASE}{username}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, text/html",
        }
        try:
            async with session.get(rss_url, headers=headers, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                if resp.status != 200:
                    logger.debug(f"RSS HTTP {resp.status} for {rss_url}")
                    return {"nodes": [], "sub_urls": [], "msg_links": []}
                xml_content = await resp.text()
                if not xml_content or "<rss" not in xml_content:
                    logger.debug(f"RSS 响应非 XML for {rss_url}")
                    return {"nodes": [], "sub_urls": [], "msg_links": []}
                return self._extract_from_rss(xml_content, channel_name)
        except Exception as e:
            logger.debug(f"RSS 抓取 {rss_url} 失败: {e}")
            return {"nodes": [], "sub_urls": [], "msg_links": []}

    # Telegram 频道消息链接匹配 (t.me/频道名/消息ID)
    _TG_MSG_LINK_PATTERN = re.compile(r'https?://(?:t\.me|telegram\.dog|telegram\.me)/([a-zA-Z0-9_]+)/(\d+)')

    def _extract_from_rss(self, xml_content: str, channel_name: str) -> dict:
        """从 RSS XML 中提取节点、订阅链接和频道内消息链接"""
        nodes = []
        sub_urls: List[dict] = []
        msg_links: List[str] = []  # t.me 频道内消息链接
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.debug(f"RSS XML 解析失败: {e}")
            return {"nodes": [], "sub_urls": [], "msg_links": []}

        for item in root.iter("item"):
            # 从 <title> 提取
            title_elem = item.find("title")
            if title_elem is not None and title_elem.text:
                text = title_elem.text
                nodes.extend(self._extract_nodes_from_text(text))
                sub_urls.extend(self._extract_sub_urls(text))
                msg_links.extend(self._extract_msg_links(text))

            # 从 <description> 提取 (含 HTML 实体的节点链接)
            desc_elem = item.find("description")
            if desc_elem is not None and desc_elem.text:
                text = desc_elem.text
                # 解码 HTML 实体
                text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')

                # 从 href 属性中提取链接 (保留超链接, 纯文本会丢失这些)
                for href in re.findall(r'href=["\']([^"\'\s]+)["\']', text):
                    if self._TG_MSG_LINK_PATTERN.match(href):
                        msg_links.append(href)
                    else:
                        href_item = self._clean_sub_url(href)
                        if href_item:
                            seen_urls = {u["url"] for u in sub_urls}
                            if href_item["url"] not in seen_urls:
                                sub_urls.insert(0, href_item)

                # 去除 HTML 标签
                clean_text = re.sub(r'<[^>]+>', '', text)
                nodes.extend(self._extract_nodes_from_text(clean_text))
                sub_urls.extend(self._extract_sub_urls(clean_text))
                msg_links.extend(self._extract_msg_links(clean_text))

                # 从 <pre> 标签中提取订阅链接 (优先)
                for pre_match in re.finditer(r'<pre>(.*?)</pre>', text, re.DOTALL):
                    pre_content = pre_match.group(1).strip()
                    pre_content = pre_content.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
                    pre_urls = self._extract_sub_urls(pre_content)
                    seen_urls = {u["url"] for u in sub_urls}
                    for url_item in pre_urls:
                        if url_item["url"] not in seen_urls:
                            sub_urls.insert(0, url_item)
                            seen_urls.add(url_item["url"])

        # 去重, 保持顺序
        seen = set()
        unique_sub_urls: List[dict] = []
        for url_item in sub_urls:
            if url_item["url"] not in seen:
                seen.add(url_item["url"])
                unique_sub_urls.append(url_item)

        # 去重消息链接
        seen_links = set()
        unique_msg_links = []
        for link in msg_links:
            if link not in seen_links:
                seen_links.add(link)
                unique_msg_links.append(link)

        return {"nodes": nodes, "sub_urls": unique_sub_urls, "msg_links": unique_msg_links}

    async def _fetch_page(self, session, url: str) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with session.get(url, headers=headers, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning(f"HTTP {resp.status} for {url}")
        except Exception as e:
            logger.debug(f"抓取 {url} 失败: {e}")
        return None

    def _extract_from_html(self, html: str, channel_name: str) -> dict:
        """从 HTML 预览页中提取节点和订阅链接"""
        nodes = []
        sub_urls: List[dict] = []
        msg_links: List[str] = []

        # <meta> 标签 (og:description / twitter:description 包含消息摘要)
        for meta_match in re.finditer(r'<meta\s+(?:property|name)="(?:og:|twitter:)description"\s+content="([^"]*)"', html):
            text = meta_match.group(1)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            nodes.extend(self._extract_nodes_from_text(text))
            sub_urls.extend(self._extract_sub_urls(text))

        # <a href> 超链接 (消息文本中的链接 + inline link buttons)
        for href in re.findall(r'href="([^"]+)"', html):
            # t.me 消息链接
            if self._TG_MSG_LINK_PATTERN.match(href):
                msg_links.append(href)
            else:
                item = self._clean_sub_url(href)
                if item:
                    seen = {u["url"] for u in sub_urls}
                    if item["url"] not in seen:
                        sub_urls.append(item)

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
            seen_urls = {u["url"] if isinstance(u, dict) else u for u in sub_urls}
            for url_item in pre_urls:
                url_key = url_item["url"] if isinstance(url_item, dict) else url_item
                if url_key not in seen_urls:
                    sub_urls.insert(0, url_item)
                    seen_urls.add(url_key)
            nodes.extend(self._extract_nodes_from_text(text))

        # 去重
        seen = set()
        unique_sub_urls: List[dict] = []
        for url_item in sub_urls:
            if url_item["url"] not in seen:
                seen.add(url_item["url"])
                unique_sub_urls.append(url_item)

        # 去重消息链接
        seen_links = set()
        unique_msg_links = []
        for link in msg_links:
            if link not in seen_links:
                seen_links.add(link)
                unique_msg_links.append(link)

        return {"nodes": nodes, "sub_urls": unique_sub_urls, "msg_links": unique_msg_links}

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
        # 机场推广页 (用户要求: 碰到机场都是广告)
        "go4sharing.github.io",
    }

    # 排除的 URL 路径关键词 (广告/机场注册/非订阅)
    _EXCLUDED_PATH_KEYWORDS = {
        "ad", "ads", "invite", "register", "signup",
        "download/app", "store", "play.google",
        # 机场注册/推广路径
        "/#/register", "/#/login", "/#/signup",
        "airport", "jichang",
    }

    # 排除的 URL 查询参数 (机场推广/广告)
    _EXCLUDED_QUERY_PARAMS = {"ch", "inv", "ref", "aff", "code", "i_code"}

    def _extract_sub_urls(self, text: str) -> List[dict]:
        """从文本中提取订阅 URL (https:// 开头, 排除非订阅链接)

        返回 [{"url": str, "format_hint": str}] 列表, format_hint 根据文件扩展名推断。
        """
        urls: List[dict] = []
        for match in SUB_URL_PATTERN.findall(text):
            # 处理连续拼接的 URL: 如果 URL 中包含 https://, 截断到第一个
            # 例如 "https://a.com/pathClashhttps://b.com/sub" → "https://a.com/path"
            inner_https = match.find("https://", 1)  # 从位置1开始找, 跳过开头的 https://
            if inner_https > 0:
                match = match[:inner_https]
            # 清理 URL: 剥离黏附的中文/emoji/标点 (从第一个非 URL 合法字符处截断)
            # URL 合法字符: ASCII 可打印字符中排除空格和 <>"'
            url = re.sub(r'[^\x21-\x7E]+.*$', '', match)
            # 剥离尾部不合法的 ASCII 标点 (如右括号、逗号等)
            url = re.sub(r'[,;。，；）)》】]+$', '', url)
            if not url or len(url) < 10:
                continue
            # 排除社交/广告域名
            if any(url.startswith(f"https://{d}") for d in self._EXCLUDED_SUB_DOMAINS):
                continue
            # 排除图片/文件等非订阅链接
            if any(url.endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".mp4", ".pdf", ".apk", ".exe")):
                continue
            # 排除 tg.i-c-a.su 媒体链接
            if "/media/" in url and "tg.i-c-a.su" in url:
                continue
            # 排除广告/注册/机场推广链接
            url_lower = url.lower()
            if any(kw in url_lower for kw in self._EXCLUDED_PATH_KEYWORDS):
                continue
            # 排除机场推广查询参数 (?ch=xxx, ?ref=xxx 等)
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            if any(p in qs for p in self._EXCLUDED_QUERY_PARAMS):
                continue
            # 根据文件扩展名推断格式
            format_hint = "auto"
            for ext, fmt in FILE_FORMAT_MAP.items():
                if url.endswith(ext):
                    format_hint = fmt
                    break
            urls.append({"url": url, "format_hint": format_hint})
        return urls

    def _clean_sub_url(self, raw_url: str) -> Optional[dict]:
        """清理单个 URL 并返回 {"url": str, "format_hint": str} 或 None"""
        url = re.sub(r'[^\x21-\x7E]+.*$', '', raw_url)
        url = re.sub(r'[,;。，；）》】]+$', '', url)
        # 只接受绝对 https:// URL (排除相对路径, //cdn 等)
        if not url or not url.startswith("https://"):
            return None
        if len(url) < 12:
            return None
        # 排除非订阅链接
        if any(url.startswith(f"https://{d}") for d in self._EXCLUDED_SUB_DOMAINS):
            return None
        if any(url.endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".mp4", ".pdf", ".apk", ".exe")):
            return None
        if "/media/" in url and "tg.i-c-a.su" in url:
            return None
        url_lower = url.lower()
        if any(kw in url_lower for kw in self._EXCLUDED_PATH_KEYWORDS):
            return None
        # 排除机场推广查询参数
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if any(p in qs for p in self._EXCLUDED_QUERY_PARAMS):
            return None
        format_hint = "auto"
        for ext, fmt in FILE_FORMAT_MAP.items():
            if url.endswith(ext):
                format_hint = fmt
                break
        return {"url": url, "format_hint": format_hint}

    def _extract_msg_links(self, text: str) -> List[str]:
        """从文本中提取 Telegram 频道内消息链接 (t.me/channel/12345)"""
        return [m.group(0) for m in self._TG_MSG_LINK_PATTERN.finditer(text)]

    async def _fetch_sub_urls(
        self,
        session,
        sub_urls: List[dict],
        channel_name: str,
        channel_format: str = "auto",
    ) -> List[Node]:
        """递归 fetch 订阅链接并解析节点

        sub_urls 为 [{"url": str, "format_hint": str}] 列表。
        优先使用 format_hint, 其次使用 channel_format, 最终降级为 auto。
        """
        all_nodes = []
        # 尝试更多订阅链接 (有些可能超时/不可达)
        max_sub = 5
        for item in sub_urls[:max_sub]:
            url = item["url"] if isinstance(item, dict) else item
            hint = item.get("format_hint", "auto") if isinstance(item, dict) else "auto"
            # 优先使用 format_hint, 若为 auto 则降级到 channel_format
            parse_format = hint if hint != "auto" else channel_format
            try:
                logger.info(f"[{self.name}] {channel_name}: fetch 订阅链接 {url} (format={parse_format})")
                async with session.get(url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug(f"订阅链接 HTTP {resp.status}: {url}")
                        continue
                    content = await resp.text()
                    if not content or len(content) < 20:
                        continue
                    # 如果返回 HTML 页面 (机场推广页), 跳过
                    content_stripped = content.strip()
                    if content_stripped.startswith("<!DOCTYPE") or content_stripped.startswith("<html"):
                        logger.debug(f"订阅链接返回 HTML 页面 (机场推广), 跳过: {url}")
                        continue
                    # 用 Parser 解析, 优先使用推断格式, auto 则自动检测
                    nodes = self.parser.parse(content, parse_format)
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
