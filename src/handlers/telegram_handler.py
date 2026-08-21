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
import ipaddress
import json as _json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp

try:
    import defusedxml.ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET  # fallback, 不推荐

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)

# RSS API — 无需认证, GitHub Actions 可用
RSS_API_BASE = "https://tg.i-c-a.su/rss/"

# t.me 预览页镜像 (CI 可能被屏蔽, 作为降级)
WEB_MIRRORS = ["https://telegram.dog/s/", "https://t.me/s/"]

# HF Space TG Parser API — 可访问频道的完整消息历史 (CI/本地均可用)
HF_TG_PARSER_BASE = "https://aurora0722-tg-parser-api.hf.space/tg/history"
HF_TG_PARSER_KEY = os.environ.get("HF_TG_PARSER_KEY", "")

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
        r'(hysteria2?://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(hy2://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(anytls://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
        r'(tuic://[A-Za-z0-9\-._~:/?#\[\]\x40!$&\'()*+,;=%]+)',  # \x40 = @
        r'(socks5?://[A-Za-z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+)',
    ]

    def __init__(self, config: dict):
        super().__init__(config)
        self.bot_token = config.get("bot_token", "")
        self.api_id = config.get("api_id")
        self.api_hash = config.get("api_hash")
        self.channels = config.get("channels", [])
        self.max_messages = config.get("max_messages", 100)
        # 每个频道至少收集的有效消息数 (含节点或订阅链接的消息)
        # RSS/Web 翻页/HF API 会一直抓取直到达到该数量
        self.min_valid = int(config.get("min_valid", 50))
        # Web 预览页翻页最大页数 (每页约 20 条消息)
        self.max_web_pages = int(config.get("max_web_pages", 6))
        # RSS 缓存有效期 (秒), 减少重复请求降低限流触发率
        self.rss_cache_ttl = int(config.get("rss_cache_ttl", 1800))
        self.mode = config.get("mode", "web")
        self.proxy = config.get("proxy") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self.parser = Parser()
        self.client = None
        self.channel_results: Dict[str, dict] = {}  # {channel_name: {nodes, sub_urls, status, updated_at}}
        self._dead_urls = self._load_dead_urls()

        # 合并配置中的排除域名 (机场推广/纯广告域名), 不影响默认集
        extra_domains = config.get("exclude_domains", []) or []
        if extra_domains:
            self._EXCLUDED_SUB_DOMAINS = set(self._EXCLUDED_SUB_DOMAINS) | {str(d).strip().lower() for d in extra_domains if str(d).strip()}

    async def fetch(self) -> List[Node]:
        if not self.enabled:
            return []

        channels = [ch for ch in self.channels if ch.get("enabled", True)]
        # 频道级并发 (限制并发数避免 RSS API 限流)
        max_concurrent = int(self.config.get("max_concurrent_channels", 3))
        sem = asyncio.Semaphore(max_concurrent)

        async def _worker(ch: dict) -> List[Node]:
            async with sem:
                try:
                    return await self._fetch_channel(ch)
                except Exception as e:
                    logger.error(f"[{self.name}] 抓取频道 {ch.get('name')} 失败: {e}")
                    return []

        results = await asyncio.gather(*[_worker(ch) for ch in channels])
        all_nodes = [n for res in results for n in res]

        logger.info(f"[{self.name}] Telegram 抓取完成: {len(all_nodes)} 个节点")

        # 跨频道节点去重 (按服务器+端口去重)
        seen = set()
        unique_nodes = []
        for node in all_nodes:
            # 去重键包含 type + 唯一标识，避免同址不同协议/不同凭据的节点被误并
            key = (node.type, node.server, node.port, node.uuid or node.password or node.username) if hasattr(node, 'server') and hasattr(node, 'port') else id(node)
            if key not in seen:
                seen.add(key)
                unique_nodes.append(node)

        if len(unique_nodes) < len(all_nodes):
            logger.info(f"[{self.name}] 跨频道去重: {len(all_nodes)} → {len(unique_nodes)} 个节点")

        self._save_dead_urls(self._dead_urls)
        return unique_nodes

    async def _fetch_channel(self, channel_config: dict) -> List[Node]:
        """抓取单个频道 — RSS 优先, 无结果则翻页, 再降级 Web"""
        username = channel_config.get("username", "")
        channel_name = channel_config.get("name", "unknown")
        pinned_url = channel_config.get("pinned_url", "")
        channel_format = channel_config.get("format", "auto")
        website_url = channel_config.get("website_url", "")

        if not username:
            logger.warning(f"[{self.name}] 需要 username: {channel_name}")
            return []

        username = username.lstrip("@")
        nodes = []
        sub_urls: List[dict] = []

        async with aiohttp.ClientSession(max_field_size=65536) as session:
            pending_msg_links: List[str] = []  # 待抓取的消息链接

            # 0. 如果配置了已知订阅链接 (sub_urls), 直接加入
            config_sub_urls = channel_config.get("sub_urls", [])
            if config_sub_urls:
                for u in config_sub_urls:
                    if isinstance(u, dict):
                        sub_urls.append(u)
                    else:
                        sub_urls.append({"url": u, "format_hint": channel_format if channel_format != "auto" else "auto"})
                logger.info(f"[{self.name}] {channel_name}: 配置了 {len(config_sub_urls)} 个已知订阅链接")

            # 0.5. 如果配置了 pinned_url (置顶消息链接), 直接抓取该消息
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
            # 累计有效消息数 (含节点或订阅链接的消息)
            total_valid = rss_result.get("valid_count", 0)
            logger.info(f"[{self.name}] {channel_name}: RSS {total_valid} 条有效消息")

            # 2. HF Space TG Parser API 补充 (CI 配置密钥时有效, 深翻页高效)
            hf_result = await self._fetch_via_hf_api(session, username, channel_name)
            if hf_result["nodes"] or hf_result["sub_urls"]:
                nodes.extend(hf_result["nodes"])
                sub_urls.extend(hf_result["sub_urls"])
                pending_msg_links.extend(hf_result.get("msg_links", []))
                total_valid += hf_result.get("valid_count", 0)
                logger.info(f"[{self.name}] {channel_name}: HF API 补充 {len(hf_result['nodes'])} 个节点, {len(hf_result['sub_urls'])} 个订阅链接, 累计 {total_valid} 条有效消息")

            # 3. 仍不足 min_valid 时, Web 翻页补充历史消息 (无 HF 密钥时的本地深抓取)
            if total_valid < self.min_valid:
                older = await self._fetch_older_messages(session, username, channel_name, self.min_valid)
                if older.get("nodes"):
                    nodes.extend(older["nodes"])
                if older.get("sub_urls"):
                    sub_urls.extend(older["sub_urls"])
                total_valid += older.get("valid_count", 0)
                pending_msg_links.extend(older.get("msg_links", []))
                logger.info(f"[{self.name}] {channel_name}: Web 翻页补充后累计 {total_valid} 条有效消息")

            # 有效消息不足时记录警告
            if total_valid < self.min_valid:
                logger.warning(f"[{self.name}] 频道 {channel_name} 仅 {total_valid} 条有效消息 (目标 {self.min_valid})")

            # 4. 抓取发现的消息链接 (如"点我传送"指向的置顶消息)
            seen_msg_ids = set()
            # 已通过 pinned_url 抓过的消息不再重复
            if pinned_url:
                mid = re.search(r'/(\d+)/?$', pinned_url)
                if mid:
                    seen_msg_ids.add(mid.group(1))
            msg_link_count = 0
            max_msg_links = 10  # 限制消息链接抓取数量, 避免过度请求
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
                # 提取消息链接中的频道名和消息 ID, 用于 HF API
                msg_match = self._TG_MSG_LINK_PATTERN.match(link)
                link_channel = msg_match.group(1) if msg_match else None
                link_msg_id = msg_match.group(2) if msg_match else None

                logger.info(f"[{self.name}] {channel_name}: 抓取消息链接 {link}")
                html = await self._fetch_page(session, link)
                if html:
                    result = self._extract_from_html(html, channel_name)
                    nodes.extend(result["nodes"])
                    sub_urls.extend(result["sub_urls"])
                    msg_link_count += 1
                elif link_channel and link_msg_id:
                    # Web 抓取失败, 尝试 HF API 单条消息接口
                    logger.info(f"[{self.name}] {channel_name}: Web 失败, 尝试 HF API 消息 {link_channel}/{link_msg_id}")
                    hf_msg = await self._fetch_msg_via_hf_api(session, link_channel, link_msg_id)
                    if hf_msg:
                        nodes.extend(hf_msg["nodes"])
                        sub_urls.extend(hf_msg["sub_urls"])
                        msg_link_count += 1
                await asyncio.sleep(1)

            # 4.5. 抓取关联网站 (如频道消息中指向的博客/订阅页面)
            if website_url:
                logger.info(f"[{self.name}] {channel_name}: 抓取关联网站 {website_url}")
                html = await self._fetch_page(session, website_url)
                if html:
                    web_result = self._extract_from_html(html, channel_name)
                    nodes.extend(web_result["nodes"])
                    sub_urls.extend(web_result["sub_urls"])
                    logger.info(f"[{self.name}] {channel_name}: 网站获取 {len(web_result['nodes'])} 个节点, {len(web_result['sub_urls'])} 个订阅链接")

            # 4.6. V2Queen 特殊处理: 从消息 #fragment 中提取 v2clash.blog 日期, 构造订阅 URL (只取最新2个日期)
            #   优先插入 sub_urls 头部, 确保 _fetch_sub_urls 优先抓取
            v2clash_dates = None
            if channel_config.get("v2clash"):
                v2clash_dates = await self._extract_v2clash_date(session, username)
            if v2clash_dates:
                v2clash_subs = []
                for date in v2clash_dates[:2]:
                    v2clash_subs.extend([
                        {"url": f"https://v2clash.blog/Link/{date}-v2ray.txt", "format_hint": "auto"},
                        {"url": f"https://v2clash.blog/Link/{date}-clash.yaml", "format_hint": "clash"},
                    ])
                # 过滤掉已在 sub_urls 中的
                seen_urls = {u["url"] for u in sub_urls}
                new_v2clash = [item for item in v2clash_subs if item["url"] not in seen_urls]
                # 插入头部, 优先抓取
                sub_urls = new_v2clash + sub_urls
                logger.info(f"[{self.name}] {channel_name}: v2clash.blog 日期 {v2clash_dates[:2]}, 构造 {len(new_v2clash)} 个订阅链接 (优先抓取)")

            # 5. 先对直接节点去重, 记录 direct_nodes_count
            if nodes:
                seen_direct = set()
                unique_direct = []
                for node in nodes:
                    # 去重键包含 type + 唯一标识，避免同址不同协议/不同凭据的节点被误并
                    key = (node.type, node.server, node.port, node.uuid or node.password or node.username) if hasattr(node, 'server') and hasattr(node, 'port') else id(node)
                    if key not in seen_direct:
                        seen_direct.add(key)
                        unique_direct.append(node)
                nodes = unique_direct
            direct_nodes_count = len(nodes)

            # 6. 递归 fetch 订阅链接
            sub_urls_valid: List[str] = []
            sub_urls_failed: List[str] = []
            sub_urls_dead: List[str] = []
            if sub_urls:
                # URL 去重
                seen = set()
                unique: List[dict] = []
                for u in sub_urls:
                    url_key = u["url"] if isinstance(u, dict) else u
                    if url_key not in seen:
                        seen.add(url_key)
                        unique.append(u if isinstance(u, dict) else {"url": u, "format_hint": "auto"})
                logger.info(f"[{self.name}] {channel_name}: 发现 {len(unique)} 个订阅链接, 开始递归解析")
                sub_nodes, valid_sub_urls, failed_sub_urls, dead_sub_urls = await self._fetch_sub_urls(session, unique, channel_name, channel_format)
                logger.info(f"[{self.name}] {channel_name}: 订阅链接解析获取 {len(sub_nodes)} 个节点 (成功: {len(valid_sub_urls)}, 失败: {len(failed_sub_urls)}, 永久失效: {len(dead_sub_urls)})")
                nodes.extend(sub_nodes)
                sub_urls_valid = valid_sub_urls
                sub_urls_failed = failed_sub_urls
                sub_urls_dead = dead_sub_urls

            # 6. 频道内节点去重 (同一频道不同订阅源可能包含重复节点)
            if nodes:
                seen_ch = set()
                unique_ch = []
                for node in nodes:
                    # 去重键包含 type + 唯一标识，避免同址不同协议/不同凭据的节点被误并
                    key = (node.type, node.server, node.port, node.uuid or node.password or node.username) if hasattr(node, 'server') and hasattr(node, 'port') else id(node)
                    if key not in seen_ch:
                        seen_ch.add(key)
                        unique_ch.append(node)
                if len(unique_ch) < len(nodes):
                    logger.info(f"[{self.name}] {channel_name}: 频道内去重 {len(nodes)} → {len(unique_ch)} 个节点")
                nodes = unique_ch

        if not nodes:
            logger.warning(f"[{self.name}] 频道 {channel_name} (@{username}) 抓取失败")

        # 记录频道抓取结果
        all_urls = [u["url"] if isinstance(u, dict) else u for u in sub_urls]
        self.channel_results[channel_name] = {
            "nodes": len(nodes),
            "direct_nodes": direct_nodes_count,
            "sub_urls": all_urls,
            "valid_sub_urls": sub_urls_valid,
            "failed_sub_urls": sub_urls_failed,
            "dead_sub_urls": sub_urls_dead,
            "status": "success" if nodes else "empty",
            "updated_at": datetime.now().isoformat(),
        }

        # 永久失效的 URL (404/HTML) 自动加入排除列表, 下次跳过
        for url in sub_urls_dead:
            self._dead_urls.add(url)

        self.mark_source(nodes, f"tg:{channel_name}")
        return nodes

    async def _fetch_older_messages(self, session, username: str, channel_name: str, min_valid: int = None) -> dict:
        """RSS 结果不足时, 通过 Web 预览页翻页获取更多历史消息。
        返回 {"nodes", "sub_urls", "valid_count"}, 与 _fetch_via_rss 一致。
        """
        if min_valid is None:
            min_valid = self.min_valid
        for mirror in WEB_MIRRORS:
            try:
                url = f"{mirror}{username}"
                html = await self._fetch_page(session, url, timeout=10)
                if not html:
                    continue
                result = await self._web_paginate(session, mirror, username, html, channel_name, min_valid)
                if result and (result["nodes"] or result["sub_urls"]):
                    return result
            except Exception:
                continue
        return {"nodes": [], "sub_urls": [], "valid_count": 0, "msg_links": []}

    async def _web_paginate(self, session, mirror: str, username: str, first_html: str, channel_name: str, min_valid: int = None) -> Optional[dict]:
        """Web 预览页翻页 — 通过 ?before= 消息 ID 加载更早的消息。
        持续翻页直到收集到 min_valid 条有效消息 (含节点或订阅链接), 或翻完 max_web_pages 页。
        """
        if min_valid is None:
            min_valid = self.min_valid
        all_nodes = []
        all_sub_urls = []
        all_msg_links = []
        valid_count = 0
        html = first_html
        max_pages = self.max_web_pages

        for page in range(max_pages):
            # 从页面提取消息 ID (data-post="channel/12345")
            msg_ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
            if not msg_ids:
                break

            # 提取当前页内容
            result = self._extract_from_html(html, channel_name)
            all_nodes.extend(result["nodes"])
            all_sub_urls.extend(result["sub_urls"])
            all_msg_links.extend(result.get("msg_links", []))
            valid_count += result.get("valid_count", 0)

            logger.info(f"[{self.name}] {channel_name}: Web 翻页 {page+1} 累计 {valid_count} 条有效消息")
            # 达到目标有效消息数, 停止翻页
            if valid_count >= min_valid:
                break

            # 用最早的消息 ID 翻页
            oldest_id = min(int(mid) for mid in msg_ids)
            next_url = f"{mirror}{username}?before={oldest_id}"
            html = await self._fetch_page(session, next_url, timeout=10)
            if not html:
                break
            await asyncio.sleep(1)

        if not all_nodes and not all_sub_urls:
            return None
        # 去重消息链接
        seen_links = set()
        unique_msg_links = []
        for link in all_msg_links:
            if link not in seen_links:
                seen_links.add(link)
                unique_msg_links.append(link)
        return {"nodes": all_nodes, "sub_urls": all_sub_urls, "valid_count": valid_count, "msg_links": unique_msg_links}

    async def _fetch_via_rss(self, session, username: str, channel_name: str) -> dict:
        """通过 RSS API 获取频道消息并提取节点和订阅链接
        带重试: tg.i-c-a.su 批量请求时可能 FLOOD_WAIT 限流/响应非 XML, 重试几次。
        带本地缓存: 短时间内的重复请求 (本地调试/多次运行) 直接读缓存, 降低限流触发率。
        """
        rss_url = f"{RSS_API_BASE}{username}"

        # 尝试读缓存 (TTL 内直接复用)
        cached = self._rss_cache_get(username)
        if cached is not None:
            logger.debug(f"[{self.name}] {channel_name}: RSS 命中缓存 (TTL={self.rss_cache_ttl}s)")
            return self._extract_from_rss(cached, channel_name)

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, text/html",
        }
        # 限流/失败时重试 (FLOOD_WAIT 通常需要等待 10-30 秒)
        max_retries = 3
        backoff = [3, 8, 15]
        for attempt in range(max_retries):
            try:
                async with session.get(rss_url, headers=headers, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    body = await resp.text()
                    if resp.status != 200:
                        logger.debug(f"RSS HTTP {resp.status} for {rss_url} (attempt {attempt + 1})")
                        if resp.status == 429 or "FLOOD_WAIT" in body:
                            wait = self._flood_wait_seconds(body, backoff, attempt)
                            await asyncio.sleep(wait)
                            continue
                        return {"nodes": [], "sub_urls": [], "msg_links": [], "valid_count": 0}
                    if not body or "<rss" not in body:
                        # 非 XML (限流 JSON/FLOOD_WAIT), 等待后重试
                        logger.debug(f"RSS 响应非 XML for {rss_url} (attempt {attempt + 1}): {body[:100]}")
                        if "FLOOD_WAIT" in body:
                            wait = self._flood_wait_seconds(body, backoff, attempt)
                            await asyncio.sleep(wait)
                            continue
                        return {"nodes": [], "sub_urls": [], "msg_links": [], "valid_count": 0}
                    # 成功后写入缓存
                    self._rss_cache_put(username, body)
                    return self._extract_from_rss(body, channel_name)
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.debug(f"RSS 抓取 {rss_url} 失败 (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff[attempt] if attempt < len(backoff) else 20)
        logger.debug(f"RSS 多次重试仍失败: {rss_url}")
        return {"nodes": [], "sub_urls": [], "msg_links": [], "valid_count": 0}

    # ── RSS 本地缓存 ──
    _RSS_CACHE_DIR = Path("data/cache/rss_cache")

    def _rss_cache_get(self, username: str) -> Optional[str]:
        """读 RSS 缓存, 未命中或过期返回 None"""
        try:
            cache_file = self._RSS_CACHE_DIR / f"{re.sub(r'[^A-Za-z0-9_]', '', username)}.json"
            if not cache_file.exists():
                return None
            with open(cache_file, "r", encoding="utf-8") as f:
                data = _json.load(f)
            age = (datetime.now() - datetime.fromisoformat(data["ts"])).total_seconds()
            if age > self.rss_cache_ttl:
                return None
            return data["xml"]
        except Exception:
            return None

    def _rss_cache_put(self, username: str, xml: str):
        """写 RSS 缓存"""
        try:
            self._RSS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_file = self._RSS_CACHE_DIR / f"{re.sub(r'[^A-Za-z0-9_]', '', username)}.json"
            with open(cache_file, "w", encoding="utf-8") as f:
                _json.dump({"ts": datetime.now().isoformat(), "xml": xml}, f)
        except Exception:
            pass

    def _flood_wait_seconds(self, body: str, backoff: list, attempt: int) -> int:
        """解析 FLOOD_WAIT_N 中的等待秒数, 不足时取退避值。
        等待时间封顶 60 秒, 防止第三方 RSS API 返回异常大值拖垮整个管道。
        """
        m = re.search(r"FLOOD_WAIT_(\d+)", body)
        if m:
            return min(int(m.group(1)), 60)
        return backoff[attempt] if attempt < len(backoff) else 20

    async def _fetch_via_hf_api(self, session, username: str, channel_name: str) -> dict:
        """通过 HF Space TG Parser API 获取频道消息 (替代被屏蔽的 Web 镜像)
        自动跳过纯广告消息, 只保留含有效订阅/节点内容的消息。
        分页请求, 确保每个频道至少收集到 min_valid 条有效消息。
        """
        if not HF_TG_PARSER_KEY:
            return {"nodes": [], "sub_urls": [], "msg_links": [], "valid_count": 0}
        min_valid = self.min_valid  # 目标: 至少 N 条有效消息 (默认 50)
        page_size = 100  # 每页最多 100 条 (HF API 限制)
        max_pages = max(5, (min_valid * 2) // page_size + 1)  # 足够翻页到 min_valid
        logger.info(f"[{self.name}] 尝试 HF TG API: channel={username} (目标 {min_valid} 条有效消息)")

        all_nodes = []
        all_sub_urls: List[dict] = []
        all_msg_links: List[str] = []
        all_doc_msgs: List[dict] = []  # {"channel", "msg_id", "filename"}
        valid_count = 0
        seen_msg_ids = set()
        last_msg_id = 0  # 翻页游标 (offset_id)

        for page in range(max_pages):
            # HF API 分页: 用 offset_id (消息 id 游标), 非页码偏移
            params = f"channel={username}&limit={page_size}&key={HF_TG_PARSER_KEY}"
            if last_msg_id > 0:
                params += f"&offset_id={last_msg_id}"
            real_url = f"{HF_TG_PARSER_BASE}?{params}"

            try:
                async with session.get(real_url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug(f"HF TG API HTTP {resp.status} for {username} page {page+1}")
                        break
                    import json as _json
                    raw_text = await resp.text()
                    try:
                        data = _json.loads(raw_text)
                    except _json.JSONDecodeError:
                        break
                    if isinstance(data, dict):
                        messages = data.get("messages", [])
                    elif isinstance(data, list):
                        messages = data
                    else:
                        break
                    if not messages:
                        break
            except Exception as e:
                logger.debug(f"HF TG API 失败 {username} page {page+1}: {e}")
                break

            # 解析本页消息
            page_nodes = 0
            page_valid = 0
            for msg in messages:
                # 去重: 跳过已处理的消息
                msg_id = msg.get("id") if isinstance(msg, dict) else None
                if msg_id and msg_id in seen_msg_ids:
                    continue
                if msg_id:
                    seen_msg_ids.add(msg_id)

                text = msg.get("text", "") if isinstance(msg, dict) else str(msg)
                # 检测文档附件 (节点文件, 只收集文本格式; 用 msg_id 走 HF download 端点下载)
                media = msg.get("media") if isinstance(msg, dict) else None
                if media and isinstance(media, dict) and media.get("type") == "document":
                    filename = media.get("filename", "")
                    text_exts = (".txt", ".yaml", ".yml", ".json", ".base64", ".conf", ".list", ".md")
                    if filename and any(filename.lower().endswith(ext) for ext in text_exts):
                        all_doc_msgs.append({"channel": username, "msg_id": msg_id, "filename": filename})
                if not text and not media:
                    continue
                nodes = self._extract_nodes_from_text(text) if text else []
                sub_urls = self._extract_sub_urls(text) if text else []
                msg_links = self._extract_msg_links(text) if text else []
                # 完全无内容的消息跳过 (广告/纯文字, 无文档)
                if not nodes and not sub_urls and not msg_links and not media:
                    continue
                all_nodes.extend(nodes)
                all_sub_urls.extend(sub_urls)
                all_msg_links.extend(msg_links)
                page_nodes += len(nodes)
                # 有效消息 = 包含直接节点 或 包含订阅链接
                if nodes or sub_urls:
                    valid_count += 1
                    page_valid += 1

            logger.info(f"[{self.name}] HF TG API {channel_name} page {page+1}: {len(messages)} 条消息, {page_valid} 条有效, {page_nodes} 个节点")

            # 更新翻页游标 (本页最早的消息 id)
            page_ids = [m.get("id") for m in messages if isinstance(m, dict) and m.get("id")]
            if page_ids:
                last_msg_id = min(page_ids)

            # 已达到目标有效消息数, 停止翻页
            if valid_count >= min_valid:
                break
            # 本页无有效消息且已翻过至少 2 页, 停止
            if page_valid == 0 and page >= 1:
                break

        # 下载文档附件中的节点 (通过 HF download 端点, 无需自己的 Bot)
        if all_doc_msgs:
            doc_nodes = await self._download_telegram_docs(session, all_doc_msgs)
            if doc_nodes:
                logger.info(f"[{self.name}] {channel_name}: 从 {len(all_doc_msgs)} 个文档中获取 {len(doc_nodes)} 个节点")
                all_nodes.extend(doc_nodes)
                valid_count += 1  # 含文档节点的消息计为有效消息
        # 去重
        seen = set()
        unique_subs: List[dict] = []
        for u in all_sub_urls:
            if u["url"] not in seen:
                seen.add(u["url"])
                unique_subs.append(u)
        seen_links = set()
        unique_msgs = []
        for l in all_msg_links:
            if l not in seen_links:
                seen_links.add(l)
                unique_msgs.append(l)
        logger.info(f"[{self.name}] HF TG API {channel_name}: 共 {valid_count} 条有效消息, {len(all_nodes)} 节点, {len(unique_subs)} 订阅链接")
        return {"nodes": all_nodes, "sub_urls": unique_subs, "msg_links": unique_msgs, "valid_count": valid_count}

    async def _download_telegram_docs(self, session, doc_msgs: List[dict]) -> List:
        """通过 HF Space TG Parser API 的 /tg/download 端点下载文档附件并解析节点

        HF Space 用自己的 Telethon 客户端抓取媒体文件, 返回原始文件内容。
        doc_msgs 为 [{"channel", "msg_id", "filename"}] 列表。
        不使用自己的 Bot (file_id 属于抓取 Bot, 我们的 Bot 无法下载)。
        """
        if not HF_TG_PARSER_KEY:
            return []
        all_nodes = []
        seen_ids = set()
        for doc in doc_msgs[:3]:  # 最多尝试3个文档
            msg_key = (doc.get("channel"), doc.get("msg_id"))
            if msg_key in seen_ids:
                continue
            seen_ids.add(msg_key)
            channel = doc.get("channel", "")
            msg_id = doc.get("msg_id")
            filename = doc.get("filename", "")
            if not channel or not msg_id:
                continue
            dl_url = f"{HF_TG_PARSER_BASE.replace('/tg/history', '/tg/download')}?channel={channel}&msg_id={msg_id}&key={HF_TG_PARSER_KEY}"
            try:
                async with session.get(dl_url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug(f"HF 文档下载失败 HTTP {resp.status}: {filename}")
                        continue
                    content = await resp.text()
                    if not content or len(content) < 10:
                        continue
                    # 解析文件内容
                    nodes = self.parser.parse(content, "auto")
                    if nodes:
                        logger.info(f"[{self.name}] 文档 {filename}: 解析 {len(nodes)} 个节点")
                        all_nodes.extend(nodes)
                    else:
                        logger.debug(f"文档 {filename}: 无有效节点")
            except Exception as e:
                logger.debug(f"文档下载失败 {filename}: {e}")
        return all_nodes

    async def _fetch_msg_via_hf_api(self, session, username: str, msg_id: str) -> Optional[dict]:
        """通过 HF Space TG Parser API 获取单条消息 (替代 Web 抓取消息链接)"""
        if not HF_TG_PARSER_KEY:
            return None
        api_url = f"https://aurora0722-tg-parser-api.hf.space/tg/message?channel={username}&msg_id={msg_id}&key={HF_TG_PARSER_KEY}"
        try:
            async with session.get(api_url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if isinstance(data, dict) and data.get("code") == 0:
                    msg = data.get("message", {})
                    text = msg.get("text", "") if isinstance(msg, dict) else ""
                    if text:
                        return {
                            "nodes": self._extract_nodes_from_text(text),
                            "sub_urls": self._extract_sub_urls(text),
                        }
                return None
        except Exception:
            return None

    async def _extract_v2clash_date(self, session, username: str) -> Optional[List[str]]:
        """从频道最新消息中提取 v2clash.blog 日期, 返回 YYYYMMDD 格式列表 (最新2个)"""
        if not HF_TG_PARSER_KEY:
            return None
        api_url = f"{HF_TG_PARSER_BASE}?channel={username}&limit=10&key={HF_TG_PARSER_KEY}"
        try:
            async with session.get(api_url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                messages = data.get("messages", []) if isinstance(data, dict) else []
                dates = []
                for msg in messages:
                    text = msg.get("text", "") if isinstance(msg, dict) else ""
                    # 匹配 v2clash.blog%202026-6.9 或 v2clash.blog 2026-6.9
                    match = re.search(r'v2clash\.blog(?:%20|\s+)(\d{4})-(\d+)\.(\d+)', text)
                    if match:
                        year, month, day = match.group(1), match.group(2), match.group(3)
                        date_str = f"{year}{int(month):02d}{int(day):02d}"
                        if date_str not in dates:
                            dates.append(date_str)
                        if len(dates) >= 2:
                            break
                return dates if dates else None
        except Exception:
            return None

    # Telegram 频道消息链接匹配 (t.me/频道名/消息ID)
    _TG_MSG_LINK_PATTERN = re.compile(r'https?://(?:t\.me|telegram\.dog|telegram\.me)/([a-zA-Z0-9_]+)/(\d+)')

    def _extract_from_rss(self, xml_content: str, channel_name: str) -> dict:
        """从 RSS XML 中提取节点、订阅链接和频道内消息链接"""
        nodes = []
        sub_urls: List[dict] = []
        msg_links: List[str] = []  # t.me 频道内消息链接
        valid_count = 0  # 含节点或订阅链接的有效消息数
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.debug(f"RSS XML 解析失败: {e}")
            return {"nodes": [], "sub_urls": [], "msg_links": [], "valid_count": 0}

        for item in root.iter("item"):
            item_start_nodes = len(nodes)
            item_start_subs = len(sub_urls)

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
                    # 协议节点链接 (vmess://, vless://, trojan://, ss:// 等)
                    elif any(href.startswith(proto) for proto in ("vmess://", "vless://", "trojan://", "ss://", "ssr://", "hysteria2://", "hy2://", "anytls://")):
                        href_nodes = self._extract_nodes_from_text(href)
                        if href_nodes:
                            nodes.extend(href_nodes)
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

            # 该条消息含节点或订阅链接 → 记为有效消息
            if len(nodes) > item_start_nodes or len(sub_urls) > item_start_subs:
                valid_count += 1

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

        return {"nodes": nodes, "sub_urls": unique_sub_urls, "msg_links": unique_msg_links, "valid_count": valid_count}

    async def _fetch_page(self, session, url: str, timeout: int = 30) -> Optional[str]:
        # SSRF 防护：拒绝内网/回环/链路本地等保留地址
        if not self._is_safe_url(url):
            logger.debug(f"跳过不安全 URL (SSRF 防护): {url}")
            return None
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with session.get(url, headers=headers, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
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
        valid_count = 0  # 含节点或订阅链接的有效消息数

        # <meta> 标签 (og:description / twitter:description 包含消息摘要)
        # 注意: meta 描述的是页面第一条消息, 与下方消息 div 重复, 只提取不计数
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
            # 协议节点链接 (vmess://, vless://, trojan://, ss:// 等)
            elif any(href.startswith(proto) for proto in ("vmess://", "vless://", "trojan://", "ss://", "ssr://", "hysteria2://", "hy2://", "anytls://")):
                href_nodes = self._extract_nodes_from_text(href)
                if href_nodes:
                    nodes.extend(href_nodes)
            else:
                item = self._clean_sub_url(href)
                if item:
                    seen = {u["url"] for u in sub_urls}
                    if item["url"] not in seen:
                        sub_urls.append(item)

        # 消息文本 (每条消息一个 div, 含节点或订阅链接即计为有效消息)
        for msg_html in re.compile(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', msg_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").replace("&quot;", '"')
            n = self._extract_nodes_from_text(text)
            s = self._extract_sub_urls(text)
            if n or s:
                valid_count += 1
            nodes.extend(n)
            sub_urls.extend(s)

        # <pre> 标签 (Base64 订阅内容 或 订阅链接)
        for pre_html in re.compile(r'<pre[^>]*>(.*?)</pre>', re.DOTALL).findall(html):
            text = re.sub(r'<[^>]+>', '', pre_html)
            text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            # 先尝试解析为 Base64/Clash 等格式
            pre_yielded = False
            try:
                pre_nodes = self.parser.parse(text, "auto")
                if pre_nodes:
                    nodes.extend(pre_nodes)
                    pre_yielded = True
            except Exception:
                pass
            if not pre_yielded:
                # 不是节点格式, 尝试提取订阅链接
                pre_urls = self._extract_sub_urls(text)
                seen_urls = {u["url"] if isinstance(u, dict) else u for u in sub_urls}
                for url_item in pre_urls:
                    url_key = url_item["url"] if isinstance(url_item, dict) else url_item
                    if url_key not in seen_urls:
                        sub_urls.insert(0, url_item)
                        seen_urls.add(url_key)
                        pre_yielded = True
                n = self._extract_nodes_from_text(text)
                if n:
                    nodes.extend(n)
                    pre_yielded = True
            if pre_yielded:
                valid_count += 1

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

        return {"nodes": nodes, "sub_urls": unique_sub_urls, "msg_links": unique_msg_links, "valid_count": valid_count}

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
    # 注意: 只用具体片段, 避免 "ad"/"ads" 这类子串误伤合法 URL
    # (如 .../download 中含 "ad", .../read/ 中含 "ad")
    _EXCLUDED_PATH_KEYWORDS = {
        "/ad/", "/ads/", "advert", "adserver", "adservice",
        "adclick", "doubleclick", "googlesyndication",
        "invite", "register", "signup",
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
            # 剥离尾部不合法的 ASCII 标点 (如右括号、逗号、反引号等)
            url = re.sub(r'[,;。，；）)》】`]+$', '', url)
            if not url or len(url) < 10:
                continue
            # 排除社交/广告域名
            if any(url.startswith(f"https://{d}") for d in self._EXCLUDED_SUB_DOMAINS):
                continue
            # 排除广告域名前缀 (ad.example.com / ads.xxx / adserver.xxx / 统计/追踪域名)
            hostname = urlparse(url).hostname or ""
            if hostname.startswith(("ad.", "ads.", "adserver.", "adservice.", "adtrack.", "tracking.", "analytics.", "googletagmanager.", "doubleclick.")):
                continue
            # 排除图片/文件等非订阅链接
            if any(url.endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".mp4", ".pdf", ".apk", ".exe")):
                continue
            # 排除 tg.i-c-a.su 媒体链接
            if "/media/" in url and "tg.i-c-a.su" in url:
                continue
            # 排除广告/注册/机场推广链接 (检查路径+fragment, 不检查 query 参数, 避免 token 误匹配)
            parsed_lower = urlparse(url)
            url_check = (parsed_lower.path + "#" + parsed_lower.fragment).lower()
            if any(kw in url_check for kw in self._EXCLUDED_PATH_KEYWORDS):
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

    def _is_safe_url(self, url: str) -> bool:
        """SSRF 防护：拒绝内网 / 回环 / 链路本地 / 保留地址。

        仅放行公开可访问的地址；IP 字面量形式的私有地址一律拒绝，
        域名形式额外拒绝 localhost。抓取方无法解析时也会被拒绝。
        """
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        if not host:
            return False
        try:
            ip = ipaddress.ip_address(host)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local
                        or ip.is_reserved or ip.is_multicast or ip.is_unspecified)
        except ValueError:
            pass
        if host == "localhost" or host.endswith(".localhost"):
            return False
        return True

    def _clean_sub_url(self, raw_url: str) -> Optional[dict]:
        """清理单个 URL 并返回 {"url": str, "format_hint": str} 或 None"""
        url = re.sub(r'[^\x21-\x7E]+.*$', '', raw_url)
        url = re.sub(r'[),;。，；）》】`]+$', '', url)
        # 只接受绝对 https:// URL (排除相对路径, //cdn 等)
        if not url or not url.startswith("https://"):
            return None
        if len(url) < 12:
            return None
        # 排除非订阅链接
        if any(url.startswith(f"https://{d}") for d in self._EXCLUDED_SUB_DOMAINS):
            return None
        # 排除广告域名前缀
        hostname = urlparse(url).hostname or ""
        if hostname.startswith(("ad.", "ads.", "adserver.", "adservice.", "adtrack.", "tracking.", "analytics.", "googletagmanager.", "doubleclick.")):
            return None
        if any(url.endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".mp4", ".pdf", ".apk", ".exe")):
            return None
        if "/media/" in url and "tg.i-c-a.su" in url:
            return None
        parsed_lower = urlparse(url)
        url_check = (parsed_lower.path + "#" + parsed_lower.fragment).lower()
        if any(kw in url_check for kw in self._EXCLUDED_PATH_KEYWORDS):
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

    # 死链接缓存文件
    _DEAD_URLS_FILE = Path("data/cache/dead_sub_urls.json")

    def _load_dead_urls(self) -> set:
        """从文件加载已知死链接"""
        default = {"https://go4sharing.github.io"}
        if not self._DEAD_URLS_FILE.exists():
            return default
        try:
            import json as _json
            with open(self._DEAD_URLS_FILE, "r", encoding="utf-8") as f:
                saved = _json.load(f)
            return default | set(saved)
        except Exception:
            return default

    def _save_dead_urls(self, dead_urls: set):
        """持久化死链接到文件"""
        try:
            import json as _json
            self._DEAD_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self._DEAD_URLS_FILE, "w", encoding="utf-8") as f:
                _json.dump(sorted(dead_urls), f, indent=2)
        except Exception:
            pass

    async def _fetch_sub_urls(
        self,
        session,
        sub_urls: List[dict],
        channel_name: str,
        channel_format: str = "auto",
    ) -> tuple:
        """递归 fetch 订阅链接并解析节点

        sub_urls 为 [{"url": str, "format_hint": str}] 列表。
        优先使用 format_hint, 其次使用 channel_format, 最终降级为 auto。
        返回 (nodes, valid_urls, failed_urls, dead_urls):
          - valid_urls: 产生节点的 URL
          - failed_urls: 未产生节点但可能临时失败 (超时/解析失败), 下次可重试
          - dead_urls: 永久失效 (404/HTML), 自动加入排除列表
        """
        all_nodes = []
        valid_urls: List[str] = []
        failed_urls: List[str] = []
        dead_urls: List[str] = []
        max_sub = 10
        max_conc = int(self.config.get("max_concurrent_subs", 5))

        async def _fetch_one(item) -> tuple:
            """返回 (nodes, status), status: valid/failed/dead"""
            url = item["url"] if isinstance(item, dict) else item
            # SSRF 防护：拒绝内网/回环/链路本地等保留地址
            if not self._is_safe_url(url):
                logger.debug(f"跳过不安全 URL (SSRF 防护): {url}")
                return [], "dead"
            # 跳过已知死链接
            if url in self._dead_urls:
                logger.debug(f"订阅链接在排除列表中, 跳过: {url}")
                return [], "dead"
            hint = item.get("format_hint", "auto") if isinstance(item, dict) else "auto"
            parse_format = hint if hint != "auto" else channel_format
            try:
                logger.info(f"[{self.name}] {channel_name}: fetch 订阅链接 {url} (format={parse_format})")
                async with session.get(url, proxy=self.proxy, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.debug(f"订阅链接 HTTP {resp.status}: {url}")
                        return [], "dead" if resp.status == 404 else "failed"
                    content = await resp.text()
                    if not content or len(content) < 20:
                        return [], "failed"
                    content_stripped = content.strip()
                    if content_stripped.startswith("<!DOCTYPE") or content_stripped.startswith("<html"):
                        # HTML 可能是 JS 挑战/限流页, 也可能是 404 页。不永久拉黑,
                        # 标记为临时失败, 下次运行重试 (避免误伤合法的文件型订阅源)
                        logger.debug(f"订阅链接返回 HTML 页面 (JS挑战/404), 暂记失败: {url}")
                        return [], "failed"
                    nodes = self.parser.parse(content, parse_format)
                    if nodes:
                        logger.info(f"[{self.name}] {channel_name}: 订阅链接解析 {len(nodes)} 个节点: {url}")
                        return nodes, "valid"
                    logger.debug(f"订阅链接无有效节点: {url}")
                    return [], "failed"
            except Exception as e:
                logger.debug(f"订阅链接 fetch 失败 {url}: {e}")
                return [], "failed"

        sem = asyncio.Semaphore(max_conc)

        async def _worker(item) -> tuple:
            async with sem:
                nodes, status = await _fetch_one(item)
                url = item["url"] if isinstance(item, dict) else item
                return nodes, status, url

        results = await asyncio.gather(*[_worker(item) for item in sub_urls[:max_sub]])
        for nodes, status, url in results:
            all_nodes.extend(nodes)
            if status == "valid":
                valid_urls.append(url)
            elif status == "dead":
                dead_urls.append(url)
            else:
                failed_urls.append(url)
        return all_nodes, valid_urls, failed_urls, dead_urls

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
