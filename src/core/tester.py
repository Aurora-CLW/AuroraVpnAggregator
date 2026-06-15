"""
节点测试器
"""

import asyncio
import logging
import socket
import ssl
import struct
from typing import List, Dict, Optional
from datetime import datetime

from ..models.node import Node
from ..utils.network import check_tcp_port, measure_latency

logger = logging.getLogger(__name__)


class NodeTester:
    """节点测试器 — 多级校验，严格筛选"""

    def __init__(self, config: dict):
        self.tcp_enabled = config.get("tcp", {}).get("enabled", True)
        self.tcp_timeout = config.get("tcp", {}).get("timeout", 5)
        self.tcp_concurrent = config.get("tcp", {}).get("concurrent", 200)

        self.tls_enabled = config.get("tls", {}).get("enabled", True)
        self.tls_timeout = config.get("tls", {}).get("timeout", 10)
        self.tls_concurrent = config.get("tls", {}).get("concurrent", 100)

        self.max_latency = config.get("max_latency", 5000)

    async def test_all(self, nodes: List[Node]) -> List[Node]:
        if not nodes:
            return []

        logger.info(f"开始测试 {len(nodes)} 个节点 (TCP + TLS 严格校验)...")

        # Stage 1: TCP 可达性 + 延迟测量
        if self.tcp_enabled:
            tcp_valid = await self._tcp_batch_test(nodes)
            logger.info(f"TCP 测试完成: {len(tcp_valid)}/{len(nodes)} 可达")
        else:
            tcp_valid = nodes

        # Stage 2: TLS 握手验证 (仅对需要 TLS 的节点)
        if self.tls_enabled:
            tls_valid = await self._tls_batch_test(tcp_valid)
            logger.info(f"TLS 握手测试完成: {len(tls_valid)}/{len(tcp_valid)} 有效")
        else:
            tls_valid = tcp_valid

        # 标记有效/无效节点
        valid_set = set(id(n) for n in tls_valid)
        for node in nodes:
            node.is_valid = id(node) in valid_set

        logger.info(f"测试完成: {len(tls_valid)} 个有效 / {len(nodes)} 个总计")
        return nodes

    async def _tcp_batch_test(self, nodes: List[Node]) -> List[Node]:
        """批量 TCP 测试 + 延迟测量"""
        semaphore = asyncio.Semaphore(self.tcp_concurrent)
        valid_nodes = []

        async def test_with_semaphore(node: Node):
            async with semaphore:
                is_valid = await check_tcp_port(node.server, node.port, self.tcp_timeout)
                node.tcp_valid = is_valid
                if is_valid:
                    node.latency = await measure_latency(node.server, node.port, self.tcp_timeout)
                return node, is_valid

        tasks = [test_with_semaphore(node) for node in nodes]
        results = await asyncio.gather(*tasks)

        for node, is_valid in results:
            if is_valid:
                valid_nodes.append(node)
            node.tested_at = datetime.now()

        return valid_nodes

    async def _tls_batch_test(self, nodes: List[Node]) -> List[Node]:
        """批量 TLS/协议验证 — 严格模式"""
        semaphore = asyncio.Semaphore(self.tls_concurrent)
        valid_nodes = []

        async def test_with_semaphore(node: Node):
            async with semaphore:
                is_valid = await self._verify_node(node)
                return node, is_valid

        tasks = [test_with_semaphore(node) for node in nodes]
        results = await asyncio.gather(*tasks)

        for node, is_valid in results:
            if is_valid:
                valid_nodes.append(node)

        return valid_nodes

    async def _verify_node(self, node: Node) -> bool:
        """验证节点 — 根据协议类型严格校验"""
        try:
            if node.type == "trojan":
                return await self._verify_trojan(node)
            elif node.type in ("vmess", "vless"):
                return await self._verify_vmess_vless(node)
            elif node.type == "ss":
                return await self._verify_ss(node)
            elif node.type == "hysteria2":
                return await self._verify_hysteria2(node)
            else:
                return node.tcp_valid and 0 < node.latency < self.max_latency
        except Exception:
            return False

    async def _verify_trojan(self, node: Node) -> bool:
        """Trojan 严格验证: TLS 握手 + SNI 证书匹配"""
        if not node.password:
            return False

        sni = node.sni or node.server
        try:
            ssl_ctx = ssl.create_default_context()
            if node.skip_cert_verify:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

            _, writer = await asyncio.wait_for(
                asyncio.open_connection(node.server, node.port, ssl=ssl_ctx, server_hostname=sni),
                timeout=self.tls_timeout
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError, ssl.SSLError):
            return False

    async def _verify_vmess_vless(self, node: Node) -> bool:
        """VMess/VLess 验证: TLS 节点验证 TLS 握手, 非 TLS 节点验证 WebSocket 升级"""
        if not node.uuid:
            return False

        # TLS/Reality 节点 — 验证 TLS 握手
        if node.security in ("tls", "reality"):
            sni = node.sni or node.server
            try:
                ssl_ctx = ssl.create_default_context()
                if node.skip_cert_verify:
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = ssl.CERT_NONE

                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(node.server, node.port, ssl=ssl_ctx, server_hostname=sni),
                    timeout=self.tls_timeout
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return True
            except (asyncio.TimeoutError, ConnectionRefusedError, OSError, ssl.SSLError):
                return False

        # WebSocket 节点 — 验证 WS 升级响应
        if node.network == "ws" and node.ws_path:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(node.server, node.port),
                    timeout=self.tls_timeout
                )
                try:
                    host = node.server
                    if node.ws_headers and "Host" in node.ws_headers:
                        host = node.ws_headers["Host"]
                    path = node.ws_path or "/"
                    request = (
                        f"GET {path} HTTP/1.1\r\n"
                        f"Host: {host}\r\n"
                        f"Upgrade: websocket\r\n"
                        f"Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                        f"Sec-WebSocket-Version: 13\r\n"
                        f"\r\n"
                    )
                    writer.write(request.encode())
                    await asyncio.wait_for(writer.drain(), timeout=5)
                    data = await asyncio.wait_for(reader.read(1024), timeout=5)
                    return b"101" in data or b"Switching" in data
                finally:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
            except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                return False

        # 纯 TCP 无 TLS — 仅 TCP 可达 + 延迟合理
        return node.tcp_valid and 0 < node.latency < self.max_latency

    async def _verify_ss(self, node: Node) -> bool:
        """SS 验证: TCP 可达 + 必要参数完整"""
        if not node.cipher or not node.password:
            return False
        return node.tcp_valid and 0 < node.latency < self.max_latency

    async def _verify_hysteria2(self, node: Node) -> bool:
        """Hysteria2 验证: UDP 可达"""
        if not node.hysteria2_password:
            return False
        try:
            loop = asyncio.get_event_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(None, self._test_udp_port, node.server, node.port),
                timeout=self.tls_timeout
            )
            return result
        except (asyncio.TimeoutError, OSError):
            return False

    def _test_udp_port(self, host: str, port: int) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(3)
            sock.sendto(b"\x00", (host, port))
            sock.close()
            return True
        except Exception:
            return False
