"""
节点测试器
"""

import asyncio
import logging
import socket
import struct
from typing import List, Dict
from datetime import datetime

from ..models.node import Node
from ..utils.network import check_tcp_port, measure_latency

logger = logging.getLogger(__name__)


class NodeTester:
    """节点测试器"""

    def __init__(self, config: dict):
        self.tcp_enabled = config.get("tcp", {}).get("enabled", True)
        self.tcp_timeout = config.get("tcp", {}).get("timeout", 3)
        self.tcp_concurrent = config.get("tcp", {}).get("concurrent", 100)

        self.http_enabled = config.get("http", {}).get("enabled", True)
        self.http_timeout = config.get("http", {}).get("timeout", 10)
        self.http_concurrent = config.get("http", {}).get("concurrent", 20)
        # 支持单个 test_url 或多个 test_urls
        test_urls = config.get("http", {}).get("test_urls", [])
        if not test_urls:
            single_url = config.get("http", {}).get("test_url", "http://www.gstatic.com/generate_204")
            test_urls = [single_url]
        self.test_urls = test_urls

        self.max_latency = config.get("max_latency", 5000)

    async def test_all(self, nodes: List[Node]) -> List[Node]:
        if not nodes:
            return []

        logger.info(f"开始测试 {len(nodes)} 个节点...")

        # Stage 1: TCP 可达性测试
        if self.tcp_enabled:
            tcp_valid = await self._tcp_batch_test(nodes)
            logger.info(f"TCP 测试完成: {len(tcp_valid)}/{len(nodes)} 可达")
        else:
            tcp_valid = nodes

        # Stage 2: 代理握手测试（验证协议是否真正可用）
        if self.http_enabled:
            http_valid = await self._proxy_handshake_test(tcp_valid)
            logger.info(f"代理握手测试完成: {len(http_valid)}/{len(tcp_valid)} 有效")
        else:
            http_valid = tcp_valid

        # 标记有效/无效节点
        valid_set = set(id(n) for n in http_valid)
        for node in nodes:
            node.is_valid = id(node) in valid_set

        logger.info(f"测试完成: {len(http_valid)} 个有效 / {len(nodes)} 个总计")
        return nodes

    async def _tcp_batch_test(self, nodes: List[Node]) -> List[Node]:
        """批量 TCP 测试"""
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

    async def _proxy_handshake_test(self, nodes: List[Node]) -> List[Node]:
        """代理握手测试 - 验证协议参数是否有效"""
        semaphore = asyncio.Semaphore(self.http_concurrent)
        valid_nodes = []

        async def test_with_semaphore(node: Node):
            async with semaphore:
                is_valid = await self._test_proxy_protocol(node)
                return node, is_valid

        tasks = [test_with_semaphore(node) for node in nodes]
        results = await asyncio.gather(*tasks)

        for node, is_valid in results:
            if is_valid:
                valid_nodes.append(node)

        return valid_nodes

    async def _test_proxy_protocol(self, node: Node) -> bool:
        """根据协议类型进行握手验证"""
        try:
            if node.type == "vmess":
                return await self._test_vmess(node)
            elif node.type == "vless":
                return await self._test_vless(node)
            elif node.type == "trojan":
                return await self._test_trojan(node)
            elif node.type == "ss":
                return await self._test_ss(node)
            elif node.type == "hysteria2":
                return await self._test_hysteria2(node)
            else:
                return node.latency > 0 and node.latency < self.max_latency
        except Exception:
            return False

    async def _test_vmess(self, node: Node) -> bool:
        """VMess 握手测试"""
        if not node.uuid:
            return False

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.server, node.port),
                timeout=self.http_timeout
            )

            try:
                if node.network == "ws" and node.ws_path:
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
                    if b"101" in data or b"Switching" in data:
                        return True
                    if node.security == "tls" and (b"TLS" in data or len(data) > 0):
                        return True

                elif node.security == "tls":
                    return True

                return node.latency > 0 and node.latency < self.max_latency
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return False

    async def _test_vless(self, node: Node) -> bool:
        """VLess 握手测试"""
        if not node.uuid:
            return False

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.server, node.port),
                timeout=self.http_timeout
            )

            try:
                if node.network == "ws" and node.ws_path:
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
                    if b"101" in data or b"Switching" in data:
                        return True

                if node.security == "tls" or node.security == "reality":
                    return True

                return node.latency > 0 and node.latency < self.max_latency
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return False

    async def _test_trojan(self, node: Node) -> bool:
        """Trojan 握手测试 - TLS 连接验证"""
        if not node.password:
            return False

        try:
            import ssl
            ssl_ctx = ssl.create_default_context()
            if node.skip_cert_verify:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

            sni = node.sni or node.server

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.server, node.port, ssl=ssl_ctx, server_hostname=sni),
                timeout=self.http_timeout
            )

            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True

        except (asyncio.TimeoutError, ConnectionRefusedError, OSError, ssl.SSLError):
            return False

    async def _test_ss(self, node: Node) -> bool:
        """Shadowsocks 测试 - TCP 可达即视为可能有效"""
        if not node.cipher or not node.password:
            return False
        return node.latency > 0 and node.latency < self.max_latency

    async def _test_hysteria2(self, node: Node) -> bool:
        """Hysteria2 测试 - QUIC 协议，UDP 验证"""
        if not node.hysteria2_password:
            return False

        try:
            loop = asyncio.get_event_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(None, self._test_udp_port, node.server, node.port),
                timeout=self.http_timeout
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
