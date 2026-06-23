"""
节点解析器
支持多种格式: Clash YAML, V2Ray Base64, Sing-box JSON, 原始 URL
"""

import yaml
import base64
import json
import re
from typing import List, Optional, Dict, Any
from urllib.parse import parse_qs, unquote
import logging

from ..models.node import Node

logger = logging.getLogger(__name__)


class Parser:
    """节点解析器"""

    def parse(self, content: str, format: str = "auto") -> List[Node]:
        """
        自动识别并解析内容

        Args:
            content: 订阅内容
            format: 格式 (auto, clash, base64, singbox, urls)

        Returns:
            节点列表
        """
        content = content.strip()

        if format == "auto":
            format = self._detect_format(content)

        logger.info(f"使用格式: {format}")

        if format == "clash":
            return self._parse_clash(content)
        elif format == "base64":
            return self._parse_base64(content)
        elif format == "singbox":
            return self._parse_singbox(content)
        elif format == "urls":
            return self._parse_urls(content)
        else:
            logger.warning(f"未知格式: {format}，尝试 URL 解析")
            return self._parse_urls(content)

    def _detect_format(self, content: str) -> str:
        """自动检测内容格式"""
        # 检测 Clash YAML
        if content.startswith(("proxies:", "mixed-port:", "port:")):
            return "clash"

        # 检测 JSON
        if content.startswith("{") or content.startswith("["):
            try:
                json.loads(content)
                return "singbox"
            except:
                pass

        # 检测 Base64
        if self._is_base64(content):
            return "base64"

        # 默认为 URL 列表
        return "urls"

    def _is_base64(self, content: str) -> bool:
        """检测是否为 Base64 编码"""
        try:
            cleaned = content.strip().replace("\n", "").replace("\r", "").replace(" ", "")
            decoded = base64.b64decode(cleaned, validate=False).decode("utf-8")
            if any(proto in decoded for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
                return True
        except:
            pass
        return False

    def _parse_clash(self, content: str) -> List[Node]:
        """解析 Clash YAML 格式"""
        nodes = []

        try:
            config = yaml.safe_load(content)
            proxies = config.get("proxies", [])

            for proxy in proxies:
                try:
                    node = self._clash_proxy_to_node(proxy)
                    if node:
                        nodes.append(node)
                except Exception as e:
                    logger.debug(f"解析 Clash 代理失败: {e}")

        except yaml.YAMLError as e:
            logger.error(f"YAML 解析错误: {e}")

        logger.info(f"Clash 解析: {len(nodes)} 个节点")
        return nodes

    def _clash_proxy_to_node(self, proxy: Dict[str, Any]) -> Optional[Node]:
        """Clash proxy 转换为 Node"""
        proxy_type = proxy.get("type", "").lower()

        if proxy_type not in ["ss", "ssr", "vmess", "vless", "trojan", "hysteria2", "tuic"]:
            return None

        node = Node(
            name=proxy.get("name", "Unknown"),
            type=proxy_type,
            server=proxy.get("server", ""),
            port=proxy.get("port", 0),
        )

        # SS
        if proxy_type == "ss":
            node.cipher = proxy.get("cipher")
            node.password = proxy.get("password")

        # SSR
        elif proxy_type == "ssr":
            node.cipher = proxy.get("cipher")
            node.password = proxy.get("password")
            node.ssr_protocol = proxy.get("protocol")
            node.ssr_protocol_param = proxy.get("protocol-param")
            node.ssr_obfs = proxy.get("obfs")
            node.ssr_obfs_param = proxy.get("obfs-param")

        # VMess
        elif proxy_type == "vmess":
            node.uuid = proxy.get("uuid")
            node.alterId = proxy.get("alterId", 0)
            node.cipher = proxy.get("cipher", "auto")
            node.network = proxy.get("network", "tcp")

        # VLess
        elif proxy_type == "vless":
            node.uuid = proxy.get("uuid")
            node.flow = proxy.get("flow")
            node.network = proxy.get("network", "tcp")

        # Trojan
        elif proxy_type == "trojan":
            node.password = proxy.get("password")
            node.sni = proxy.get("sni")
            node.skip_cert_verify = proxy.get("skip-cert-verify", False)

        # Hysteria2
        elif proxy_type == "hysteria2":
            node.hysteria2_password = proxy.get("password")
            node.hysteria2_obfs = proxy.get("obfs")

        # TLS
        if proxy.get("tls"):
            node.security = "tls"
            node.sni = proxy.get("servername") or proxy.get("sni")
            node.skip_cert_verify = proxy.get("skip-cert-verify", False)
            node.alpn = proxy.get("alpn")

        # Reality
        reality_opts = proxy.get("reality-opts", {})
        if reality_opts:
            node.security = "reality"
            node.reality_public_key = reality_opts.get("public-key")
            node.reality_short_id = reality_opts.get("short-id")
            node.fingerprint = proxy.get("client-fingerprint")

        # WebSocket
        ws_opts = proxy.get("ws-opts", {})
        if ws_opts:
            node.network = "ws"
            node.ws_path = ws_opts.get("path")
            node.ws_headers = ws_opts.get("headers")

        # gRPC
        grpc_opts = proxy.get("grpc-opts", {})
        if grpc_opts:
            node.network = "grpc"
            node.grpc_service_name = grpc_opts.get("grpc-service-name")

        return node

    def _parse_base64(self, content: str) -> List[Node]:
        """解析 Base64 编码内容"""
        nodes = []

        try:
            cleaned = content.strip().replace("\n", "").replace("\r", "").replace(" ", "")
            padding = 4 - len(cleaned) % 4
            if padding != 4:
                cleaned += "=" * padding
            decoded = base64.b64decode(cleaned, validate=False).decode("utf-8")
            lines = decoded.strip().split("\n")

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                node = self._parse_node_url(line)
                if node:
                    nodes.append(node)

        except Exception as e:
            logger.error(f"Base64 解析错误: {e}")

        logger.info(f"Base64 解析: {len(nodes)} 个节点")
        return nodes

    def _parse_urls(self, content: str) -> List[Node]:
        """解析 URL 列表"""
        nodes = []

        lines = content.strip().split("\n")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            node = self._parse_node_url(line)
            if node:
                nodes.append(node)

        logger.info(f"URL 解析: {len(nodes)} 个节点")
        return nodes

    def _parse_singbox(self, content: str) -> List[Node]:
        """解析 Sing-box JSON 格式"""
        nodes = []

        try:
            config = json.loads(content)
            outbounds = config.get("outbounds", [])

            for outbound in outbounds:
                try:
                    node = self._singbox_outbound_to_node(outbound)
                    if node:
                        nodes.append(node)
                except Exception as e:
                    logger.debug(f"解析 Sing-box outbound 失败: {e}")

        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析错误: {e}")

        logger.info(f"Sing-box 解析: {len(nodes)} 个节点")
        return nodes

    def _singbox_outbound_to_node(self, outbound: Dict[str, Any]) -> Optional[Node]:
        """Sing-box outbound 转换为 Node"""
        outbound_type = outbound.get("type", "").lower()

        if outbound_type not in ["shadowsocks", "vmess", "vless", "trojan", "hysteria2", "tuic"]:
            return None

        node = Node(
            name=outbound.get("tag", "Unknown"),
            type="ss" if outbound_type == "shadowsocks" else outbound_type,
            server=outbound.get("server", ""),
            port=outbound.get("server_port", 0),
        )

        # Shadowsocks
        if outbound_type == "shadowsocks":
            node.cipher = outbound.get("method")
            node.password = outbound.get("password")

        # VMess/VLess
        elif outbound_type in ["vmess", "vless"]:
            node.uuid = outbound.get("uuid")
            if outbound_type == "vmess":
                node.alterId = outbound.get("alter_id", 0)
                node.cipher = outbound.get("security", "auto")
            if outbound_type == "vless":
                node.flow = outbound.get("flow")

        # Trojan
        elif outbound_type == "trojan":
            node.password = outbound.get("password")

        # Hysteria2
        elif outbound_type == "hysteria2":
            node.hysteria2_password = outbound.get("password")

        # Transport
        transport = outbound.get("transport", {})
        if transport:
            transport_type = transport.get("type")
            node.network = transport_type
            if transport_type == "ws":
                node.ws_path = transport.get("path")
                node.ws_headers = transport.get("headers")
            elif transport_type == "grpc":
                node.grpc_service_name = transport.get("service_name")

        # TLS
        tls = outbound.get("tls", {})
        if tls:
            node.security = "tls"
            node.sni = tls.get("server_name")
            node.alpn = tls.get("alpn")

        return node

    def _parse_node_url(self, url: str) -> Optional[Node]:
        """解析单个节点 URL"""
        try:
            if url.startswith("vmess://"):
                return self._parse_vmess_url(url)
            elif url.startswith("vless://"):
                return self._parse_vless_url(url)
            elif url.startswith("trojan://"):
                return self._parse_trojan_url(url)
            elif url.startswith("ss://"):
                return self._parse_ss_url(url)
            elif url.startswith("ssr://"):
                return self._parse_ssr_url(url)
            elif url.startswith(("hysteria2://", "hysteria://", "hy2://")):
                return self._parse_hysteria2_url(url)
            elif url.startswith("anytls://"):
                return self._parse_anytls_url(url)
            else:
                return None
        except Exception as e:
            logger.debug(f"解析 URL 失败: {e}")
            return None

    def _parse_vmess_url(self, url: str) -> Optional[Node]:
        """解析 vmess:// URL"""
        # vmess://BASE64(json)
        encoded = url.replace("vmess://", "")
        decoded = base64.b64decode(encoded).decode("utf-8")
        config = json.loads(decoded)

        node = Node(
            name=unquote(config.get("ps", "VMess Node")),
            type="vmess",
            server=config.get("add", ""),
            port=int(config.get("port", 443)),
            uuid=config.get("id", ""),
            alterId=int(config.get("aid", 0)),
            cipher=config.get("scy", "auto"),
            network=config.get("net", "tcp"),
        )

        if config.get("tls") == "tls":
            node.security = "tls"
            node.sni = config.get("sni")

        if node.network == "ws":
            node.ws_path = config.get("path")
            if config.get("host"):
                node.ws_headers = {"Host": config.get("host")}

        node.raw_url = url
        return node

    def _parse_vless_url(self, url: str) -> Optional[Node]:
        """解析 vless:// URL"""
        # vless://uuid@server:port?params#name
        import re
        from urllib.parse import urlparse

        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        node = Node(
            name=unquote(parsed.fragment) if parsed.fragment else "VLess Node",
            type="vless",
            server=parsed.hostname or "",
            port=parsed.port or 443,
            uuid=parsed.username or "",
        )

        # 参数
        node.network = params.get("type", ["tcp"])[0]
        node.security = params.get("security", ["none"])[0]
        node.flow = params.get("flow", [None])[0]
        node.sni = params.get("sni", [None])[0]

        if node.network == "ws":
            node.ws_path = params.get("path", [None])[0]
            host = params.get("host", [None])[0]
            if host:
                node.ws_headers = {"Host": host}

        node.raw_url = url
        return node

    def _parse_trojan_url(self, url: str) -> Optional[Node]:
        """解析 trojan:// URL"""
        from urllib.parse import urlparse

        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        node = Node(
            name=unquote(parsed.fragment) if parsed.fragment else "Trojan Node",
            type="trojan",
            server=parsed.hostname or "",
            port=parsed.port or 443,
            password=unquote(parsed.username or ""),
        )

        node.sni = params.get("sni", [parsed.hostname])[0]
        node.skip_cert_verify = params.get("allowInsecure", ["0"])[0] == "1"

        node.raw_url = url
        return node

    def _parse_ss_url(self, url: str) -> Optional[Node]:
        """解析 ss:// URL"""
        import re
        from urllib.parse import urlparse

        # 格式1: ss://BASE64(method:password)@server:port#name
        # 格式2: ss://method:password@server:port#name
        # 格式3: ss://BASE64(method:password)@server:port/?plugin=xxx#name

        url = url.replace("ss://", "")

        # 提取名称
        name = "SS Node"
        if "#" in url:
            url, name = url.rsplit("#", 1)
            name = unquote(name)

        # 尝试解析
        try:
            # 尝试格式1/3
            if "@" in url:
                userinfo, server_part = url.rsplit("@", 1)
                try:
                    # 补齐 base64 padding
                    padded = userinfo + "=" * (-len(userinfo) % 4)
                    decoded = base64.b64decode(padded).decode("utf-8")
                    cipher, password = decoded.split(":", 1)
                except:
                    cipher, password = userinfo.split(":", 1)

                # 解析服务器和端口
                server_match = re.match(r"([^:]+):(\d+)", server_part)
                if server_match:
                    server = server_match.group(1)
                    port = int(server_match.group(2))
                else:
                    return None

            else:
                # 纯 Base64
                padded = url + "=" * (-len(url) % 4)
                decoded = base64.b64decode(padded).decode("utf-8")
                # 格式: method:password@server:port
                parts = decoded.rsplit("@", 1)
                if len(parts) == 2:
                    userinfo, server_info = parts
                    cipher, password = userinfo.split(":", 1)
                    server, port = server_info.rsplit(":", 1)
                    port = int(port)
                else:
                    return None

            node = Node(
                name=name,
                type="ss",
                server=server,
                port=port,
                cipher=cipher,
                password=password,
            )
            node.raw_url = f"ss://{url}#{name}"
            return node

        except Exception as e:
            logger.debug(f"SS URL 解析失败: {e}")
            return None

    def _parse_ssr_url(self, url: str) -> Optional[Node]:
        """解析 ssr:// URL"""
        # ssr://BASE64(server:port:protocol:method:obfs:password_base64/?params)
        encoded = url.replace("ssr://", "")
        decoded = base64.urlsafe_b64decode(encoded + "==").decode("utf-8")

        # 分离参数
        if "/?" in decoded:
            main_part, params_part = decoded.split("/?", 1)
        else:
            main_part = decoded
            params_part = ""

        # 解析主体
        parts = main_part.split(":")
        if len(parts) < 6:
            return None

        server = parts[0]
        port = int(parts[1])
        protocol = parts[2]
        method = parts[3]
        obfs = parts[4]
        password_base64 = parts[5]

        password = base64.urlsafe_b64decode(password_base64 + "==").decode("utf-8")

        # 解析参数获取名称
        name = "SSR Node"
        if params_part:
            params = parse_qs(params_part)
            if "remarks" in params:
                name = base64.urlsafe_b64decode(params["remarks"][0] + "==").decode("utf-8")

        node = Node(
            name=name,
            type="ssr",
            server=server,
            port=port,
            cipher=method,
            password=password,
            ssr_protocol=protocol,
            ssr_obfs=obfs,
        )
        node.raw_url = url
        return node

    def _parse_hysteria2_url(self, url: str) -> Optional[Node]:
        """解析 hysteria2:// / hy2:// URL

        格式: hysteria2://password@server:port?params#name
        """
        from urllib.parse import urlparse

        # hy2:// -> hysteria2:// 统一处理
        if url.startswith("hy2://"):
            url = "hysteria2://" + url[6:]

        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        password = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443

        name = unquote(parsed.fragment) if parsed.fragment else "Hysteria2 Node"

        node = Node(
            name=name,
            type="hysteria2",
            server=server,
            port=port,
            hysteria2_password=password,
        )

        # peer / sni
        peer = params.get("peer", [None])[0]
        if peer:
            node.sni = peer

        # insecure
        insecure = params.get("insecure", ["0"])[0]
        if insecure in ("1", "true"):
            node.skip_cert_verify = True

        # obfs / obfs-password
        obfs = params.get("obfs", [None])[0]
        if obfs:
            node.hysteria2_obfs = obfs

        # alpn
        alpn = params.get("alpn", [None])[0]
        if alpn:
            node.alpn = alpn.split(",")

        node.raw_url = url
        return node

    def _parse_anytls_url(self, url: str) -> Optional[Node]:
        """解析 anytls:// URL

        格式: anytls://password@server:port?security=tls&sni=xxx&fp=chrome#name
        """
        from urllib.parse import urlparse

        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        password = unquote(parsed.username or "")
        server = parsed.hostname or ""
        port = parsed.port or 443

        name = unquote(parsed.fragment) if parsed.fragment else "AnyTLS Node"

        node = Node(
            name=name,
            type="anytls",
            server=server,
            port=port,
            password=password,
        )

        # TLS 参数
        node.security = params.get("security", ["tls"])[0]
        node.sni = params.get("sni", [None])[0]
        fp = params.get("fp", [None])[0]
        if fp:
            node.fingerprint = fp

        insecure = params.get("allowInsecure", ["0"])[0]
        if insecure in ("1", "true"):
            node.skip_cert_verify = True

        alpn = params.get("alpn", [None])[0]
        if alpn:
            node.alpn = alpn.split(",")

        node.raw_url = url
        return node
