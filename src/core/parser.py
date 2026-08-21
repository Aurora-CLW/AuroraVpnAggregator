"""
节点解析器
支持多种格式: Clash YAML, V2Ray Base64, Sing-box JSON, 原始 URL
"""

import yaml
import base64
import json
import re
from typing import List, Optional, Dict, Any
from urllib.parse import parse_qs, unquote, urlparse, quote
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
            nodes = self._parse_clash(content)
        elif format == "base64":
            nodes = self._parse_base64(content)
        elif format == "singbox":
            nodes = self._parse_singbox(content)
        elif format == "urls":
            nodes = self._parse_urls(content)
        else:
            logger.warning(f"未知格式: {format}，尝试 URL 解析")
            nodes = self._parse_urls(content)

        # 基础合法性校验（端口范围等），过滤明显非法的节点
        valid = [n for n in nodes if self._is_valid_node(n)]
        if len(valid) != len(nodes):
            logger.info(f"节点基础校验: {len(nodes)} -> {len(valid)} (过滤 {len(nodes) - len(valid)} 个非法节点)")
        return valid

    def _detect_format(self, content: str) -> str:
        """自动检测内容格式"""
        # 去掉开头的注释行与空行，避免带注释的 Clash/JSON 被误判为 urls
        stripped = content.lstrip()
        while stripped.startswith(("#", "%")):
            nl = stripped.find("\n")
            if nl == -1:
                stripped = ""
                break
            stripped = stripped[nl + 1:].lstrip()

        # 检测 Clash YAML
        if stripped.startswith(("proxies:", "mixed-port:", "port:")):
            return "clash"

        # 检测 JSON
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                json.loads(stripped)
                return "singbox"
            except json.JSONDecodeError:
                pass

        # 检测 Base64
        if self._is_base64(stripped):
            return "base64"

        # 默认为 URL 列表
        return "urls"

    def _is_valid_node(self, node: Node) -> bool:
        """节点基础合法性校验（端口范围、服务器非空等）"""
        if not node.server:
            return False
        if not isinstance(node.port, int) or not (1 <= node.port <= 65535):
            logger.debug(f"非法端口: {node.port!r} ({node.name})")
            return False
        return True

    def _is_base64(self, content: str) -> bool:
        """检测是否为 Base64 编码"""
        try:
            cleaned = content.strip().replace("\n", "").replace("\r", "").replace(" ", "")
            decoded = base64.b64decode(cleaned, validate=False).decode("utf-8")
            if any(proto in decoded for proto in ["vmess://", "vless://", "trojan://", "ss://", "ssr://", "socks://", "socks5://", "anytls://", "tuic://", "hysteria2://", "hy2://"]):
                return True
        except Exception:
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
            node.fingerprint = proxy.get("client-fingerprint")

        # Trojan
        elif proxy_type == "trojan":
            node.password = proxy.get("password")
            node.sni = proxy.get("sni")
            node.skip_cert_verify = proxy.get("skip-cert-verify", False)
            node.network = proxy.get("network")
            node.fingerprint = proxy.get("client-fingerprint")

        # Hysteria2
        elif proxy_type == "hysteria2":
            node.hysteria2_password = proxy.get("password")
            node.hysteria2_obfs = proxy.get("obfs")

        # TUIC
        elif proxy_type == "tuic":
            node.uuid = proxy.get("uuid")
            node.password = proxy.get("password")
            node.tuic_congestion_control = proxy.get("congestion-controller") or "bbr"
            node.tuic_udp_relay_mode = proxy.get("udp-relay-mode")
            node.tuic_alpn = proxy.get("alpn")

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
            # 检查是否像 base64 (只包含 base64 字符集)
            if not re.match(r'^[A-Za-z0-9+/=]+$', cleaned):
                logger.debug("内容不是有效 Base64, 跳过")
                return []
            padding = 4 - len(cleaned) % 4
            if padding != 4:
                cleaned += "=" * padding
            decoded = base64.b64decode(cleaned, validate=True).decode("utf-8")
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

        # TUIC
        elif outbound_type == "tuic":
            node.uuid = outbound.get("uuid")
            node.password = outbound.get("password")
            node.tuic_congestion_control = outbound.get("congestion_control") or "bbr"
            node.tuic_udp_relay_mode = outbound.get("udp_relay_mode")
            node.tuic_alpn = outbound.get("tls", {}).get("alpn") if outbound.get("tls") else None

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
            # Reality
            reality = tls.get("reality", {})
            if reality:
                node.security = "reality"
                node.reality_public_key = reality.get("public_key")
                node.reality_short_id = reality.get("short_id")
            if tls.get("utls"):
                node.fingerprint = tls.get("utls", {}).get("fingerprint")

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
            elif url.startswith("tuic://"):
                return self._parse_tuic_url(url)
            elif url.startswith(("socks://", "socks5://")):
                return self._parse_socks_url(url)
            else:
                return None
        except Exception as e:
            logger.debug(f"解析 URL 失败: {e}")
            return None

    def _parse_tuic_url(self, url: str) -> Optional[Node]:
        """解析 tuic:// URL

        格式: tuic:// UUID : PASSWORD @ HOST : PORT ?sni=...&congestion_control=bbr&alpn=h3#NAME
        (UUID 与 PASSWORD 用冒号分隔)
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        uuid = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        server = parsed.hostname or ""
        port = parsed.port or 443

        name = unquote(parsed.fragment) if parsed.fragment else "Tuic Node"

        node = Node(
            name=name,
            type="tuic",
            server=server,
            port=port,
            uuid=uuid,
            password=password,
        )

        # 参数
        node.tuic_congestion_control = params.get("congestion_control", ["bbr"])[0]
        node.tuic_udp_relay_mode = params.get("udp_relay_mode", [None])[0]
        sni = params.get("sni", [None])[0]
        if sni:
            node.sni = sni
        alpn = params.get("alpn", [None])[0]
        if alpn:
            node.tuic_alpn = [a for a in alpn.split(",") if a]
            node.alpn = node.tuic_alpn
        allow_insecure = params.get("allow_insecure", ["0"])[0]
        if allow_insecure in ("1", "true"):
            node.skip_cert_verify = True

        node.raw_url = url
        return node

    def _parse_socks_url(self, url: str) -> Optional[Node]:
        """解析 socks:// / socks5:// URL

        格式: socks://[USERINFO@]HOST:PORT#NAME   (USERINFO 可为 user 或 user:pass)
        userinfo 可能是 Base64 编码 (与 ss:// 相同)。
        注意: 明文 userinfo 与 Base64 易混淆, 仅当严格匹配 Base64 字符集
        且解码结果含 ":" (user:pass) 时才按 Base64 解码, 否则按明文处理,
        避免 "dXNl" 这类纯字母明文用户名被误解码为 "use"。
        """
        raw = url
        # 去掉协议前缀
        url = re.sub(r'^socks5?://', '', url)

        # 提取名称
        name = "SOCKS Node"
        if "#" in url:
            url, name = url.rsplit("#", 1)
            name = unquote(name)

        server = ""
        port = 1080
        username = None
        password = None

        # 服务器部分: 支持 [IPv6]:port 和 host:port
        server_re = re.compile(r'^(?:\[([0-9a-fA-F:.]+)\]|([^:]+)):(\d+)$')

        if "@" in url:
            userinfo, server_part = url.rsplit("@", 1)
            # 先按明文处理 (含 : 就是明文 user:pass)
            if ":" in userinfo:
                username, password = userinfo.split(":", 1)
            else:
                # 无冒号: 可能是 Base64(method:pass) 或纯明文用户名
                decoded = None
                if re.fullmatch(r'[A-Za-z0-9+/]+={0,2}', userinfo):
                    try:
                        padded = userinfo + "=" * (-len(userinfo) % 4)
                        candidate = base64.b64decode(padded).decode("utf-8")
                        if ":" in candidate:
                            decoded = candidate
                    except Exception:
                        pass
                if decoded:
                    username, password = decoded.split(":", 1)
                else:
                    username = userinfo or None

            m = server_re.match(server_part)
            if m:
                server = m.group(1) or m.group(2)
                port = int(m.group(3))
        else:
            m = server_re.match(url)
            if m:
                server = m.group(1) or m.group(2)
                port = int(m.group(3))

        if not server:
            return None

        node = Node(
            name=name,
            type="socks",
            server=server,
            port=port,
            password=password,
        )
        if username:
            node.username = unquote(username)
        if password:
            node.password = unquote(password)
        node.raw_url = raw
        return node

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
        node.fingerprint = params.get("fp", [None])[0]
        node.reality_public_key = params.get("pbk", [None])[0]
        node.reality_short_id = params.get("sid", [None])[0]

        if node.network == "ws":
            node.ws_path = params.get("path", [None])[0]
            host = params.get("host", [None])[0]
            if host:
                node.ws_headers = {"Host": host}

        if node.network == "grpc":
            node.grpc_service_name = params.get("serviceName", [None])[0]

        node.raw_url = url
        return node

    def _parse_trojan_url(self, url: str) -> Optional[Node]:
        """解析 trojan:// URL"""
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
        node.network = params.get("type", [None])[0]
        node.security = params.get("security", ["tls"])[0]
        node.fingerprint = params.get("fp", [None])[0]

        if node.network == "ws":
            node.ws_path = params.get("path", [None])[0]
            host = params.get("host", [None])[0]
            if host:
                node.ws_headers = {"Host": host}

        if node.network == "grpc":
            node.grpc_service_name = params.get("serviceName", [None])[0]

        node.raw_url = url
        return node

    def _parse_ss_url(self, url: str) -> Optional[Node]:
        """解析 ss:// URL"""

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
                except Exception:
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

        # 部分实现使用标准 Base64 (含 + /) 而非 urlsafe;
        # 先按正确补齐 padding 的方式尝试 urlsafe, 失败再回退标准 Base64
        def _b64_decode(s: str) -> str:
            s = s + "=" * (-len(s) % 4)
            try:
                return base64.urlsafe_b64decode(s).decode("utf-8")
            except Exception:
                return base64.b64decode(s).decode("utf-8")

        password = _b64_decode(password_base64)

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
