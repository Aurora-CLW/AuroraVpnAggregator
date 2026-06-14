"""
数据模型
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict
import hashlib


@dataclass
class Node:
    """VPN 节点数据模型"""

    # 基础信息
    name: str
    type: str  # ss, vmess, vless, trojan, hysteria2, tuic, etc.
    server: str
    port: int

    # 协议参数 - SS/SSR
    cipher: Optional[str] = None
    password: Optional[str] = None

    # 协议参数 - VMess/VLess
    uuid: Optional[str] = None
    alterId: int = 0
    flow: Optional[str] = None  # VLess flow

    # 网络传输
    network: Optional[str] = None  # tcp, ws, grpc, http, quic
    security: Optional[str] = None  # tls, reality, none

    # TLS 参数
    sni: Optional[str] = None
    skip_cert_verify: bool = False
    alpn: Optional[List[str]] = None
    fingerprint: Optional[str] = None  # uTLS fingerprint

    # WebSocket 参数
    ws_path: Optional[str] = None
    ws_headers: Optional[Dict[str, str]] = None

    # gRPC 参数
    grpc_service_name: Optional[str] = None

    # HTTP 参数
    http_path: Optional[str] = None
    http_host: Optional[str] = None

    # Reality 参数
    reality_public_key: Optional[str] = None
    reality_short_id: Optional[str] = None

    # Hysteria2 参数
    hysteria2_password: Optional[str] = None
    hysteria2_up_mbps: Optional[int] = None
    hysteria2_down_mbps: Optional[int] = None
    hysteria2_obfs: Optional[str] = None

    # TUIC 参数
    tuic_congestion_control: Optional[str] = None  # bbr, cubic
    tuic_alpn: Optional[List[str]] = None
    tuic_udp_relay_mode: Optional[str] = None  # native, quic

    # Shadowsocks 2022
    ss2022_psk: Optional[str] = None

    # SSR 特有参数
    ssr_protocol: Optional[str] = None
    ssr_protocol_param: Optional[str] = None
    ssr_obfs: Optional[str] = None
    ssr_obfs_param: Optional[str] = None

    # 元信息
    country: Optional[str] = None  # 自动识别的国家代码
    country_name: Optional[str] = None  # 国家名称
    city: Optional[str] = None  # 城市
    region: Optional[str] = None  # 地区
    isp: Optional[str] = None  # ISP 运营商
    source: Optional[str] = None  # 来源标记
    added_at: Optional[datetime] = None
    tested_at: Optional[datetime] = None

    # 测试结果
    tcp_valid: bool = False  # TCP 可达
    latency: int = 0  # 延迟 ms，0 表示未测试
    speed: float = 0.0  # 速度 MB/s，0 表示未测试
    is_valid: bool = False  # 最终有效性

    # 指纹（用于去重）
    node_fingerprint: str = ""

    # 原始数据
    raw_url: Optional[str] = None  # 原始节点 URL

    def __post_init__(self):
        """初始化后处理"""
        if self.added_at is None:
            self.added_at = datetime.now()
        if not self.node_fingerprint:
            self.node_fingerprint = self.generate_fingerprint()

    def generate_fingerprint(self) -> str:
        """生成节点指纹，用于去重"""
        # 核心特征：类型 + 服务器 + 端口 + UUID/密码
        key_parts = [
            self.type,
            self.server,
            str(self.port)
        ]

        # 根据类型添加唯一标识
        if self.uuid:
            key_parts.append(self.uuid)
        elif self.password:
            key_parts.append(self.password)

        # 网络传输特征
        if self.network:
            key_parts.append(self.network)
        if self.ws_path:
            key_parts.append(self.ws_path)

        key = ":".join(key_parts)
        return hashlib.md5(key.encode()).hexdigest()

    def to_clash(self) -> dict:
        """转换为 Clash 代理格式"""
        proxy = {
            "name": self.name,
            "type": self.type,
            "server": self.server,
            "port": self.port,
        }

        # SS
        if self.type == "ss":
            proxy["cipher"] = self.cipher
            proxy["password"] = self.password
            if self.ss2022_psk:
                proxy["psk"] = self.ss2022_psk

        # SSR
        elif self.type == "ssr":
            proxy["cipher"] = self.cipher
            proxy["password"] = self.password
            proxy["protocol"] = self.ssr_protocol
            proxy["protocol-param"] = self.ssr_protocol_param
            proxy["obfs"] = self.ssr_obfs
            proxy["obfs-param"] = self.ssr_obfs_param

        # VMess
        elif self.type == "vmess":
            proxy["uuid"] = self.uuid
            proxy["alterId"] = self.alterId
            proxy["cipher"] = self.cipher or "auto"
            if self.network:
                proxy["network"] = self.network

        # VLess
        elif self.type == "vless":
            proxy["uuid"] = self.uuid
            if self.flow:
                proxy["flow"] = self.flow
            if self.network:
                proxy["network"] = self.network

        # Trojan
        elif self.type == "trojan":
            proxy["password"] = self.password
            proxy["sni"] = self.sni
            proxy["skip-cert-verify"] = self.skip_cert_verify

        # Hysteria2
        elif self.type == "hysteria2":
            proxy["password"] = self.hysteria2_password
            if self.hysteria2_obfs:
                proxy["obfs"] = self.hysteria2_obfs
            if self.hysteria2_up_mbps:
                proxy["up"] = f"{self.hysteria2_up_mbps} Mbps"
            if self.hysteria2_down_mbps:
                proxy["down"] = f"{self.hysteria2_down_mbps} Mbps"

        # TUIC
        elif self.type == "tuic":
            proxy["uuid"] = self.uuid
            proxy["password"] = self.password
            proxy["congestion-controller"] = self.tuic_congestion_control or "bbr"
            if self.tuic_alpn:
                proxy["alpn"] = self.tuic_alpn
            if self.tuic_udp_relay_mode:
                proxy["udp-relay-mode"] = self.tuic_udp_relay_mode

        # TLS 配置
        if self.security == "tls" or self.type in ["trojan", "vless", "vmess"]:
            if self.type not in ["ss", "ssr", "tuic"]:  # 这些类型不需要额外 TLS
                proxy["tls"] = True
                if self.sni:
                    proxy["servername"] = self.sni
                if self.skip_cert_verify:
                    proxy["skip-cert-verify"] = True

        # Reality 配置
        if self.security == "reality":
            proxy["tls"] = True
            proxy["reality-opts"] = {}
            if self.reality_public_key:
                proxy["reality-opts"]["public-key"] = self.reality_public_key
            if self.reality_short_id:
                proxy["reality-opts"]["short-id"] = self.reality_short_id
            if self.fingerprint:
                proxy["client-fingerprint"] = self.fingerprint

        # WebSocket 配置
        if self.network == "ws":
            proxy["ws-opts"] = {}
            if self.ws_path:
                proxy["ws-opts"]["path"] = self.ws_path
            if self.ws_headers:
                proxy["ws-opts"]["headers"] = self.ws_headers

        # gRPC 配置
        if self.network == "grpc":
            proxy["grpc-opts"] = {}
            if self.grpc_service_name:
                proxy["grpc-opts"]["grpc-service-name"] = self.grpc_service_name

        # HTTP 配置
        if self.network == "http":
            proxy["http-opts"] = {}
            if self.http_path:
                proxy["http-opts"]["path"] = [self.http_path]
            if self.http_host:
                proxy["http-opts"]["headers"] = {"Host": self.http_host}

        # UDP
        proxy["udp"] = True

        return proxy

    def to_v2ray_url(self) -> str:
        """转换为 V2Ray URL 格式"""
        import base64
        from urllib.parse import urlencode, quote

        if self.type == "ss":
            # ss://BASE64(method:password)@server:port#name
            userinfo = base64.b64encode(
                f"{self.cipher}:{self.password}".encode()
            ).decode()
            return f"ss://{userinfo}@{self.server}:{self.port}#{quote(self.name)}"

        elif self.type == "vmess":
            # vmess://BASE64(json)
            vmess_obj = {
                "add": self.server,
                "port": str(self.port),
                "id": self.uuid,
                "aid": str(self.alterId),
                "scy": self.cipher or "auto",
                "net": self.network or "tcp",
                "type": "none",
                "host": "",
                "path": "",
                "tls": "tls" if self.security == "tls" else "",
                "sni": self.sni or "",
                "ps": self.name
            }
            if self.ws_path:
                vmess_obj["path"] = self.ws_path
            if self.ws_headers and "Host" in self.ws_headers:
                vmess_obj["host"] = self.ws_headers["Host"]

            encoded = base64.b64encode(
                str(vmess_obj).replace("'", '"').encode()
            ).decode()
            return f"vmess://{encoded}"

        elif self.type == "vless":
            # vless://uuid@server:port?params#name
            params = []
            if self.network:
                params.append(f"type={self.network}")
            if self.security:
                params.append(f"security={self.security}")
            if self.sni:
                params.append(f"sni={self.sni}")
            if self.flow:
                params.append(f"flow={self.flow}")
            if self.ws_path:
                params.append(f"path={quote(self.ws_path)}")

            param_str = "&".join(params)
            return f"vless://{self.uuid}@{self.server}:{self.port}?{param_str}#{quote(self.name)}"

        elif self.type == "trojan":
            # trojan://password@server:port?params#name
            params = []
            if self.sni:
                params.append(f"sni={self.sni}")
            if self.skip_cert_verify:
                params.append("allowInsecure=1")

            param_str = "&".join(params) if params else ""
            url = f"trojan://{self.password}@{self.server}:{self.port}"
            if param_str:
                url += f"?{param_str}"
            url += f"#{quote(self.name)}"
            return url

        return ""

    def to_singbox(self) -> dict:
        """转换为 Sing-box outbound 格式"""
        outbound = {
            "type": self.type,
            "tag": self.name,
            "server": self.server,
            "server_port": self.port,
        }

        if self.type == "shadowsocks":
            outbound["method"] = self.cipher
            outbound["password"] = self.password

        elif self.type in ["vmess", "vless"]:
            outbound["uuid"] = self.uuid
            if self.type == "vmess":
                outbound["alter_id"] = self.alterId
                outbound["security"] = self.cipher or "auto"
            if self.type == "vless" and self.flow:
                outbound["flow"] = self.flow

            if self.network:
                transport = {}
                if self.network == "ws":
                    transport["type"] = "ws"
                    if self.ws_path:
                        transport["path"] = self.ws_path
                    if self.ws_headers:
                        transport["headers"] = self.ws_headers
                elif self.network == "grpc":
                    transport["type"] = "grpc"
                    if self.grpc_service_name:
                        transport["service_name"] = self.grpc_service_name
                outbound["transport"] = transport

        elif self.type == "trojan":
            outbound["password"] = self.password

        elif self.type == "hysteria2":
            outbound["password"] = self.hysteria2_password
            if self.hysteria2_obfs:
                outbound["obfs"] = {"type": "salamander", "password": self.hysteria2_obfs}

        # TLS
        if self.security == "tls":
            tls = {}
            if self.sni:
                tls["server_name"] = self.sni
            if self.alpn:
                tls["alpn"] = self.alpn
            if self.skip_cert_verify:
                tls["insecure"] = True
            outbound["tls"] = tls

        # Reality
        if self.security == "reality":
            reality = {}
            if self.reality_public_key:
                reality["public_key"] = self.reality_public_key
            if self.reality_short_id:
                reality["short_id"] = self.reality_short_id
            outbound["tls"] = {
                "enabled": True,
                "server_name": self.sni,
                "reality": reality
            }

        return outbound

    def __repr__(self):
        return f"<Node {self.name} ({self.type}) {self.server}:{self.port}>"


@dataclass
class Source:
    """订阅源数据模型"""

    name: str
    type: str  # github, local, telegram, custom
    enabled: bool = True
    priority: int = 5
    url: Optional[str] = None
    path: Optional[str] = None
    format: str = "auto"  # clash, base64, surge, singbox, auto
    last_update: Optional[datetime] = None
    node_count: int = 0
    error: Optional[str] = None

    def __repr__(self):
        return f"<Source {self.name} ({self.type})>"


@dataclass
class Stats:
    """统计数据模型"""

    total_nodes: int = 0
    valid_nodes: int = 0
    invalid_nodes: int = 0

    # 按类型统计
    by_type: Dict[str, int] = field(default_factory=dict)

    # 按国家统计
    by_country: Dict[str, int] = field(default_factory=dict)

    # 按来源统计
    by_source: Dict[str, int] = field(default_factory=dict)

    # 平均延迟
    avg_latency: int = 0

    # 更新时间
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "total_nodes": self.total_nodes,
            "valid_nodes": self.valid_nodes,
            "invalid_nodes": self.invalid_nodes,
            "by_type": self.by_type,
            "by_country": self.by_country,
            "by_source": self.by_source,
            "avg_latency": self.avg_latency,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
