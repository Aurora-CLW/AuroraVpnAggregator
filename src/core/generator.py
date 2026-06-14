"""
订阅生成器
"""

import yaml
import base64
import json
import logging
from typing import List, Optional
from pathlib import Path
from datetime import datetime

from ..models.node import Node

logger = logging.getLogger(__name__)


class Generator:
    """订阅生成器"""

    def __init__(self, config: dict = None):
        """
        初始化生成器

        Args:
            config: 生成配置
        """
        self.config = config or {}
        self.naming_format = self.config.get("naming", "{country} {type} {latency}ms")
        self.sort_by = self.config.get("sort_by", "latency")
        self.max_nodes = self.config.get("max_nodes", 0)

    def generate_all(self, nodes: List[Node], output_dir: str):
        """
        生成所有格式的订阅

        Args:
            nodes: 节点列表
            output_dir: 输出目录
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 排序节点
        sorted_nodes = self._sort_nodes(nodes)

        # 限制节点数
        if self.max_nodes > 0:
            sorted_nodes = sorted_nodes[:self.max_nodes]

        # 生成各格式
        clash_content = self.generate_clash(sorted_nodes)
        (output_path / "clash.yaml").write_text(clash_content, encoding="utf-8")
        logger.info(f"生成 clash.yaml: {len(sorted_nodes)} 个节点")

        v2ray_content = self.generate_v2ray(sorted_nodes)
        (output_path / "v2ray.txt").write_text(v2ray_content, encoding="utf-8")
        logger.info(f"生成 v2ray.txt: {len(sorted_nodes)} 个节点")

        singbox_content = self.generate_singbox(sorted_nodes)
        (output_path / "singbox.json").write_text(singbox_content, encoding="utf-8")
        logger.info(f"生成 singbox.json: {len(sorted_nodes)} 个节点")

        # 生成节点数据
        nodes_data = self._generate_nodes_data(sorted_nodes)
        (output_path / "nodes.json").write_text(nodes_data, encoding="utf-8")

    def generate_clash(self, nodes: List[Node]) -> str:
        """
        生成 Clash YAML

        Args:
            nodes: 节点列表

        Returns:
            YAML 字符串
        """
        # 格式化节点名称
        formatted_nodes = self._format_node_names(nodes)

        # 生成代理列表
        proxies = []
        for node in formatted_nodes:
            proxy = node.to_clash()
            proxies.append(proxy)

        # 生成代理组
        proxy_names = [p["name"] for p in proxies]

        proxy_groups = [
            {
                "name": "Proxy",
                "type": "select",
                "proxies": ["AUTO"] + proxy_names,
            },
            {
                "name": "AUTO",
                "type": "url-test",
                "proxies": proxy_names,
                "url": "http://www.gstatic.com/generate_204",
                "interval": 300,
            },
        ]

        # 完整配置
        config = {
            "mixed-port": 7890,
            "allow-lan": True,
            "mode": "rule",
            "log-level": "info",
            "dns": {
                "enable": True,
                "enhanced-mode": "fake-ip",
                "fake-ip-range": "198.18.0.1/16",
                "nameserver": [
                    "https://dns.alidns.com/dns-query",
                    "https://doh.pub/dns-query",
                ],
            },
            "proxies": proxies,
            "proxy-groups": proxy_groups,
            "rules": [
                "GEOIP,CN,DIRECT",
                "MATCH,Proxy",
            ],
        }

        header = f"""# Aurora VPN Aggregator
# Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
# Nodes: {len(proxies)}

"""
        return header + yaml.dump(config, allow_unicode=True, sort_keys=False, default_flow_style=False)

    def generate_v2ray(self, nodes: List[Node]) -> str:
        """
        生成 V2Ray Base64

        Args:
            nodes: 节点列表

        Returns:
            Base64 字符串
        """
        urls = []

        for node in nodes:
            url = node.to_v2ray_url()
            if url:
                urls.append(url)

        content = "\n".join(urls)
        return base64.b64encode(content.encode("utf-8")).decode("utf-8")

    def generate_singbox(self, nodes: List[Node]) -> str:
        """
        生成 Sing-box JSON

        Args:
            nodes: 节点列表

        Returns:
            JSON 字符串
        """
        outbounds = []

        for node in nodes:
            outbound = node.to_singbox()
            outbounds.append(outbound)

        # 添加 DIRECT 和 DNS outbounds
        outbounds.insert(0, {"type": "direct", "tag": "DIRECT"})
        outbounds.insert(0, {"type": "dns", "tag": "DNS"})

        config = {
            "outbounds": outbounds,
            "route": {
                "rules": [
                    {"protocol": "dns", "outbound": "DNS"},
                    {"geoip": ["cn"], "outbound": "DIRECT"},
                    {"geosite": ["cn"], "outbound": "DIRECT"},
                ],
                "final": "Proxy",
            },
        }

        return json.dumps(config, indent=2, ensure_ascii=False)

    def _sort_nodes(self, nodes: List[Node]) -> List[Node]:
        """排序节点"""
        if self.sort_by == "latency":
            return sorted(nodes, key=lambda n: n.latency or 9999)
        elif self.sort_by == "speed":
            return sorted(nodes, key=lambda n: -(n.speed or 0))
        elif self.sort_by == "country":
            return sorted(nodes, key=lambda n: n.country or "ZZ")
        else:
            return nodes

    # 节点名称中的广告/冗余关键词 (需清理)
    _NAME_NOISE_KEYWORDS = [
        "免费机场", "免费节点", "免费VPN", "机场订阅", "订阅",
        "TG:", "tg:", "telegram", "Telegram", "@",
        "保持最新", "以防失联", "更新于", "广告", "推广",
        "点击", "传送", "入群", "关注", "频道",
    ]

    def _format_node_names(self, nodes: List[Node]) -> List[Node]:
        """格式化节点名称 — 统一格式: 🇺🇸 US | Trojan | 01"""
        from ..utils.geoip import get_country_flag

        # 按 (country, type) 分组计数, 用于生成序号
        counters: dict = {}

        for node in nodes:
            # 清理原始名称中的广告和冗余内容
            clean_name = node.name or ""
            for kw in self._NAME_NOISE_KEYWORDS:
                if kw in clean_name:
                    clean_name = clean_name.split(kw)[0].rstrip("|-— ")
            clean_name = clean_name.strip("|-— :： ")

            # 如果清理后名称仍包含重复的协议名 (如 "TROJAN蔓TROJAN")
            name_upper = clean_name.upper()
            for proto in ["VMESS", "VLESS", "TROJAN", "SS", "SSR"]:
                if name_upper.count(proto) > 1:
                    idx = name_upper.rfind(proto)
                    clean_name = clean_name[:idx] + clean_name[idx + len(proto):]
                    clean_name = clean_name.strip("|-— :： ")

            # 构建统一格式: 🇺🇸 US | Trojan
            parts = []
            if node.country:
                parts.append(f"{get_country_flag(node.country)} {node.country}")
            parts.append(node.type.upper())
            # 清理后的原始名称 (如果有且不与类型重复)
            if clean_name and clean_name.upper() not in ("", node.type.upper(), "UNKNOWN"):
                if node.type.upper() not in clean_name.upper():
                    parts.append(clean_name)

            base_name = " | ".join(parts) if parts else "Unknown"
            # 同名节点加序号
            key = base_name
            counters[key] = counters.get(key, 0) + 1
            if counters[key] > 1:
                node.name = f"{base_name} | {counters[key]:03d}"
            else:
                node.name = base_name

        return nodes

    def _generate_nodes_data(self, nodes: List[Node]) -> str:
        """生成节点数据 JSON"""
        data = {
            "version": "1.0.0",
            "updated_at": datetime.now().isoformat(),
            "total": len(nodes),
            "nodes": [
                {
                    "name": n.name,
                    "type": n.type,
                    "server": n.server,
                    "port": n.port,
                    "country": n.country,
                    "latency": n.latency,
                    "is_valid": n.is_valid,
                    "source": n.source,
                    "uuid": n.uuid,
                    "password": n.password,
                    "cipher": n.cipher,
                    "network": n.network,
                    "security": n.security,
                    "sni": n.sni,
                    "skip_cert_verify": n.skip_cert_verify,
                    "ws_path": n.ws_path,
                    "ws_headers": n.ws_headers,
                    "grpc_service_name": n.grpc_service_name,
                    "reality_public_key": n.reality_public_key,
                    "reality_short_id": n.reality_short_id,
                    "fingerprint": n.fingerprint,
                    "hysteria2_password": n.hysteria2_password,
                    "flow": n.flow,
                    "alterId": n.alterId,
                }
                for n in nodes
            ],
        }
        return json.dumps(data, indent=2, ensure_ascii=False)


def generate_subscription(nodes: List[Node], output_dir: str, config: dict = None):
    """
    生成订阅的便捷函数

    Args:
        nodes: 节点列表
        output_dir: 输出目录
        config: 配置
    """
    generator = Generator(config)
    generator.generate_all(nodes, output_dir)
