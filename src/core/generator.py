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
        self.sort_by = self.config.get("sort_by", "latency")
        self.max_nodes = self.config.get("max_nodes", 0)
        # 输出格式白名单（默认全部生成）；nodes.json 始终生成
        self.formats = self.config.get("formats", ["clash", "v2ray", "singbox"])

    def generate_all(self, nodes: List[Node], output_dir: str, partition_size: int = 0):
        """
        生成所有格式的订阅，支持分片

        Args:
            nodes: 节点列表
            output_dir: 输出目录
            partition_size: 分片大小（每个分片最大节点数，0=不分片）
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 排序节点
        sorted_nodes = self._sort_nodes(nodes)

        # 统一格式化节点名称 (一次完成, 所有格式共享, 避免跨函数副作用导致的不一致)
        sorted_nodes = self._format_node_names(sorted_nodes)

        # 限制节点数
        if self.max_nodes > 0:
            sorted_nodes = sorted_nodes[:self.max_nodes]

        # 生成全量订阅
        self._generate_single(sorted_nodes, output_path)

        # 生成分片订阅
        if partition_size > 0 and len(sorted_nodes) > partition_size:
            self._generate_partitions(sorted_nodes, output_path, partition_size)

    def _generate_single(self, nodes: List[Node], output_path: Path):
        """生成单个订阅文件集（全量）"""
        if "clash" in self.formats:
            clash_content = self.generate_clash(nodes)
            (output_path / "clash.yaml").write_text(clash_content, encoding="utf-8")
            logger.info(f"生成 clash.yaml: {len(nodes)} 个节点")

        if "v2ray" in self.formats:
            v2ray_content = self.generate_v2ray(nodes)
            (output_path / "v2ray.txt").write_text(v2ray_content, encoding="utf-8")
            logger.info(f"生成 v2ray.txt: {len(nodes)} 个节点")

        if "singbox" in self.formats:
            singbox_content = self.generate_singbox(nodes)
            (output_path / "singbox.json").write_text(singbox_content, encoding="utf-8")
            logger.info(f"生成 singbox.json: {len(nodes)} 个节点")

        # 生成节点数据
        nodes_data = self._generate_nodes_data(nodes)
        (output_path / "nodes.json").write_text(nodes_data, encoding="utf-8")

    def _generate_partitions(self, nodes: List[Node], output_path: Path, partition_size: int):
        """生成分片订阅"""
        total = len(nodes)
        num_partitions = (total + partition_size - 1) // partition_size

        for i in range(num_partitions):
            start = i * partition_size
            end = min(start + partition_size, total)
            partition_nodes = nodes[start:end]
            part_dir = output_path / f"part{i + 1}"
            part_dir.mkdir(parents=True, exist_ok=True)

            self._generate_single(partition_nodes, part_dir)
            logger.info(f"分片 {i + 1}/{num_partitions}: {len(partition_nodes)} 个节点 → {part_dir}")

    def generate_clash(self, nodes: List[Node]) -> str:
        """
        生成 Clash YAML

        Args:
            nodes: 节点列表

        Returns:
            YAML 字符串
        """
        # 生成代理列表 (节点名称已在 generate_all 中统一格式化)
        proxies = []
        for node in nodes:
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

        # 添加 Proxy 选择器组（url-test），供 route.final 引用；
        # 之前 final 写死 "Proxy" 但 outbounds 中并无该 tag，导致配置无法加载。
        proxy_tags = [o["tag"] for o in outbounds if o.get("tag") not in ("DNS", "DIRECT")]
        outbounds.insert(0, {
            "type": "url-test",
            "tag": "Proxy",
            "outbounds": proxy_tags,
            "url": "http://www.gstatic.com/generate_204",
            "interval": 300,
        })

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

    def _format_node_names(self, nodes: List[Node]) -> List[Node]:
        """格式化节点名称 — 统一格式: 🇺🇸 US | VLESS | 120ms | 5.2MB/s | @频道 | 001
        频道标注来源 (source 如 tg:FreeV2rays), 截断控制名称长度。
        """
        from ..utils.geoip import get_country_flag

        # 按 (country, type, channel) 分组计数, 用于生成序号
        counters: dict = {}

        for node in nodes:
            # 构建统一格式: 🇺🇸 US | VLESS | 120ms | 5.2MB/s
            parts = []
            if node.country:
                parts.append(f"{get_country_flag(node.country)} {node.country}")
            parts.append(node.type.upper())
            # 延迟
            if node.latency and node.latency > 0:
                parts.append(f"{node.latency}ms")
            # 速度
            if node.speed and node.speed > 0:
                parts.append(f"{node.speed}MB/s")
            # 来源频道标注 (截断, 控制名称长度)
            if node.source:
                src = node.source
                if ":" in src:
                    src = src.split(":", 1)[1]
                src = src.strip()
                if src:
                    parts.append(f"@{src[:10]}")

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
            "nodes": [n.to_dict() for n in nodes],
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
    partition_size = (config or {}).get("partition_size", 0)
    generator.generate_all(nodes, output_dir, partition_size=partition_size)
