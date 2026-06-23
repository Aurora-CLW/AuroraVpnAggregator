#!/usr/bin/env python3
"""
Aurora VPN Aggregator 主入口
"""

import sys
import asyncio
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List
import yaml
import os

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.models.node import Node
from src.core.fetcher import Fetcher
from src.core.parser import Parser
from src.core.tester import NodeTester
from src.core.generator import Generator
from src.core.deduplicator import Deduplicator
from src.handlers import get_handler
from src.utils.logger import setup_logger
from src.utils.geoip import GeoIPLookup

logger = logging.getLogger(__name__)


class AuroraAggregator:
    """Aurora VPN 订阅聚合器"""

    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = self._load_config(config_path)
        self.setup_logging()

        self.fetcher = Fetcher(
            timeout=self.config.get("update", {}).get("timeout", 30),
            retry=self.config.get("update", {}).get("retry", 3)
        )
        self.parser = Parser()
        self.deduplicator = Deduplicator(
            method=self.config.get("dedup", {}).get("method", "fingerprint"),
            max_per_server=self.config.get("dedup", {}).get("max_per_server", 10)
        )
        self.generator = Generator(self.config.get("output", {}))
        self.geoip = None
        self.channel_results = {}  # {channel_name: {nodes, sub_urls, status, updated_at}}

    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        config_file = Path(path)
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
            logger.info(f"配置加载成功: {path}")
            return config
        else:
            logger.warning(f"配置文件不存在: {path}，使用默认配置")
            return {}

    def setup_logging(self):
        """配置日志"""
        log_config = self.config.get("logging", {})
        setup_logger(
            name="aurora",
            level=log_config.get("level", "INFO"),
            log_file=log_config.get("file"),
        )

    async def load_sources(self) -> List[Node]:
        """加载所有订阅源"""
        all_nodes = []
        sources_dir = Path("config/sources")

        if not sources_dir.exists():
            logger.warning("订阅源配置目录不存在")
            return all_nodes

        for source_file in sources_dir.glob("*.yaml"):
            logger.info(f"加载订阅源配置: {source_file.name}")

            with open(source_file, "r", encoding="utf-8") as f:
                source_config = yaml.safe_load(f) or {}

            source_type = source_config.get("type")
            if not source_type:
                continue

            if not source_config.get("enabled", True):
                logger.info(f"订阅源已禁用: {source_file.name}")
                continue

            # Telegram 等复合类型: 整个配置作为一个处理器
            if source_type in ("telegram",):
                handler = get_handler(source_type, source_config)
                try:
                    nodes = await handler.fetch()
                    all_nodes.extend(nodes)
                    # 保存频道抓取结果
                    if hasattr(handler, 'channel_results'):
                        self.channel_results.update(handler.channel_results)
                except Exception as e:
                    logger.error(f"处理订阅源失败 [{source_file.name}]: {e}")
                continue

            # GitHub 等类型: sources 子列表中每个源一个处理器
            sources = source_config.get("sources", [])
            for src in sources:
                if not src.get("enabled", True):
                    continue

                handler = get_handler(source_type, src)
                try:
                    nodes = await handler.fetch()
                    all_nodes.extend(nodes)
                except Exception as e:
                    logger.error(f"处理订阅源失败 [{src.get('name')}]: {e}")

        logger.info(f"共加载 {len(all_nodes)} 个节点")
        return all_nodes

    def enrich_nodes(self, nodes: List[Node]) -> List[Node]:
        """丰富节点信息（地理位置等）"""
        if not self.config.get("geoip", {}).get("enabled", True):
            return nodes

        try:
            self.geoip = GeoIPLookup()
            logger.info("开始识别节点地理位置...")

            for node in nodes:
                if node.server:
                    geo = self.geoip.lookup(node.server)
                    node.country = geo.get("country")
                    node.country_name = geo.get("country_name")
                    node.city = geo.get("city")

            logger.info("地理位置识别完成")

        except Exception as e:
            logger.warning(f"地理位置识别失败: {e}")

        return nodes

    async def run(self, skip_test: bool = False, generate_only: bool = False):
        """
        运行聚合器

        Args:
            skip_test: 跳过节点测试
            generate_only: 仅生成订阅
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("Aurora VPN Aggregator 启动")
        logger.info("=" * 60)

        # Step 1: 加载订阅源
        if generate_only:
            logger.info("跳过订阅加载，直接生成订阅文件")
            # 从现有节点数据加载
            nodes = self._load_existing_nodes()
            # 恢复之前保存的频道抓取结果
            self._restore_channel_results()
        else:
            # 加载上次的节点 (用于合并保留)
            old_nodes = self._load_existing_nodes()
            old_map = {}
            for n in old_nodes:
                key = (n.server, n.port)
                old_map[key] = n

            nodes = await self.load_sources()

            # 合并: 本次抓到的节点 + 上次有效但本次没抓到的节点
            if old_nodes:
                nodes = self._merge_with_old(nodes, old_map)

        if not nodes:
            logger.warning("没有获取到任何节点")
            return

        # Step 2: 去重
        logger.info(f"去重前: {len(nodes)} 个节点")
        nodes = self.deduplicator.deduplicate(nodes)
        logger.info(f"去重后: {len(nodes)} 个节点")

        # Step 3: 丰富节点信息
        nodes = self.enrich_nodes(nodes)

        # Step 4: 节点测试 / 从测试结果分流
        tested_nodes = nodes
        valid_nodes = nodes
        if not skip_test and not generate_only:
            logger.info("开始节点测试...")
            testing_config = self.config.get("testing", {})
            tester = NodeTester(testing_config)
            tested_nodes = await tester.test_all(nodes)

        # 从节点列表分流: 全部节点 vs 仅有效节点
        # generate_only 模式下，is_valid 标记来自 test_with_xray.py 的结果
        valid_nodes = [n for n in tested_nodes if n.is_valid]
        invalid_nodes = [n for n in tested_nodes if not n.is_valid]

        if len(valid_nodes) < len(tested_nodes):
            logger.info(f"节点校验: {len(valid_nodes)} 通过 / {len(invalid_nodes)} 失败 / {len(tested_nodes)} 总计")
        elif not valid_nodes and tested_nodes:
            # 无测试结果时全部保留
            valid_nodes = tested_nodes

        # 按频道统计校验结果, 更新 channel_results
        if self.channel_results:
            from collections import defaultdict
            ch_valid = defaultdict(int)
            ch_invalid = defaultdict(int)
            for node in tested_nodes:
                src = node.source or ""
                ch_name = src.split(":", 1)[1] if ":" in src else src
                if node.is_valid:
                    ch_valid[ch_name] += 1
                else:
                    ch_invalid[ch_name] += 1
            for name in self.channel_results:
                self.channel_results[name]["valid_nodes"] = ch_valid.get(name, 0)
                self.channel_results[name]["invalid_nodes"] = ch_invalid.get(name, 0)

        # Step 5: 过滤 (仅对有效节点做过滤)
        filter_config = self.config.get("filter", {})
        if filter_config.get("exclude_countries"):
            valid_nodes = self.deduplicator.filter_by_country(
                valid_nodes,
                exclude=filter_config["exclude_countries"]
            )
        if filter_config.get("exclude_keywords"):
            valid_nodes = self.deduplicator.filter_by_keywords(
                valid_nodes,
                filter_config["exclude_keywords"]
            )

        # Step 6: 限制节点数
        max_nodes = self.config.get("output", {}).get("max_nodes", 500)
        valid_nodes = self.deduplicator.limit_nodes(valid_nodes, max_nodes)

        logger.info(f"全部节点: {len(tested_nodes)} | 有效节点: {len(valid_nodes)}")

        # Step 7: 生成订阅 — 两套: 全部节点(主路径) + 校验通过的(pass/)
        output_dir = "output"
        self.generator.generate_all(tested_nodes, output_dir)

        # 校验通过的节点订阅 (pass/ 子目录)
        pass_output_dir = Path(output_dir) / "pass"
        pass_output_dir.mkdir(parents=True, exist_ok=True)
        self.generator.generate_all(valid_nodes, str(pass_output_dir))

        # 保存频道抓取结果到 output (供 generate_only 模式恢复)
        if self.channel_results:
            import json
            cr_path = Path(output_dir) / "channel_results.json"
            with open(cr_path, "w", encoding="utf-8") as f:
                json.dump(self.channel_results, f, indent=2, ensure_ascii=False)

        # Step 8: 复制到 docs 目录（用于 GitHub Pages）
        self._copy_to_docs(valid_nodes, tested_nodes)

        # 完成
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info(f"聚合完成，耗时: {elapsed:.2f} 秒")
        logger.info(f"有效节点: {len(valid_nodes)} | 全部节点: {len(tested_nodes)}")
        logger.info("=" * 60)

        return nodes

    def _load_existing_nodes(self) -> List[Node]:
        """加载现有节点数据"""
        import json

        nodes_file = Path("output/nodes.json")
        if not nodes_file.exists():
            logger.warning("节点数据文件不存在")
            return []

        with open(nodes_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        nodes = []
        for n in data.get("nodes", []):
            node = Node(
                name=n.get("name", "Unknown"),
                type=n.get("type", "vmess"),
                server=n.get("server", ""),
                port=n.get("port", 443),
                uuid=n.get("uuid"),
                password=n.get("password"),
                cipher=n.get("cipher"),
                network=n.get("network"),
                security=n.get("security"),
                sni=n.get("sni"),
                skip_cert_verify=n.get("skip_cert_verify", False),
                ws_path=n.get("ws_path"),
                ws_headers=n.get("ws_headers"),
                grpc_service_name=n.get("grpc_service_name"),
                reality_public_key=n.get("reality_public_key"),
                reality_short_id=n.get("reality_short_id"),
                fingerprint=n.get("fingerprint"),
                hysteria2_password=n.get("hysteria2_password"),
                flow=n.get("flow"),
                alterId=n.get("alterId", 0),
                country=n.get("country"),
                source=n.get("source"),
                latency=n.get("latency", 0),
            )
            node.is_valid = n.get("is_valid", False)
            node.tcp_valid = n.get("tcp_valid", False)
            node.missing_runs = n.get("missing_runs", 0)
            nodes.append(node)

        # 保留测试结果：is_valid=True 和 is_valid=False 的节点都保留
        valid = [n for n in nodes if n.is_valid]
        invalid = [n for n in nodes if not n.is_valid]
        if valid:
            logger.info(f"加载 {len(valid)} 个有效节点, {len(invalid)} 个失败节点")
        else:
            logger.info(f"无测试结果，加载全部 {len(nodes)} 个节点")
        return nodes

    def _merge_with_old(self, new_nodes: List[Node], old_map: dict) -> List[Node]:
        """合并本次抓取的节点与上次的节点

        策略:
        - 本次抓到的节点优先 (更新已有条目)
        - 上次有效但本次没抓到的节点保留, missing_runs += 1
        - 连续缺失超过 MAX_MISSING_RUNS 轮的节点自动清除
        """
        MAX_MISSING_RUNS = 3

        # 本次抓到的节点 key 集合
        new_keys = set()
        for n in new_nodes:
            key = (n.server, n.port)
            new_keys.add(key)
            # 如果旧节点有测试结果, 保留 is_valid 和 latency
            if key in old_map:
                old = old_map[key]
                # 旧节点的测试结果比新节点更有价值
                if old.is_valid and not n.is_valid:
                    n.is_valid = True
                    n.latency = old.latency
                # 保留 missing_runs 计数 (重置为 0, 因为本次抓到了)
                n.missing_runs = 0

        # 保留上次有效但本次没抓到的节点
        preserved = 0
        pruned = 0
        for key, old_node in old_map.items():
            if key not in new_keys:
                old_node.missing_runs = getattr(old_node, 'missing_runs', 0) + 1
                if old_node.missing_runs <= MAX_MISSING_RUNS:
                    new_nodes.append(old_node)
                    preserved += 1
                else:
                    pruned += 1

        if preserved or pruned:
            logger.info(f"节点合并: 本次 {len(new_keys)} 个, 保留历史 {preserved} 个, 清除过期 {pruned} 个")

        return new_nodes

    def _restore_channel_results(self):
        """从 output/channel_results.json 恢复频道抓取结果 (generate_only 模式使用)"""
        import json
        cr_path = Path("output/channel_results.json")
        if cr_path.exists() and not self.channel_results:
            with open(cr_path, "r", encoding="utf-8") as f:
                self.channel_results = json.load(f)
            logger.info(f"恢复频道抓取结果: {len(self.channel_results)} 个频道")

    def _copy_to_docs(self, valid_nodes: List[Node], all_nodes: List[Node]):
        """复制输出到 docs 目录（安全混淆路径）"""
        import shutil
        import hashlib
        import json

        docs_dir = Path("docs")
        docs_dir.mkdir(parents=True, exist_ok=True)

        output_dir = Path("output")

        # 获取访问 token（从环境变量或配置）
        access_token = os.environ.get("AURORA_TOKEN", "")
        if not access_token:
            access_token = self.config.get("security", {}).get("token", "aurora2026")

        # 混淆路径: docs/s/{token}/
        sub_dir = docs_dir / "s" / access_token
        sub_dir.mkdir(parents=True, exist_ok=True)

        # 清理旧的混淆目录（保留当前 token）
        s_dir = docs_dir / "s"
        if s_dir.exists():
            for d in s_dir.iterdir():
                if d.is_dir() and d.name != access_token:
                    shutil.rmtree(d, ignore_errors=True)

        # 复制全部节点订阅 (主路径)
        for filename in ["clash.yaml", "v2ray.txt", "singbox.json", "nodes.json"]:
            src = output_dir / filename
            if src.exists():
                shutil.copy(src, sub_dir / filename)

        # 复制校验通过的节点订阅 (pass/ 子目录)
        pass_dir = sub_dir / "pass"
        pass_dir.mkdir(parents=True, exist_ok=True)
        for filename in ["clash.yaml", "v2ray.txt", "singbox.json", "nodes.json"]:
            src = output_dir / "pass" / filename
            if src.exists():
                shutil.copy(src, pass_dir / filename)

        # 生成统计信息
        stats = self._generate_stats(valid_nodes, all_nodes)

        # 嵌入源配置 (供 Web UI 源管理使用, 避免额外请求 GitHub API)
        stats["tg_channels"] = self._load_tg_channel_config()
        stats["github_sources"] = self._load_github_source_config()

        # 将 GitHub Token 嵌入受保护的 stats.json（已通过密码门保护）
        gh_token = os.environ.get("AURORA_GH_TOKEN", "")
        if not gh_token:
            gh_token = self.config.get("security", {}).get("github_token", "")
        if gh_token:
            stats["gh_token"] = gh_token

        with open(sub_dir / "stats.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)

        # 生成频道抓取详情 (供 Web UI "查看最新" 使用)
        if self.channel_results:
            with open(sub_dir / "channel_results.json", "w", encoding="utf-8") as f:
                json.dump(self.channel_results, f, indent=2, ensure_ascii=False)
        elif not (sub_dir / "channel_results.json").exists():
            # generate_only 模式下 channel_results 为空, 保留之前的结果
            pass

        # 同时在根目录放一份不含节点详情的公开统计（仅显示数量）
        public_stats = {
            "total_nodes": stats.get("total_nodes", 0),
            "updated_at": stats.get("updated_at", ""),
        }
        with open(docs_dir / "stats.json", "w", encoding="utf-8") as f:
            json.dump(public_stats, f, indent=2, ensure_ascii=False)

        # 生成带 token hash 的 index.html
        self._build_secured_index(docs_dir, access_token)

        logger.info(f"输出已复制到 {sub_dir}")

    def _build_secured_index(self, docs_dir: Path, token: str):
        """生成带密码验证的 index.html"""
        import hashlib

        # 计算 token 的 SHA-256 hash
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        template_path = docs_dir / "index.html"
        if not template_path.exists():
            logger.warning("index.html 模板不存在，跳过安全注入")
            return

        content = template_path.read_text(encoding="utf-8")

        # 替换 hash 占位符
        content = content.replace("__AUTH_HASH_PLACEHOLDER__", token_hash)

        template_path.write_text(content, encoding="utf-8")
        logger.info(f"已注入安全验证 (hash: {token_hash[:16]}...)")

    def _load_tg_channel_config(self) -> list:
        """加载 Telegram 频道配置 (嵌入 stats.json 供 Web UI 使用)"""
        config_path = Path("config/sources/telegram.yaml")
        if not config_path.exists():
            return []
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
            channels = []
            for ch in config.get("channels", []):
                if not ch.get("enabled", True):
                    continue
                entry = {
                    "name": ch.get("name", ""),
                    "username": ch.get("username", ""),
                    "enabled": ch.get("enabled", True),
                    "format": ch.get("format", "auto"),
                }
                if ch.get("website_url"):
                    entry["website_url"] = ch["website_url"]
                if ch.get("sub_urls"):
                    entry["sub_urls"] = ch["sub_urls"]
                channels.append(entry)
            return channels
        except Exception as e:
            logger.warning(f"加载 TG 频道配置失败: {e}")
            return []

    def _load_github_source_config(self) -> list:
        """加载 GitHub 源配置 (嵌入 stats.json 供 Web UI 使用)"""
        config_path = Path("config/sources/github.yaml")
        if not config_path.exists():
            return []
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
            sources = []
            for src in config.get("sources", []):
                if not src.get("enabled", True):
                    continue
                sources.append({
                    "name": src.get("name", ""),
                    "url": src.get("url", ""),
                    "format": src.get("format", "auto"),
                    "enabled": src.get("enabled", True),
                })
            return sources
        except Exception as e:
            logger.warning(f"加载 GitHub 源配置失败: {e}")
            return []

    def _generate_stats(self, valid_nodes: List[Node], all_nodes: List[Node] = None) -> dict:
        """生成统计信息"""
        from collections import Counter

        if all_nodes is None:
            all_nodes = valid_nodes

        if not valid_nodes and not all_nodes:
            return {
                "total_nodes": 0,
                "updated_at": datetime.now().isoformat(),
            }

        # 按类型统计
        by_type = Counter(n.type for n in valid_nodes)

        # 按国家统计
        by_country = Counter(n.country for n in valid_nodes if n.country)

        # 按来源统计
        by_source = Counter(n.source for n in valid_nodes if n.source)

        # 平均延迟
        latencies = [n.latency for n in valid_nodes if n.latency > 0]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0

        return {
            "total_nodes": len(all_nodes),
            "valid_nodes": len(valid_nodes),
            "invalid_nodes": len(all_nodes) - len(valid_nodes),
            "avg_latency": int(avg_latency),
            "by_type": dict(by_type),
            "by_country": dict(by_country.most_common(20)),
            "by_source": dict(by_source),
            "channel_results": self.channel_results,
            "updated_at": datetime.now().isoformat(),
        }


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Aurora VPN Aggregator")
    parser.add_argument(
        "--config",
        "-c",
        default="config/settings.yaml",
        help="配置文件路径"
    )
    parser.add_argument(
        "--no-test",
        action="store_true",
        help="跳过节点测试"
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="仅生成订阅（从现有节点数据）"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="调试模式"
    )

    args = parser.parse_args()

    # 设置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 运行聚合器
    aggregator = AuroraAggregator(args.config)
    await aggregator.run(
        skip_test=args.no_test,
        generate_only=args.generate_only
    )


if __name__ == "__main__":
    asyncio.run(main())
