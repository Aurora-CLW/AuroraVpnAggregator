#!/usr/bin/env python3
"""单独测试单个频道 (避免批量运行时 RSS API 限流), 验证频道是否真的能获取节点/订阅链接"""
import asyncio
import sys
from pathlib import Path
import yaml

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.handlers.telegram_handler import TelegramHandler

async def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None
    config_path = project_root / "config" / "sources" / "telegram.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    channels = [ch for ch in config.get("channels", []) if ch.get("enabled", True)]
    if target:
        channels = [ch for ch in channels if ch.get("name", "") == target or ch.get("username", "").lstrip("@") == target]
    if not channels:
        print(f"未找到频道: {target}")
        return

    for ch in channels:
        name = ch.get("name", "unknown")
        handler = TelegramHandler({"channels": [ch]})
        try:
            nodes = await handler._fetch_channel(ch)
            result = handler.channel_results.get(name, {})
            print(f"\n[{name}] nodes={len(nodes)} status={result.get('status')}")
            print(f"  direct_nodes={result.get('direct_nodes')} valid_sub_urls={result.get('valid_sub_urls')} failed_sub_urls={result.get('failed_sub_urls')} dead_sub_urls={result.get('dead_sub_urls')}")
            # 列出前 5 个节点示例
            for n in nodes[:5]:
                print(f"  - {n.type} {n.server}:{n.port}")
        except Exception as e:
            print(f"\n[{name}] ERROR: {type(e).__name__}: {str(e)[:200]}")

if __name__ == "__main__":
    asyncio.run(main())
