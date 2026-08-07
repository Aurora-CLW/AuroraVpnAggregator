#!/usr/bin/env python3
"""逐个测试 Telegram 频道的抓取能力，报告每个频道能否获取消息及提取的节点数"""
import asyncio
import sys
from pathlib import Path
import yaml

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.handlers.telegram_handler import TelegramHandler

async def test_channel(name, ch):
    handler = TelegramHandler({"channels": [ch]})
    try:
        nodes = await handler._fetch_channel(ch)
        result = handler.channel_results.get(name, {})
        status = result.get("status", "unknown")
        sub_urls = result.get("sub_urls", [])
        print(f"[{name}] nodes={len(nodes)} status={status} sub_urls={len(sub_urls)}")
        return name, len(nodes), status, len(sub_urls)
    except Exception as e:
        print(f"[{name}] ERROR: {type(e).__name__}: {str(e)[:150]}")
        return name, 0, f"ERROR:{type(e).__name__}", 0

async def main():
    config_path = project_root / "config" / "sources" / "telegram.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    channels = [ch for ch in config.get("channels", []) if ch.get("enabled", True)]
    print(f"共 {len(channels)} 个频道待测试\n")

    results = []
    for i, ch in enumerate(channels):
        name = ch.get("name", "unknown")
        results.append(await test_channel(name, ch))
        if i < len(channels) - 1:
            await asyncio.sleep(1)

    print("\n" + "=" * 60)
    print(f"{'频道':<22} {'节点':>5}  {'状态':<28} 订阅链接")
    print("=" * 60)
    for name, n, status, sub in results:
        print(f"{name:<22} {n:>5}  {status:<28} {sub}")
    print("=" * 60)
    ok = sum(1 for _, n, status, _ in results if n > 0)
    print(f"成功获取节点的频道: {ok}/{len(results)}")

if __name__ == "__main__":
    asyncio.run(main())
