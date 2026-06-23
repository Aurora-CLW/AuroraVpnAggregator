#!/usr/bin/env python3
"""
使用 xray-core 真实代理测试节点
在 GitHub Actions 中运行，需要先安装 xray
"""

import asyncio
import json
import subprocess
import tempfile
import time
import os
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.node import Node


def find_xray() -> str:
    for cmd in ["xray", "/usr/local/bin/xray", "/usr/bin/xray"]:
        try:
            result = subprocess.run([cmd, "version"], capture_output=True, timeout=5)
            if result.returncode == 0:
                return cmd
        except Exception:
            continue
    return ""


def _validate_node(node: Node) -> Optional[str]:
    """检查节点是否有足够的参数生成有效 xray 配置"""
    if not node.server:
        return "missing server"
    try:
        port = int(node.port) if node.port else 0
        if port <= 0:
            return "missing port"
    except (ValueError, TypeError):
        return "invalid port"

    if node.type == "vmess":
        if not node.uuid:
            return "vmess: missing uuid"
    elif node.type == "vless":
        if not node.uuid:
            return "vless: missing uuid"
        if node.security == "reality":
            if not node.reality_public_key:
                return "vless+reality: missing publicKey"
            if not node.fingerprint:
                return "vless+reality: missing fingerprint"
    elif node.type == "trojan":
        if not node.password:
            return "trojan: missing password"
    elif node.type == "ss":
        if not node.cipher or not node.password:
            return "ss: missing cipher/password"
    elif node.type == "hysteria2":
        return "hysteria2: not supported by xray"
    elif node.type == "anytls":
        return "anytls: not supported by xray"
    else:
        return f"unsupported type: {node.type}"

    return None


def _safe_port(node: Node) -> int:
    """安全获取节点端口为 int"""
    try:
        return int(node.port) if node.port else 443
    except (ValueError, TypeError):
        return 443


def build_xray_config(node: Node, socks_port: int) -> dict:
    """为单个节点生成 xray 配置"""
    inbound = {
        "port": socks_port,
        "listen": "127.0.0.1",
        "protocol": "socks",
        "settings": {"auth": "noauth", "udp": True},
    }

    outbound = _node_to_xray_outbound(node)
    if not outbound:
        return {}

    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [inbound],
        "outbounds": [outbound, {"protocol": "freedom", "tag": "direct"}],
    }
    return config


def _node_to_xray_outbound(node: Node) -> Optional[dict]:
    """Node 转换为 xray outbound 配置"""
    port = _safe_port(node)
    outbound = {
        "protocol": "",
        "settings": {},
        "tag": "proxy",
    }

    if node.type == "vmess":
        outbound["protocol"] = "vmess"
        outbound["settings"]["vnext"] = [{
            "address": node.server,
            "port": port,
            "users": [{
                "id": node.uuid or "",
                "alterId": node.alterId,
                "security": node.cipher or "auto",
            }],
        }]

    elif node.type == "vless":
        outbound["protocol"] = "vless"
        users = {"id": node.uuid or "", "encryption": "none"}
        if node.flow:
            users["flow"] = node.flow
        outbound["settings"]["vnext"] = [{
            "address": node.server,
            "port": port,
            "users": [users],
        }]

    elif node.type == "trojan":
        outbound["protocol"] = "trojan"
        outbound["settings"]["servers"] = [{
            "address": node.server,
            "port": port,
            "password": node.password or "",
        }]

    elif node.type == "ss":
        outbound["protocol"] = "shadowsocks"
        outbound["settings"]["servers"] = [{
            "address": node.server,
            "port": port,
            "method": node.cipher or "aes-256-gcm",
            "password": node.password or "",
        }]

    else:
        return None

    # 传输层
    stream = {"network": node.network or "tcp"}

    if node.network == "ws":
        ws = {"path": node.ws_path or "/"}
        if node.ws_headers:
            ws["headers"] = node.ws_headers
        stream["wsSettings"] = ws
    elif node.network == "grpc":
        stream["grpcSettings"] = {
            "serviceName": node.grpc_service_name or "",
        }

    # TLS
    if node.security == "tls" or node.type in ["trojan"]:
        tls = {"allowInsecure": node.skip_cert_verify}
        if node.sni:
            tls["serverName"] = node.sni
        tls["fingerprint"] = node.fingerprint or "chrome"
        stream["security"] = "tls"
        stream["tlsSettings"] = tls

    if node.security == "reality":
        stream["security"] = "reality"
        reality = {}
        if node.reality_public_key:
            reality["publicKey"] = node.reality_public_key
        if node.reality_short_id:
            reality["shortId"] = node.reality_short_id
        if node.sni:
            reality["serverName"] = node.sni
        reality["fingerprint"] = node.fingerprint or "chrome"
        stream["realitySettings"] = reality

    outbound["streamSettings"] = stream
    return outbound


async def test_node_with_xray(
    node: Node, xray_bin: str, socks_port: int, timeout: int = 5
) -> bool:
    """用 xray 测试单个节点"""
    config = build_xray_config(node, socks_port)
    if not config:
        return False

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(config, f)
        config_path = f.name

    process = None
    try:
        process = subprocess.Popen(
            [xray_bin, "run", "-c", config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        await asyncio.sleep(1.0)

        if process.poll() is not None:
            return False

        start = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                "--max-time", str(timeout),
                "--socks5-hostname", f"127.0.0.1:{socks_port}",
                "http://www.gstatic.com/generate_204",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout + 2)
            elapsed = int((time.time() - start) * 1000)

            status = stdout.decode().strip() if stdout else ""

            if status in ("200", "204", "301", "302"):
                node.latency = elapsed
                node.is_valid = True
                return True

        except (asyncio.TimeoutError, Exception):
            pass

        return False

    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        await asyncio.sleep(0.1)
        try:
            os.unlink(config_path)
        except OSError:
            pass


async def test_nodes_batch(
    nodes: List[Node], xray_bin: str, concurrent: int = 20, timeout: int = 5
) -> List[Node]:
    """批量测试节点（并发执行，每个节点独占端口）"""
    # 先过滤可测试的节点
    testable = []
    skipped = 0
    for node in nodes:
        if _validate_node(node) is None:
            testable.append(node)
        else:
            node.is_valid = False
            skipped += 1

    print(f"  可测试: {len(testable)}, 跳过: {skipped}")

    valid_nodes = []
    tested = 0
    base_port = 20000
    semaphore = asyncio.Semaphore(concurrent)

    async def _test_one(idx: int, node: Node):
        nonlocal tested
        port = base_port + idx
        async with semaphore:
            is_valid = await test_node_with_xray(node, xray_bin, port, timeout)
            tested += 1
            if is_valid:
                valid_nodes.append(node)
            if tested % 50 == 0:
                print(f"  已测试 {tested}/{len(testable)}, 有效 {len(valid_nodes)}")

    tasks = [_test_one(i, n) for i, n in enumerate(testable)]
    await asyncio.gather(*tasks)

    print(f"  测试完成: {tested} 个测试, {skipped} 个跳过, {len(valid_nodes)} 个有效")
    return valid_nodes


def node_to_dict(n: Node) -> dict:
    """将节点转换为完整字典（保留所有协议参数）"""
    return {
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


async def main():
    xray_bin = find_xray()
    if not xray_bin:
        print("错误: 未找到 xray，跳过代理测试")
        sys.exit(1)

    print(f"使用 xray: {xray_bin}")

    nodes_file = Path("output/nodes.json")
    if not nodes_file.exists():
        print("错误: output/nodes.json 不存在")
        sys.exit(1)

    with open(nodes_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_nodes = data.get("nodes", [])
    print(f"加载 {len(raw_nodes)} 个节点待测试")

    nodes = []
    for n in raw_nodes:
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
        nodes.append(node)

    # 按类型统计
    from collections import Counter
    type_counts = Counter(n.type for n in nodes)
    print(f"类型分布: {dict(type_counts)}")

    # 并发测试所有节点
    valid_nodes = await test_nodes_batch(nodes, xray_bin, concurrent=20, timeout=5)

    print(f"\n测试完成: {len(valid_nodes)}/{len(nodes)} 有效")

    # 写回结果：包含所有节点，更新测试状态
    result_data = {
        "version": "1.0.0",
        "updated_at": data.get("updated_at", ""),
        "total": len(nodes),
        "valid_count": len(valid_nodes),
        "nodes": [node_to_dict(n) for n in nodes],
    }

    with open(nodes_file, "w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False)

    print(f"已更新 {nodes_file} (全部 {len(nodes)} 个节点，含测试状态)")


if __name__ == "__main__":
    asyncio.run(main())
