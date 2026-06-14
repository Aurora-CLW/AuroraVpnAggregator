"""
网络工具
"""

import asyncio
import socket
import time
from typing import Tuple, Optional
from concurrent.futures import ThreadPoolExecutor


async def check_tcp_port(host: str, port: int, timeout: int = 3) -> bool:
    """
    检查 TCP 端口是否可达

    Args:
        host: 主机地址
        port: 端口号
        timeout: 超时时间（秒）

    Returns:
        是否可达
    """
    def _check():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _check)


async def check_tcp_ports_batch(
    hosts_ports: list,
    timeout: int = 3,
    concurrent: int = 100
) -> dict:
    """
    批量检查 TCP 端口

    Args:
        hosts_ports: [(host, port), ...] 列表
        timeout: 超时时间
        concurrent: 并发数

    Returns:
        {(host, port): bool, ...}
    """
    semaphore = asyncio.Semaphore(concurrent)

    async def check_with_limit(host, port):
        async with semaphore:
            result = await check_tcp_port(host, port, timeout)
            return (host, port), result

    tasks = [check_with_limit(host, port) for host, port in hosts_ports]
    results = await asyncio.gather(*tasks)

    return dict(results)


async def measure_latency(
    host: str,
    port: int,
    timeout: int = 5
) -> int:
    """
    测量 TCP 连接延迟

    Args:
        host: 主机地址
        port: 端口号
        timeout: 超时时间

    Returns:
        延迟（毫秒），失败返回 0
    """
    def _measure():
        try:
            start = time.time()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            latency = int((time.time() - start) * 1000)
            sock.close()
            return latency
        except Exception:
            return 0

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _measure)


async def http_request(
    url: str,
    method: str = "GET",
    timeout: int = 10,
    headers: dict = None
) -> Tuple[bool, int, Optional[str]]:
    """
    发送 HTTP 请求

    Args:
        url: 请求 URL
        method: 请求方法
        timeout: 超时时间
        headers: 请求头

    Returns:
        (是否成功, 状态码/延迟, 响应内容)
    """
    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            start = time.time()
            async with session.request(
                method,
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                latency = int((time.time() - start) * 1000)
                content = await response.text()
                return True, response.status, content
    except asyncio.TimeoutError:
        return False, 0, "Timeout"
    except Exception as e:
        return False, 0, str(e)


async def http_request_via_proxy(
    url: str,
    proxy_type: str,
    proxy_host: str,
    proxy_port: int,
    timeout: int = 10,
    **proxy_kwargs
) -> Tuple[bool, int]:
    """
    通过代理发送 HTTP 请求

    Args:
        url: 请求 URL
        proxy_type: 代理类型 (http/socks5)
        proxy_host: 代理主机
        proxy_port: 代理端口
        timeout: 超时时间

    Returns:
        (是否成功, 延迟毫秒)
    """
    import aiohttp
    import aiohttp_socks

    try:
        if proxy_type == "socks5":
            connector = aiohttp_socks.ProxyConnector(
                host=proxy_host,
                port=proxy_port,
                **proxy_kwargs
            )
        else:
            connector = None

        async with aiohttp.ClientSession(connector=connector) as session:
            start = time.time()
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                proxy=f"http://{proxy_host}:{proxy_port}" if proxy_type == "http" else None
            ) as response:
                latency = int((time.time() - start) * 1000)
                if response.status in [200, 204]:
                    return True, latency
                return False, 0
    except Exception:
        return False, 0
