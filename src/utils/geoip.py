"""
地理位置识别
"""

import logging
import json
import time
import urllib.request
from typing import Optional, Dict, List
from pathlib import Path

logger = logging.getLogger(__name__)


class GeoIPLookup:
    """地理位置查询"""

    def __init__(self, db_path: str = "data/cache/GeoLite2-City.mmdb"):
        self.db_path = Path(db_path)
        self.reader = None
        self._cache: Dict[str, Dict] = {}
        self._init_reader()

    def _init_reader(self):
        try:
            import geoip2.database
            if self.db_path.exists() and self.db_path.stat().st_size > 1024 * 1024:
                self.reader = geoip2.database.Reader(str(self.db_path))
                logger.info(f"GeoIP 数据库加载成功: {self.db_path}")
            else:
                logger.warning(f"GeoIP 数据库不存在或过小, 将使用在线 API 作为回退")
        except ImportError:
            logger.warning("geoip2 未安装, 将使用在线 API 作为回退")
        except Exception as e:
            logger.warning(f"GeoIP 初始化失败: {e}, 将使用在线 API 作为回退")

    def lookup(self, ip: str) -> Dict[str, Optional[str]]:
        result = {
            "country": None,
            "country_name": None,
            "city": None,
            "region": None,
            "isp": None,
        }

        if not ip:
            return result

        if ip in self._cache:
            return {**result, **self._cache[ip]}

        if self.reader:
            try:
                response = self.reader.city(ip)
                result["country"] = response.country.iso_code
                result["country_name"] = response.country.name
                result["city"] = response.city.name
                result["region"] = response.subdivisions.most_specific.name if response.subdivisions else None
                result["isp"] = response.traits.isp
                if result["country"]:
                    self._cache[ip] = dict(result)
                    return result
            except Exception:
                pass

        online_result = self._lookup_online(ip)
        if online_result:
            result.update(online_result)
        self._cache[ip] = dict(result)
        return result

    def _lookup_online(self, ip: str) -> Dict[str, Optional[str]]:
        """在线查询: ipwho.is → ip-api.com 降级"""
        # 源 1: ipwho.is (免费, 无速率限制)
        r = self._lookup_ipwhois(ip)
        if r:
            return r
        # 源 2: ip-api.com (免费, 45 req/min)
        r = self._lookup_ipapi(ip)
        if r:
            return r
        return {}

    def _lookup_ipwhois(self, ip: str) -> Dict[str, Optional[str]]:
        try:
            url = f"https://ipwho.is/{ip}"
            req = urllib.request.Request(url, headers={"User-Agent": "AuroraVPN/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("success", False) and data.get("country_code"):
                    return {
                        "country": data.get("country_code"),
                        "country_name": data.get("country"),
                        "city": data.get("city"),
                        "region": data.get("region"),
                        "isp": data.get("connection", {}).get("isp"),
                    }
        except Exception:
            pass
        return {}

    def _lookup_ipapi(self, ip: str) -> Dict[str, Optional[str]]:
        try:
            url = f"http://ip-api.com/json/{ip}?fields=country,countryCode,city,regionName,isp&lang=en"
            req = urllib.request.Request(url, headers={"User-Agent": "AuroraVPN/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("status") == "success":
                    return {
                        "country": data.get("countryCode"),
                        "country_name": data.get("country"),
                        "city": data.get("city"),
                        "region": data.get("regionName"),
                        "isp": data.get("isp"),
                    }
        except Exception:
            pass
        return {}

    def batch_lookup(self, ips: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """批量查询 IP 地理位置 — 去重 + 多源 API"""
        results: Dict[str, Dict[str, Optional[str]]] = {}
        uncached = [ip for ip in ips if ip and ip not in self._cache]

        # 本地数据库查询
        if self.reader:
            still_uncached = []
            for ip in uncached:
                try:
                    response = self.reader.city(ip)
                    r = {
                        "country": response.country.iso_code,
                        "country_name": response.country.name,
                        "city": response.city.name,
                        "region": response.subdivisions.most_specific.name if response.subdivisions else None,
                        "isp": response.traits.isp,
                    }
                    if r["country"]:
                        results[ip] = r
                        self._cache[ip] = r
                        continue
                except Exception:
                    pass
                still_uncached.append(ip)
            uncached = still_uncached

        # 在线查询: ipwho.is 批量 → ip-api.com 降级
        if uncached:
            online_results = self._batch_lookup_online(uncached)
            results.update(online_results)
            for ip, r in online_results.items():
                self._cache[ip] = r

        # 填充缓存
        for ip in ips:
            if ip and ip not in results and ip in self._cache:
                results[ip] = self._cache[ip]

        return results

    def _batch_lookup_online(self, ips: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """在线批量查询: 并发 ipwho.is → 降级 ip-api.com"""
        import concurrent.futures
        import re
        results: Dict[str, Dict[str, Optional[str]]] = {}

        # 过滤非公网 IP (域名、内网、保留地址)
        _private_re = re.compile(
            r'^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.|127\.|0\.|169\.254\.|::1?$|fc|fe80)'
        )
        def _is_public_ip(ip: str) -> bool:
            # 纯数字+点 = 可能是 IP
            if not re.match(r'^[\d.]+$', ip):
                return False
            return not _private_re.match(ip)

        public_ips = [ip for ip in ips if _is_public_ip(ip)]
        skipped = len(ips) - len(public_ips)
        if skipped:
            logger.info(f"GeoIP: 跳过 {skipped} 个非公网 IP (域名/内网)")

        # 阶段 1: ipwho.is 并发查询 (无速率限制, 20 线程)
        logger.info(f"GeoIP 在线查询: 并发查询 {len(public_ips)} 个 IP (ipwho.is)...")
        failed = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            future_to_ip = {pool.submit(self._lookup_ipwhois, ip): ip for ip in public_ips}
            done = 0
            for future in concurrent.futures.as_completed(future_to_ip):
                ip = future_to_ip[future]
                done += 1
                try:
                    r = future.result()
                    if r and r.get("country"):
                        results[ip] = r
                    else:
                        failed.append(ip)
                except Exception:
                    failed.append(ip)
                if done % 100 == 0:
                    logger.info(f"GeoIP 进度: {done}/{len(public_ips)} (成功 {len(results)})")
        logger.info(f"GeoIP ipwho.is 完成: {len(results)}/{len(public_ips)} 成功, {len(failed)} 失败")

        # 阶段 2: ip-api.com 降级 (最多查 200 个, 避免耗时过长)
        if failed:
            fallback_batch = failed[:200]
            logger.info(f"GeoIP 降级: ip-api.com 查询 {len(fallback_batch)} 个 IP...")
            for i, ip in enumerate(fallback_batch):
                r = self._lookup_ipapi(ip)
                if r and r.get("country"):
                    results[ip] = r
                if (i + 1) % 45 == 0 and i < len(fallback_batch) - 1:
                    time.sleep(60)
            logger.info(f"GeoIP ip-api.com 完成: {len(results)}/{len(public_ips)} 总成功")

        return results

    def get_country(self, ip: str) -> Optional[str]:
        result = self.lookup(ip)
        return result.get("country")

    def get_country_name(self, ip: str) -> Optional[str]:
        result = self.lookup(ip)
        return result.get("country_name")

    def close(self):
        if self.reader:
            self.reader.close()
            self.reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 国家代码到国旗 emoji 映射
COUNTRY_FLAG = {
    "US": "🇺🇸", "HK": "🇭🇰", "JP": "🇯🇵", "KR": "🇰🇷",
    "SG": "🇸🇬", "TW": "🇹🇼", "DE": "🇩🇪", "GB": "🇬🇧",
    "FR": "🇫🇷", "NL": "🇳🇱", "CA": "🇨🇦", "AU": "🇦🇺",
    "RU": "🇷🇺", "IN": "🇮🇳", "BR": "🇧🇷", "IT": "🇮🇹",
    "ES": "🇪🇸", "AR": "🇦🇷", "TH": "🇹🇭", "VN": "🇻🇳",
    "MY": "🇲🇾", "ID": "🇮🇩", "PH": "🇵🇭", "NZ": "🇳🇿",
    "CH": "🇨🇭", "SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰",
    "FI": "🇫🇮", "PL": "🇵🇱", "AT": "🇦🇹", "BE": "🇧🇪",
    "IE": "🇮🇪", "PT": "🇵🇹", "CZ": "🇨🇿", "RO": "🇷🇴",
    "HU": "🇭🇺", "IL": "🇮🇱", "TR": "🇹🇷", "ZA": "🇿🇦",
    "AE": "🇦🇪", "SA": "🇸🇦", "EG": "🇪🇬", "NG": "🇳🇬",
    "KE": "🇰🇪", "MX": "🇲🇽", "CO": "🇨🇴", "CL": "🇨🇱",
    "PE": "🇵🇪", "UA": "🇺🇦", "KZ": "🇰🇿", "IR": "🇮🇷",
}


def get_country_flag(country_code: str) -> str:
    """
    获取国家国旗 emoji

    Args:
        country_code: 国家代码

    Returns:
        国旗 emoji，未知返回 🌍
    """
    if not country_code:
        return "🌍"
    return COUNTRY_FLAG.get(country_code.upper(), "🌍")
