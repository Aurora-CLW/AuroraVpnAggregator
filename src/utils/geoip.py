"""
地理位置识别
"""

import logging
import json
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

        # 缓存命中
        if ip in self._cache:
            return {**result, **self._cache[ip]}

        # 优先使用本地数据库
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

        # 回退到在线 API (单个查询)
        online_result = self._lookup_online(ip)
        if online_result:
            result.update(online_result)
        self._cache[ip] = dict(result)
        return result

    def _lookup_online(self, ip: str) -> Dict[str, Optional[str]]:
        """通过 ip-api.com 在线查询 (免费, 无需 key)"""
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
        except Exception as e:
            logger.debug(f"在线 GeoIP 查询失败 ({ip}): {e}")
        return {}

    def batch_lookup(self, ips: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """批量查询 IP 地理位置 (ip-api.com 批量 API, 一次最多 100 个)"""
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

        # 在线批量查询 (每批 100 个)
        if uncached:
            batch_results = self._batch_lookup_online(uncached)
            results.update(batch_results)
            for ip, r in batch_results.items():
                self._cache[ip] = r

        # 填充缓存结果
        for ip in ips:
            if ip and ip not in results and ip in self._cache:
                results[ip] = self._cache[ip]

        return results

    def _batch_lookup_online(self, ips: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """通过 ip-api.com 批量 API 查询 (POST, 一次最多 100 个)"""
        results: Dict[str, Dict[str, Optional[str]]] = {}
        batch_size = 100

        for i in range(0, len(ips), batch_size):
            batch = ips[i:i + batch_size]
            try:
                payload = json.dumps(batch).encode("utf-8")
                req = urllib.request.Request(
                    "http://ip-api.com/batch?fields=country,countryCode,city,regionName,isp&lang=en",
                    data=payload,
                    headers={"Content-Type": "application/json", "User-Agent": "AuroraVPN/1.0"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    if isinstance(data, list):
                        for j, item in enumerate(data):
                            if j < len(batch) and item.get("status") == "success":
                                r = {
                                    "country": item.get("countryCode"),
                                    "country_name": item.get("country"),
                                    "city": item.get("city"),
                                    "region": item.get("regionName"),
                                    "isp": item.get("isp"),
                                }
                                results[batch[j]] = r
                logger.info(f"GeoIP 批量查询: {len(batch)} 个 IP, 成功 {len([k for k in results if k in batch])} 个")
            except Exception as e:
                logger.warning(f"GeoIP 批量查询失败 (batch {i // batch_size + 1}): {e}")
                # 降级为逐个查询
                for ip in batch:
                    r = self._lookup_online(ip)
                    if r:
                        results[ip] = r

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
