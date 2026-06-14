"""
地理位置识别
"""

from typing import Optional, Dict
from pathlib import Path


class GeoIPLookup:
    """地理位置查询"""

    def __init__(self, db_path: str = "data/cache/GeoLite2-City.mmdb"):
        """
        初始化查询器

        Args:
            db_path: GeoIP 数据库路径
        """
        self.db_path = Path(db_path)
        self.reader = None
        self._init_reader()

    def _init_reader(self):
        """初始化数据库读取器"""
        try:
            import geoip2.database
            if self.db_path.exists():
                self.reader = geoip2.database.Reader(str(self.db_path))
        except ImportError:
            pass
        except Exception as e:
            print(f"GeoIP 初始化失败: {e}")

    def lookup(self, ip: str) -> Dict[str, Optional[str]]:
        """
        查询 IP 地理位置

        Args:
            ip: IP 地址

        Returns:
            {
                "country": "US",
                "country_name": "United States",
                "city": "New York",
                "region": "New York",
                "isp": "Cloudflare"
            }
        """
        result = {
            "country": None,
            "country_name": None,
            "city": None,
            "region": None,
            "isp": None,
        }

        if not self.reader:
            return result

        try:
            import geoip2.database
            response = self.reader.city(ip)

            result["country"] = response.country.iso_code
            result["country_name"] = response.country.name
            result["city"] = response.city.name
            result["region"] = response.subdivisions.most_specific.name if response.subdivisions else None
            result["isp"] = response.traits.isp

        except Exception:
            pass

        return result

    def get_country(self, ip: str) -> Optional[str]:
        """
        获取国家代码

        Args:
            ip: IP 地址

        Returns:
            国家代码 (如 US, JP, HK)
        """
        result = self.lookup(ip)
        return result.get("country")

    def get_country_name(self, ip: str) -> Optional[str]:
        """
        获取国家名称

        Args:
            ip: IP 地址

        Returns:
            国家名称
        """
        result = self.lookup(ip)
        return result.get("country_name")

    def close(self):
        """关闭数据库连接"""
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
