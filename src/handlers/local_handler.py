"""
本地文件处理器
"""

import logging
from typing import List, Optional
from pathlib import Path

from .base import BaseHandler
from ..models.node import Node
from ..core.parser import Parser

logger = logging.getLogger(__name__)


class LocalHandler(BaseHandler):
    """本地文件处理器"""

    def __init__(self, config: dict):
        super().__init__(config)
        self.path = config.get("path", "")
        self.format = config.get("format", "auto")
        self.parser = Parser()

    async def fetch(self) -> List[Node]:
        """读取本地文件"""
        if not self.enabled or not self.path:
            return []

        content = self._read_file()

        if not content:
            return []

        # 解析节点
        nodes = self.parser.parse(content, self.format)

        # 标记来源
        self.mark_source(nodes)

        logger.info(f"[{self.name}] 读取完成: {len(nodes)} 个节点")
        return nodes

    def _read_file(self) -> Optional[str]:
        """读取文件内容"""
        try:
            file_path = Path(self.path)

            # 如果是相对路径，转换为绝对路径
            if not file_path.is_absolute():
                # 从项目根目录查找
                project_root = Path(__file__).parent.parent.parent
                file_path = project_root / self.path

            if not file_path.exists():
                logger.warning(f"[{self.name}] 文件不存在: {file_path}")
                return None

            content = file_path.read_text(encoding="utf-8")
            logger.debug(f"[{self.name}] 读取成功: {len(content)} 字节")
            return content

        except Exception as e:
            logger.error(f"[{self.name}] 读取失败: {e}")
            return None
