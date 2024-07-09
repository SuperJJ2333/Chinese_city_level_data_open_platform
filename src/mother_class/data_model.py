import re
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from common.form_utils import format_date


class DataModel:
    def __init__(self, title: str, subject: str, description: str, source_department: str, release_time: str,
                 update_time: str, open_conditions: str, data_volume: int, is_api: str, file_type: List[str],
                 access_count: int, download_count: int, api_call_count: int, link: str, update_cycle: str,
                 location: Optional[str] = None):
        """
        数据模型初始化

        参数:
        title (str): 标题
        subject (str): 主题
        description (str): 描述
        source_department (str): 来源部门
        release_time (str): 发布时间，格式为 'yyyy-mm-dd'
        update_time (str): 更新时间，格式为 'yyyy-mm-dd'
        open_conditions (str): 开放条件
        data_volume (int): 数据量
        is_api (str): 是否为API
        file_type (List[str]): 文件类型列表
        access_count (int): 访问次数
        download_count (int): 下载次数
        api_call_count (int): API调用次数
        link (str): 链接
        update_cycle (str): 更新周期
        location (Optional[str]): 位置
        """
        self.title = self.strip_string(title)
        self.subject = self.strip_string(subject)
        self.description = self.strip_string(description)
        self.source_department = self.strip_string(source_department)
        self.release_time = self.parse_date(self.strip_string(release_time))
        self.update_time = self.parse_date(self.strip_string(update_time))
        self.open_conditions = self.strip_string(open_conditions)
        self.data_volume = self.parse_int(data_volume)
        self.is_api = self.strip_string(is_api)
        self.file_type = file_type or []
        self.access_count = self.parse_int(access_count)
        self.download_count = self.parse_int(download_count)
        self.api_call_count = self.parse_int(api_call_count)
        self.link = self.strip_string(link)
        self.update_cycle = self.strip_string(update_cycle)
        self.location = self.strip_string(location) if location is not None else ""

    def strip_string(self, value: str) -> str:
        """
        移除字符串首尾的空白字符

        参数:
        value (str): 输入字符串

        返回:
        str: 去除首尾空白字符后的字符串
        """
        return value.strip() if isinstance(value, str) else value

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        """
        将日期字符串解析为 datetime 对象

        参数:
        date_str (str): 日期字符串，格式为 'yyyy-mm-dd'

        返回:
        datetime: 解析后的 datetime 对象
        """

        return format_date(date_str)

    @staticmethod
    def parse_int(num_str):
        """
        将日期字符串或数字解析为 int 对象，无效或缺失的输入返回 np.nan。

        参数:
        num_str (str or int or float): 要解析的数字或字符串。

        返回:
        int 或 np.nan: 解析后的整数或 NaN
        """
        if num_str is None or num_str == "":
            return np.nan  # 使用 np.nan 代替 None 表示无效或缺失的整数

        if isinstance(num_str, str):
            # 使用正则表达式匹配所有整数
            numbers = re.findall(r'\d+', num_str)
            # 将匹配到的字符串数字转换为整型
            if not numbers:
                return np.nan
            num = int(''.join(numbers))
        elif isinstance(num_str, float):
            if pd.isnull(num_str):
                return np.nan
            else:
                num = int(num_str)
        elif isinstance(num_str, int):
            num = num_str
        else:
            return np.nan  # 如果输入类型不是预期类型，返回 np.nan

        return num

    def to_dict(self) -> dict:
        """
        将对象的属性输出为字典格式

        返回:
        dict: 包含所有属性的字典
        """
        return {
            "location": self.location,
            "title": self.title,
            "subject": self.subject,
            "description": self.description,
            "source_department": self.source_department,
            "release_time": self.release_time,
            "update_time": self.update_time,
            "open_conditions": self.open_conditions,
            "data_volume": self.data_volume,
            "is_api": self.is_api,
            "file_type": self.file_type,
            "access_count": self.access_count,
            "download_count": self.download_count,
            "api_call_count": self.api_call_count,
            "link": self.link,
            "update_cycle": self.update_cycle
        }

    def __repr__(self):
        """
        返回对象的字符串表示

        返回:
        str: 对象的字符串表示
        """
        return (f"DataModel(title={self.title}, subject={self.subject}, description={self.description}, "
                f"source_department={self.source_department}, release_time={self.release_time}, "
                f"update_time={self.update_time}, open_conditions={self.open_conditions}, data_volume={self.data_volume}, "
                f"is_api={self.is_api}, file_type={self.file_type}, access_count={self.access_count}, "
                f"download_count={self.download_count}, api_call_count={self.api_call_count}, link={self.link}, "
                f"update_cycle={self.update_cycle})")

    def __dict__(self):
        return self.to_dict()


if __name__ == '__main__':
    # 示例用法
    data = DataModel(
        title="Sample Title",
        subject="Sample Subject",
        description="This is a sample description.",
        source_department="Sample Department",
        release_time="2023-05-01",
        update_time="2023-05-20",
        open_conditions="Open to public",
        data_volume=1000,
        is_api="yes",
        file_type=["csv", "json"],
        access_count=150,
        download_count=75,
        api_call_count=200,
        link="http://example.com",
        update_cycle="monthly"
    )

    print(data)
