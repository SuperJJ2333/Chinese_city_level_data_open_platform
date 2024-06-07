import copy
import json
import math
import sys
import threading
import time
from typing import List, Callable
import requests
from DrissionPage import ChromiumPage, ChromiumOptions, SessionPage
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from lxml import html
from requests import RequestException
import urllib3

from src.common.content_utils import *
from src.common.form_utils import *

# 禁用 InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss} - <level>{level}</level> - {message}</green>",
    level="INFO",
)


class PageBase:
    def __init__(self, city_info: dict, is_headless=False, thread_num: int = 5, proxies: dict = None):
        """
        初始化爬虫母类。

        参数:
        city_info (dict): 城市信息字典，包含 'name', 'base_url', 'province', 'total_page_num', 'each_page_num' 等键。
        content_xpath (dict): 用于解析页面的XPath字典。
        is_headless (bool): 是否启用无头浏览器模式。
        thread_num (int): 线程数量。
        proxies (dict): 代理设置字典。
        """
        self.name = city_info['name']
        self.province = city_info['province']
        self.base_url = city_info['base_url']
        self.total_items_num = city_info.get('total_items_num', 0)
        self.each_page_num = city_info.get('each_page_count', 0)
        self.is_api = False if city_info.get('is_api', 'False') == 'False' else True

        # 设置日志信息
        self.logger = logger

        if self.total_items_num != 0:
            self.total_page_num = math.ceil(self.total_items_num / self.each_page_num) + 1

        self.is_headless = is_headless
        self.thread_num = thread_num
        self.proxies = proxies if proxies is not None else {"http": None, "https": None}

        self.data_list = []
        self.total_data = []
        self.lock = threading.Lock()

        self.page, self.session = self.start_page()

    def start_page(self):
        """根据是否为无头模式初始化浏览器页面。"""
        options = ChromiumOptions()
        options.auto_port()
        if self.is_headless:
            options.headless()
        page = ChromiumPage(options)

        logger.info(f"{self.name} - 浏览器初始化完成 - 爬虫最大进程数为{self.thread_num}")
        session = SessionPage()

        return page, session

    def get_content(self):
        """获取新闻内容。"""
        self.total_data = process_news_data(self.data_list, self.page, self.total_data)
        self.save_files()

    def save_files(self):
        """保存数据到文件。"""
        logger.success(f"{self.name} - 共爬取数据{len(self.total_data)}条")

        self.page.quit()
        self.session.close()

        save_to_excel(self.total_data, self.name, self.province)

