import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class XianningCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """
    def __init__(self, is_headless=True):
        city_info = {'name': '咸宁市',
                     'province': 'Hubei',
                     'total_items_num': 23 + 500,
                     'each_page_count': 23,
                     'base_url': 'http://www.xianning.gov.cn/cysjdy/sjmljson/sjml.json'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",

        }

        self.city_type = [{'Maanshan': ['3405', 40]}, {'Tonglin': ['3407', 22]}]

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            session.get(url=url, proxies=self.proxies, headers=self.headers)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.html
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_data(json_data)
                processed_data = self.process_data(extracted_data)
                views_list.extend(processed_data)
                self.logger.info(f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_data(self, extracted_data):

        session = self.page
        item_data = []

        for item in extracted_data:
            url = item.get('link')
            session.get(url=url)

            item_data.append(self.extract_page_data(session, item))

        return item_data

    def extract_data(self, json_data):
        data = json.loads(json_data.replace("\n", ""))
        data_list = []

        for item in data:
            url = item.get('link')
            file_type = item.get('chnldesc')
            title = item.get('title')
            topic = item.get('theme')
            department = item.get('department')

            release_date = item.get('pubDate')
            update_date = item.get('update')

            data_list.append({
                'link': url,
                'file_type': file_type,
                'title': title,
                'topic': topic,
                'department': department,
                'release_date': release_date,
                'update_date': update_date,
            })

        return data_list

    def extract_page_data(self, session, item):

        title = item.get('title')
        subject = item.get('theme')
        description = title
        source_department = item.get('department')

        release_time = item.get('release_date')
        update_time = item.get('update_date')

        open_conditions = ''
        data_volume = None

        file_type = [item.get('chnldesc')]

        access_count = session.ele('x://*[@id="djl"]').text if session.ele('x://*[@id="djl"]') else None
        download_count = session.ele('x://*[@id="xzl"]').text if session.ele('x://*[@id="xzl"]') else None
        api_call_count = session.ele('x://*[@id="dyl"]').text if session.ele('x://*[@id="dyl"]') else None

        is_api = 'True' if '接口' in title else 'False'

        link = item.get('link')  # 假设没有具体的链接信息可用

        update_cycle = ''

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    @staticmethod
    def format_update_cycle(it):
        """
        根据传入的整数值返回对应的更新周期描述。

        参数:
        it (int or str): 更新周期的代码，可以是字符串或整数。

        返回:
        str: 对应的更新周期描述。
        """
        # 定义映射字典
        cycle_dict = {
            0: '实时',
            1: '每日',
            2: '每月',
            3: '每周',
            4: '每季度',
            5: '每半年',
            6: '每年',
            7: '自定义',
            8: '每两年'
        }

        # 处理空字符串或长度为0的情况
        if it == '' or (isinstance(it, str) and len(it) == 0):
            return ''

        # 尝试将输入转换为整数
        try:
            it = int(it)
        except ValueError:
            return it  # 如果转换失败，返回原始输入

        # 根据字典返回对应的更新周期描述
        return cycle_dict.get(it, it)  # 如果没有匹配的键，返回原始输入


if __name__ == '__main__':
    page = XianningCrawler(is_headless=True)
    page.run()
