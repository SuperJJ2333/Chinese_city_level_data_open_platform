import json
import time
from datetime import datetime

import pandas as pd
from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuangdongCrawler(PageBase):
    """
    post 请求获取api数据
    一级跳转：直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '深圳市',
                     'province': 'Guangdong',
                     'total_items_num': 3985 + 1200,
                     'each_page_count': 20,
                     'base_url': 'https://gddata.gd.gov.cn/backOpen/open/data/data/openCatalog/findPageBySearch'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Content-Type": "application/json",
            "Referer": "https://gddata.gd.gov.cn/opdata/index",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        }

        self.params = '{"pageSize":20,"pageNo":2,"officecCode":"37","subjectType":"","sourceType":"","keyword":"","openType":""}'

        self.params = json.loads(self.params)

        self.city_code = {
            '韶关市': '37',
            '珠海市': '40',
            '汕头市': '28',
            '佛山市': '38',
            '江门市': '47',
            '湛江市': '32',
            '茂名市': '31',
            '肇庆市': '518',
            '惠州市': '30',
            '梅州市': '58',
            '汕尾市': '59',
            '河源市': '510',
            '阳江市': '511',
            '清远市': '512',
            '东莞市': '513',
            '潮州市': '515',
            '揭阳市': '516',
            '云浮市': '517',
        }

    def run(self):
        session = self.session
        for city_name, city_code in self.city_code.items():
            self.name = city_name
            self.params['officecCode'] = city_code

            # 发送post请求
            session.post(url=self.base_url, headers=self.headers, json=self.params, proxies=self.proxies)
            # 计算总页数
            data = session.response.text
            json_data = json.loads(data)
            self.total_items_num = json_data['data']['cityCount']
            self.total_page_num = self.count_page_num()

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNo'] = i
            response.post(url=self.base_url, headers=self.headers, json=self.params, proxies=self.proxies)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_page_data(json_data)
                views_list.extend(extracted_data)
                self.logger.info(
                    f'{self.name} - 第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, resId):
        '''
        1. 发送post请求，获取api数据
        2. 解析api数据，获取开放条件和数据量
        :param resId:
        :return:
        '''

        url = 'https://gddata.gd.gov.cn/backOpen/open/data/data/openCatalog/selectDataCatalogByResId'

        params = '{"resId":"3070220200043/000089"}'

        params = json.loads(params)
        params['resId'] = resId
        session = SessionPage()

        session.post(url=url, headers=self.headers, json=params, proxies=self.proxies)

        response = session.json

        json_data = response

        item = json_data['data']

        open_conditions = item.get('openMode', '')

        data_volume = item.get('recordTotal', None)

        return open_conditions, data_volume

    def extract_page_data(self, json_data):
        data = json_data
        results = data['data']['page']['list']  # 根据新的数据路径调整
        models = []

        for item in results:
            title = item.get('resTitle', '')
            subject = item.get('subjectName', '')
            description = item.get('resAbstract', '')
            source_department = item.get('officeName', '')

            release_time = datetime.strptime(item.get('pubDate', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') \
                if item.get('pubDate') else None
            update_time = datetime.strptime(item.get('dataUpdateTime', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') \
                if item.get('dataUpdateTime') else None

            is_api = 'True' if '1' in item.get('sourceType', '') else 'False'
            file_type = item.get('sourceSuffix', '').split(',')

            access_count = item.get('visits', None)
            download_count = item.get('downloads', None)
            api_call_count = item.get('invokedCount', None)
            link = ''  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('updateCycle', ''))
            location = self.name

            open_conditions, data_volume = self.process_page(item.get('resId'))

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, location)
            models.append(model.to_dict())

        return models

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
            1: "实时更新",
            2: "按日更新",
            3: "按周更新",
            4: "按月更新",
            5: "按季更新",
            6: "半年更新",
            7: "按年更新",
            8: "不更新",
            99: "其它"
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
    page = GuangdongCrawler(is_headless=True)
    page.run()
