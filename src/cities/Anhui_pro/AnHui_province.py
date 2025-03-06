import json
import math
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class AnHuiCrawler(PageBase):
    """
    post请求获取api数据
    二级跳转：在详情页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {
            'city_name': '马鞍山市',
            'name': 'Maanshan',
            'province': 'Anhui',
            'total_items_num': 526,
            'each_page_count': 10,
            'base_url': 'http://data.ahzwfw.gov.cn:8000/dataopen-web/data/findByPage'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Content-Length": "92",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": "JSESSIONID=04CA840FFF67040A2B160C3292CCFFB3; __jsluid_h=0bd39649bc8e5793e137cdaac9ce51f9",
            "Host": "data.ahzwfw.gov.cn:8000", "IFLYTEK_CSRFTOKEN": "null", "Origin": "http://data.ahzwfw.gov.cn:8000",
            "Proxy-Connection": "keep-alive", "Referer": "http://data.ahzwfw.gov.cn:8000/dataopen-web/api-data.html",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
            "screen_ratio": "1536*864"
        }

        self.city_type = [{'安徽省': ['3400', 1246]}, {'马鞍山市': ['3405', 140]}, {'铜陵市': ['3407', 122]}]

        self.params = {'pageSize': '10', 'pageNum': '1', 'xzqh': '3405'}

    def run(self):
        for city_type in self.city_type:
            self.name = list(city_type.keys())[0]
            self.total_items_num = city_type[self.name][1]
            self.city_code = city_type[self.name][0]
            self.total_page_num = math.ceil(self.total_items_num / self.each_page_num) + 1

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        response = self.session
        self.params['xzqh'] = self.city_code

        for i in range(1, self.total_page_num):
            self.params['pageNum'] = str(i)
            response.post(url=self.base_url, headers=self.headers, data=self.params, proxies=self.proxies)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_data(json_data)
                detailed_data = self.process_detailed_page(extracted_data)
                views_list.extend(detailed_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_detailed_page(self, views_list):

        session = SessionPage()
        detailed_url = 'http://data.ahzwfw.gov.cn:8000/dataopen-web/data/findByRid'
        params = {'rid': '00821a44dc5a451c99778ca55c74fa8b'}
        data_list = []

        for item in views_list:
            params['rid'] = item
            session.post(url=detailed_url, headers=self.headers, data=params, proxies=self.proxies)

            json_data = session.response.text
            if json_data:
                extracted_data = self.extract_page_data(json_data)
                data_list.append(extracted_data)

        return data_list

    def extract_data(self, json_data):

        data_dict = json_data
        info_list = []

        # 检查内容是否存在
        if data_dict['data']['rows']:
            # 遍历每个数据项，提取数据集名称和文件类型
            for item in data_dict['data']['rows']:
                # self.logger.info(f"爬取的id为：{item['rid']}")
                info_list.append(item['rid'])

        return info_list

    def extract_page_data(self, json_data):
        data = json.loads(json_data)
        item = data['data']

        title = item.get('catalogName', '')
        subject = item.get('belongFieldName', '')
        description = item.get('summary', '')
        source_department = item.get('providerDept', '')

        release_time = datetime.strptime(item.get('publishTime', ''), '%Y%m%d%H%M%S').strftime(
            '%Y-%m-%d') if item.get('publishTime', '') else ''
        update_time = datetime.strptime(item.get('updateTime', ''), '%Y%m%d%H%M%S').strftime(
            '%Y-%m-%d') if item.get('updateTime', '') else ''

        open_conditions = item.get('openTypeName', '')
        data_volume = None
        is_api = 'True' if 'API' in item.get('formats', []) else 'False'
        file_type = item.get('formats', [])

        access_count = item.get('llcs', None)
        download_count = item.get('xzcs', None)
        api_call_count = item.get('dycs', None)
        link = f'http://data.ahzwfw.gov.cn:8000/dataopen-web/api-data-details.html?rid={item.get("rid", "")}'  # Link is not provided in the data
        update_cycle = item.get('updateCycleTxt', '')

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_type, access_count, download_count,
                          api_call_count, link, update_cycle, location=self.name)

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
    page = AnHuiCrawler(is_headless=True)
    page.run()
