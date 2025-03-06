import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class FujianCrawler(PageBase):
    """
    get请求：获取api数据
    二级跳转：在详细页获取目的数据

    多次请求：获取api数据
    一级跳转：在目录页获取api数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': 'Fujian',
                     'province': 'Fujian',
                     'total_items_num': 1516 + 200,
                     'each_page_count': 10,
                     'base_url': 'https://data.fujian.gov.cn/datadevelop/prod-api/open-portal/catalog/list?pageSize=10&format=&openType=&type=2&key=&orderByColumn=&isAsc=&currentTab=city'
                     }

        api_city_info = {
            'name': 'Fujian',
            'province': 'Fujian',
            'total_items_num': 1516 + 200,
            'each_page_count': 10,
            'base_url': 'https://data.fujian.gov.cn/datadevelop/prod-api/open-portal/resourceService/list?pageSize=100&format=&type=2&key=&orderByColumn=&isAsc=&currentTab=city&serviceName=&approvalAuthority=',
            'is_api': 'True'
        }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {
                        "Cookie": "wza-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; oportal-x-access=98f9bf3c-ed1b-43af-b0e5-40fa22a15a92",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        }

        self.params = {'sort': '0', 'pageNo': '38', 'pageSize': '15'}

        self.city_code_map = {
            "福州市": "350100000000",
            "福建省": '000',
            "厦门市": "350200000000",
            "莆田市": "350300000000",
            "三明市": "350400000000",
            "泉州市": "350500000000",
            "漳州市": "350600000000",
            "南平市": "350700000000",
            "龙岩市": "350800000000",
            "宁德市": "350900000000",
            "平潭综合实验区": "350128000000"
        }

    def run(self):
        session = self.session

        for city_name, city_code in self.city_code_map.items():
            if city_code == '000':
                if self.is_api:
                    city_url = 'https://data.fujian.gov.cn/datadevelop/prod-api/open-portal/catalog/list?pageSize=10&format=&openType=&type=-1&key=&orderByColumn=&isAsc=&currentTab=all'
                    self.name = city_name + '_api'
                else:
                    city_url = 'https://data.fujian.gov.cn/datadevelop/prod-api/open-portal/catalog/list?pageSize=10&type=1&deptCode=&cityCode=&format=&openType=&key=&orderByColumn=&isAsc=&currentTab=all'
                    self.name = city_name
            else:
                if self.is_api:
                    city_url = f'{self.base_url}&regionCode={city_code}'
                    self.name = city_name + '_api'
                else:
                    city_url = f'{self.base_url}&cityCode={city_code}'
                    self.name = city_name

            # 获取总页数
            session.get(url=city_url, headers=self.headers, proxies=self.proxies)
            json_data = session.response.text if session.response.text else session.response.content
            self.total_items_num = json.loads(json_data).get('data', 0).get('total', 0)
            self.total_page_num = self.count_page_num()

            self.total_data = self.process_views(city_url)
            self.save_files()

    def process_views(self, city_url):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = f'{city_url}&pageNum={i}'
            session.get(url=url, headers=self.headers, proxies=self.proxies)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                page_data = self.extract_data(json_data) if not self.is_api \
                    else self.extract_api_page_data(json_data)
                views_list.extend(page_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, url_page):

        session_page = SessionPage()

        session_page.get(url=url_page, headers=self.headers, proxies=self.proxies)

        release_time, update_cycle = self.extract_page_data(session_page.response.text)

        return {'release_time': release_time, 'update_cycle': update_cycle}

    def extract_data(self, json_data):

        data = json.loads(json_data)
        results = data['data']['rows']
        models = []

        for item in results:
            title = item.get('catalogName', '')
            subject = item.get('themeName', '')  # Using 'Unknown' if themeName is not available
            description = item.get('catalogDes', '')
            source_department = item.get('orgName', '')

            update_time = item.get('updateTime')  # Using the same time for update if not specified otherwise

            open_conditions = '无条件开放' if item.get('openType') == '1' else '有条件开放'
            data_volume = item.get('dataVol', None)
            is_api = 'False'
            file_type = [file.get('fileFormat', '').upper() for file in (item.get('files') or [])]
            access_count = item.get('visits', None)
            download_count = item.get('downloads', None)
            api_call_count = None  # No specific field in JSON, assuming 0
            link = f"https://data.fujian.gov.cn/datadevelop/prod-api/open-portal/catalog/getCataInfo?cataId={item.get('catalogID')}"  # Example link construction

            if item.get('catalogID'):
                page_data = self.process_page(link)

                release_time = page_data['release_time']
                update_cycle = page_data['update_cycle']
            else:
                release_time = update_time
                update_cycle = ''

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_page_data(self, json_data):
        json_data = json.loads(json_data).get('data', {})

        release_time = json_data['releasedTime']

        update_cycle = json_data['updateCycle']

        return release_time, update_cycle

    def extract_api_page_data(self, json_data):
        data = json.loads(json_data)
        results = data['data']['rows']
        models = []

        for item in results:
            title = item.get('catalogName', '') if item.get('catalogName') is not None else item.get('serviceName', '')
            subject = item.get('themeName', '')
            description = item.get('catalogDes', '') if item.get('catalogDes') is not None else item.get('serviceDesc', '')
            source_department = item.get('orgName', '')

            release_time = item.get('releaseTime')
            update_time = item.get('updateTime', '')

            open_conditions = '无条件开放' if item.get('openType') == '1' else '有条件开放'
            data_volume = item.get('dataVol', None)
            is_api = 'True'
            file_type = ['接口']  # Assuming files are listed

            access_count = item.get('visits', None)
            download_count = item.get('downloads', None)
            api_call_count = item.get('invokeTimes', None)
            link = ""  # Example link construction
            update_cycle = ''  # Placeholder if not available

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
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
            0: '实时',
            2: '每日',
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
    page = FujianCrawler(is_headless=True)
    page.run()
