import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class WuhanCrawler(PageBase):
    """
    post：请求获取api数据
    直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '武汉市',
                     'province': 'Hubei',
                     'total_items_num': 1432 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://data.wuhan.gov.cn/api/portal/data/catalog/page'
                     }

        api_city_info = {'name': '武汉市_api',
                         'province': 'Hubei',
                         'total_items_num': 1027 + 500,
                         'each_page_count': 10,
                         'base_url': 'https://data.wuhan.gov.cn/api/portal/data/service/page-all',
                         'is_api': 'True'
                         }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json", "Host": "data.wuhan.gov.cn",
                        "Origin": "https://data.wuhan.gov.cn",
                        "Referer": "https://data.wuhan.gov.cn/page/data/data_directory.html", "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = '{"current":2,"size":10}'

        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['current'] = i
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=self.params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                page_data = self.extract_page_data(json_data)
                views_list.extend(page_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, json_data):

        data_dict = json.loads(json_data)
        session_page = SessionPage()

        for item in data_dict['smcDataSetList']:
            url = f'https://data.zhihuichuzhou.com:8007/#/wdfwDetail?id=116982227351{item["createUser"]["id"]}'

            session_page.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page)

    def extract_data(self, json_data):

        data = json_data

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        models = []

        for item in results['result']:
            # 提取和构建所需信息
            title = item.get('zy', '')
            subject = item.get('filedName', '')
            description = item.get('zymc', '')
            source_department = item.get('tgdwmc', '')
            release_time = datetime.strptime(item.get('zxtbsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('gxsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            open_conditions = ''  # 假设默认为'无'
            data_volume = 0  # 假设默认为0
            is_api = 'False'  # 假设默认为'否'
            file_type = [item.get('fjhzm', '')]
            access_count = item.get('fws', None)
            download_count = item.get('xzs', None)  # 假设默认为0
            api_call_count = 0  # 假设默认为0
            link = ''  # 未提供
            update_cycle = self.format_update_cycle(item.get('gxpl', ''))

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle)
            models.append(model.to_dict())

        return models

    def extract_page_data(self, json_data):
        data = json.loads(json_data)
        results = data['data']['records']  # 根据新的数据路径调整
        models = []

        for item in results:
            title = item.get('cataTitle', '')
            subject = item.get('dataDomainDetail', '')
            description = item.get('summary', '')
            source_department = item.get('registerOrgName', '')

            release_time = item.get('releaseTime', '')
            update_time = item.get('updateTime', '')

            file_formats = item.get('cataFormatDetail', '').split(',') if item.get('cataFormatDetail') else []
            file_type = [fmt.strip() for fmt in file_formats]

            access_count = item.get('pageView', None)
            download_count = item.get('downloads', None)
            api_call_count = item.get('applyNum', None)  # Assuming 'serviceCount' reflects API calls
            is_api = 'False' if not self.is_api else 'True'

            open_conditions = item.get('openTypeDetail', '未知')  # Defaulting to '未知' if not specified
            data_volume = item.get('fileCount', None)

            link = f'https://data.wuhan.gov.cn/page/data/data_interface_details.html?cataId={item["cataId"]}&serviceCode={item["serviceCode"]}'  # No URL provided in the data

            update_cycle = self.format_update_cycle(item.get('updateCycle', '未指定'))

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle)
            models.append(model.to_dict())

        return models

    @staticmethod
    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data)

        # 假设所有相关信息都在 'smcDataSetList' 键中
        results = json_data.get('result', [])
        models = []

        for item in results['data']:
            # 提取和构建所需信息
            title = item.get('name', '')
            subject = item.get('appTypeName', '')
            description = item.get('summary', '')
            source_department = item.get('companyName', '')

            release_time = datetime.strptime(item.get('lastUpdateTime', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('lastUpdateTime', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

            open_conditions = ''  # 假设默认为'无'
            data_volume = item.get('dataCount', '')  # 假设默认为0
            is_api = 'True'
            file_type = ['json']

            access_count = item.get('viewCount', None)
            download_count = item.get('downCount', None)
            api_call_count = None  # 未提供
            link = f'http://open.huaibeidata.cn:1123/#/interface/detail/{item["id"]}'  # 未提供
            update_cycle = item.get('updateCycle', '')

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle)
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
            1: '不定期',
            2: '实时',
            3: '每日',
            4: '每周',
            5: '每月',
            6: '每季度',
            7: '每半年',
            8: '每年'
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
    page = WuhanCrawler(is_headless=True)
    page.run()
