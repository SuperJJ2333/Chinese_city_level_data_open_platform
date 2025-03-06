import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YinchuanCrawler(PageBase):
    """
    post：请求获取api数据
    直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '银川市',
                     'province': 'Ningxia',
                     'total_items_num': 1005 + 1000,
                     'each_page_count': 6,
                     'base_url': 'http://data.yinchuan.gov.cn/odweb/catalog/catalog.do?method=GetCatalog'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/javascript, */*; q=0.01","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"104","Content-Type":"application/x-www-form-urlencoded; charset=UTF-8","Cookie":"JSESSIONID=0DA1A11FAA9839C44A6E1C2F62C48AF6; sub_domain_cokie=vddoduFNzoRbwW7ZeAgVnw%3D%3D; region_name=%E9%93%B6%E5%B7%9D%E5%B8%82; _gscu_2130434076=18996953wqyzwc12; _gscbrs_2130434076=1; _gscs_2130434076=18996953flvjmz12|pv:5","Host":"data.yinchuan.gov.cn","Origin":"http://data.yinchuan.gov.cn","Referer":"http://data.yinchuan.gov.cn/odweb/catalog/index.htm","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest"}
        self.params = {'start': '6', 'length': '6', 'pageLength': '6', '_order': '1:b'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            url = self.base_url
            self.params['start'] = str(i * 6)
            session.post(url=url, headers=self.headers, proxies=self.proxies, data=self.params)

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
            access_count = item.get('fws', 0)
            download_count = item.get('xzs', 0)  # 假设默认为0
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
        data = data['data']  # 调整数据路径以符合新的数据格式
        models = []

        for item in data:
            title = item.get('cata_title', '')
            subject = ", ".join([group['group_name'] for group in item.get('cataLogGroups', [])])
            description = item.get('description', '')
            source_department = item.get('org_name', '')

            # 处理时间戳转换为可读格式
            release_time = datetime.fromtimestamp(item['conf_released_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
                if item.get('conf_released_time') else None
            update_time = datetime.fromtimestamp(item['conf_update_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
                if item.get('conf_update_time') else None

            open_conditions = item.get('open_type', '')
            data_volume = item.get('catalogStatistic', {}).get('data_count', None)
            is_api = 'True' if item.get('catalogStatistic', {}).get('api_count', 0) > 0 else 'False'
            file_type = []
            access_count = item.get('catalogStatistic', {}).get('use_visit', None)
            download_count = item.get('catalogStatistic', {}).get('use_file_count', None)
            api_call_count = item.get('catalogStatistic', {}).get('api_count', None)
            link = ''  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('conf_update_cycle', ''))

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
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

            access_count = item.get('viewCount', 0)
            download_count = item.get('downCount', 0)
            api_call_count = 0
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
            0: '实时',
            2: '每日',
            3: '每年',
            4: '每季度',
            5: '每半年',
            7: '每年',
            6: '自定义',
            8: '实时'
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
    page = YinchuanCrawler(is_headless=True)
    page.run()
