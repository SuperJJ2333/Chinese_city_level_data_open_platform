import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class HuangshanCrawler(PageBase):
    """
        get请求获取api数据
        直接在目录页获取目的数据

        但是目录页有多个类型，需要遍历每个类型，获取数据
    """
    def __init__(self, is_headless=True):
        self.types_dict = {
            '金融服务': [18, 12],
            '医疗卫生': [20, 19],
            '文化娱乐': [28, 9],
            '就业服务': [30, 12],
            '能源环境': [32, 27],
            '交通服务': [33, 2],
            '公共服务': [37, 52],
            '农村农业': [38, 4],
            '法律服务': [39, 31],
            '教育科技': [40, 11],
            '经济发展': [41, 14],
            '公共安全': [46, 9],
        }

        city_info = {'name': '黄山市',
                     'province': 'Anhui',
                     'total_items_num': 380 + 100,
                     'each_page_count': 8,
                     'base_url': 'https://www.huangshan.gov.cn/site/label/8888?labelName=dataOpenResourceCatalogList&isPage=1&pageSize=8&providerOrgan=&dateFormat=yyyy-MM-dd&length=34&orderBy=&sortOrder=asc&containerId=resourceCatsDiv',
                     'is_api': 'True',
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Cookie": "wzws_sessionid=gjVkMTY3ZoFlOTVmYjigZmBWDoAyMjEuNC4zMi4yNA==; SECKEY_ABVK=j0rtMU0eON3WKE/UhEeZYHnT8yHqRO3RzGaAuVm1Tcw%3D; BMAP_SECKEY=Z1ROGlRy-zdYROkyYNTEasFrFUn8Jkm2PI7HodtXDElyZZOBkRwQqGOwI8wCkJXpn2MJrpzUbve6Cbev7TFnTfgZ_nXvHhd77uyBHmCpWvMBr54aWjNe0CuGbA_26J5fU12uhuBvj0WPk0gosgwbZy-V_dfYz3_5_4aR_2t5InOBk0zpN55LZ6GFWlHPNHFn",
                        "Host": "open.huaibeidata.cn:1123", "Origin": "http://open.huaibeidata.cn:1123",
                        "Proxy-Connection": "keep-alive", "Referer": "http://open.huaibeidata.cn:1123/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"}

        self.params = {'sort': '0', 'pageNo': '38', 'pageSize': '15'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for type_name, type_info in self.types_dict.items():
            type_id = type_info[0]
            total_count = type_info[1]
            type_url = self.base_url + f'&dataArea={type_id}'
            total_page_num = total_count // self.each_page_num + 1

            for i in range(1, total_page_num + 1):
                url = type_url + f'&pageIndex={i}'
                session.get(url=url, proxies=self.proxies)

                while True:
                    try:
                        json_data = session.response.text if session.response.text else session.response.content
                        break
                    except Exception as e:
                        time.sleep(1)
                        self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

                if json_data:
                    page_data = self.extract_page_data(session)
                    views_list.extend(page_data)
                    self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, json_data):

        data_dict = json.loads(json_data)
        session_page = SessionPage()

        for item in data_dict['smcDataSetList']:
            url = f'https://data.wuhu.cn/datagov-ops/data/toDetailPage?id={item["createUser"]["id"]}'

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

    def extract_page_data(self, session_page):

        models_list = []
        frames_list = session_page.eles('x://ul/li')

        for frame in frames_list:
            title = frame.ele('x://div/div/a').text
            subject = frame.ele('x://div/div/p[2]/span[3]').text.split('：')[-1].strip()
            description = ''
            source_department = frame.ele('x://div/div/p[2]/span[2]').text.split('：')[-1].strip()

            release_time = frame.ele('x://div/div/p[2]/span[1]').text.split('：')[-1].strip()
            update_time = frame.ele('x://div/div/p[2]/span[1]').text.split('：')[-1].strip()

            open_conditions = ''
            data_volume = None  # 假设默认为0
            file_type = [file_type for file_type in frame.eles('x://div/div/p[1]/span[2]/a')]
            is_api = 'True' if 'JSON' in file_type else 'False'

            access_count = frame.ele('x://div/div/p[2]/span[5]').text.split('：')[-1].strip()
            download_count = frame.ele('x://div/div/p[2]/span[6]').text.split('：')[-1].strip()
            api_call_count = None
            link = ''

            update_cycle = frame.ele('x://div/div/p[2]/span[4]').text.split('：')[-1].strip()

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models_list.append(model.to_dict())

        return models_list

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
    page = HuangshanCrawler(is_headless=True)
    page.run()
