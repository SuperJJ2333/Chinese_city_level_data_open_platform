import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class WuhuCrawler(PageBase):
    """
    post 请求获取api数据
    一级跳转：目的数据在目录页
    """
    def __init__(self, is_headless=True):
        city_info = {'name': '芜湖市',
                     'province': 'Anhui',
                     'total_items_num': 375 + 200,
                     'each_page_count': 15,
                     'base_url': 'https://data.wuhu.cn/datagov-ops/data/getDataSetByWhere'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": "JSESSIONID=98A7E8F79E7F1FCA944CDE2118B8D7A2",
            "Host": "data.wuhu.cn",
            "Origin": "https://data.wuhu.cn",
            "Referer": "https://data.wuhu.cn/datagov-ops/data/dataIndex",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }

        self.params = {'sort': '0', 'pageNo': '38', 'pageSize': '15'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNo'] = str(i)
            url = self.base_url
            session.post(url=url, headers=self.headers, data=self.params, proxies=self.proxies)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = json_data.replace('\\', '')
                page_data = self.extract_page_data(extracted_data)
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

    def extract_page_data(self, json_data):
        json_data = json.loads(json_data.strip('"'))

        # 假设所有相关信息都在 'smcDataSetList' 键中
        results = json_data.get('smcDataSetList', [])
        models = []

        for item in results:
            # 提取和构建所需信息
            title = item.get('datasetName', '')
            subject = item.get('dataType', '')
            description = item.get('summary', '')
            source_department = item.get('isValid', '')
            release_time = datetime.strptime(item.get('createDate', ''), '%Y-%m-%d').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('updateDate', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            open_conditions = '公开' if item.get('isValid', '') == '1' else '有条件开放'  # 假设默认为'无'
            data_volume = None  # 假设默认为0
            is_api = 'False' if item.get('isValid', '') == '1' else 'True'  # 假设默认为'否'
            file_type = [file.get('fileType', '').replace('.', '') for file in item.get('fileList', [])]
            access_count = item.get('smcDataSetBrowseCount', {}).get('browseCount', None)
            download_count = item.get('dataSetDownloadCount', {}).get('downloadCount', None)
            api_call_count = item.get('dataSetDownloadCount', {}).get('apiInvokeCount', None)
            link = f'https://data.wuhu.cn/datagov-ops/data/toDetailPage?id={item["createUser"]["id"]}'  # 未提供
            update_cycle = self.format_update_cycle(item.get('dataUpdateFrequency', ''))

            # 创建DataModel实例
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
    page = WuhuCrawler(is_headless=True)
    page.run()
