import concurrent
import json
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from common.form_utils import convert_timestamp_to_date
from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class SuiningCrawler(PageBase):
    """
    post：请求获取api数据
    直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '遂宁市',
                     'province': 'Sichuan',
                     'total_items_num': 9052 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://www.suining.gov.cn/exchangeopengateway/v1.0/mh/sjml/getMlxxList'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"198","Content-Type":"application/json;charset=UTF-8","Cookie":"__jsluid_s=5ec85c44241a179cfbe3cc750d1c8e77; Hm_lvt_04894bddc914b5373a794a477b8a29c5=1719130939,1720160432; _appId=66da18329738687beda378c9b35e41b3","Host":"www.suining.gov.cn","Origin":"https://www.suining.gov.cn","Referer":"https://www.suining.gov.cn/data","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","appCode":"66da18329738687beda378c9b35e41b3","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\"","token":"undefined"}

        self.params = '{"pageNo":2,"pageSize":10,"zylx":["01","02"],"ssbmId":["00"],"mlmc":"","gxsjPx":2,"llslPx":"","sqslPx":"","pfPx":"","ssztlmId":["00"],"ssjclmId":["00"],"ssqtlmId":["00"],"kflx":["00"],"wjlx":["00"]}'

        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_multi_view()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['pageNo'] = str(i)
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

    def process_multi_view(self):
        items_list = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(self.process_single_view, i): i for i in range(1, self.total_page_num)}

            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    item_data = future.result()
                    items_list.extend(item_data)
                    self.logger.info(f'第{future_to_item[future]}页 - 已获取数据{len(items_list)}/{self.total_items_num}条')
                except Exception as e:
                    self.logger.error(f'处理单页数据失败 - {e}')

        return items_list

    def process_single_view(self, page_num):

        session = self.session

        url = self.base_url
        self.params['pageNo'] = str(page_num)
        session.post(url=url, headers=self.headers, proxies=self.proxies, json=self.params)

        while True:
            try:
                json_data = session.response.text if session.response.text else session.response.content
                break
            except Exception as e:
                time.sleep(1)
                self.logger.warning(f'第{page_num}页 - 解析JSON数据失败，正在重试 - {e}')

        if json_data:
            page_data = self.extract_page_data(json_data)

            return page_data

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
        if data is None:
            return []
        results = data['data']['rows']  # 调整数据路径以适配新的JSON结构
        models = []

        for item in results:
            # 直接从条目中提取所需数据，对于可能不存在的数据提供默认空值处理
            title = item.get('mlmc', '')
            subject = item.get('ssbmmc', '')
            description = item.get('mlms', '')
            source_department = item.get('ssbmmc', '')

            release_time = convert_timestamp_to_date(item.get('fbsj', ''))
            update_time = convert_timestamp_to_date(item.get('xgsj', ''))
            update_cycle = self.format_update_cycle(item.get('gxzq', ''))

            open_conditions = "有条件开放" if item.get('gxlx', '') == "01" else "无条件开放"
            data_volume = item.get('jghxxjls', None)

            mapping = {
                "01": "库表",
                "02": "文件",
                "03": "接口",
                "04": "应用"
            }
            gxzq_value = item.get('gxzq', '')

            file_type = [mapping.get(gxzq_value, '')] if gxzq_value in mapping else []

            is_api = 'True' if '接口' in file_type else 'False'

            access_count = item.get('kfptLlsl', None)
            download_count = item.get('wjxxsl', None)
            api_call_count = item.get('kfptSqsl', None)

            link = f'https://www.suining.gov.cn/data#/DataSet/{item["mlbh"]}'

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
            1: '每日',
            2: '每周',
            3: '每月',
            4: '每季度',
            6: '每半年',
            5: '每年',
            6: '自定义',
            8: '实时',
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
    page = SuiningCrawler(is_headless=True)
    page.run()
