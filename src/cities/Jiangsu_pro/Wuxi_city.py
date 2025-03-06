import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class WuxiCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：xpath get请求 在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '无锡市',
                     'province': 'Jiangsu',
                     'total_items_num': 3550 + 1500,
                     'each_page_count': 10,
                     'base_url': 'https://data.wuxi.gov.cn/data/catalog/catalog.do?method=GetCatalog'
                     }

        api_city_info = {'name': '无锡市_api',
                         'province': 'Jiangsu',
                         'total_items_num': 4990 + 1500,
                         'each_page_count': 10,
                         'base_url': 'https://data.wuxi.gov.cn/data/dev/developer/serviceList.do?method=queryApiList&page={page_num}',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        if not self.is_api:
            self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                            "Accept-Encoding": "gzip, deflate, br, zstd",
                            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                            "Host": "data.wuxi.gov.cn", "Origin": "https://data.wuxi.gov.cn",
                            "Referer": "https://data.wuxi.gov.cn/data/catalog/index.htm", "Sec-Fetch-Dest": "empty",
                            "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                            "X-Requested-With": "XMLHttpRequest",
                            "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

            self.params = {'org_code': '', 'region_code': '', 'conf_use_type': '', 'group_id': '',
                           'conf_catalog_format': '', 'open_type': '', 'page': '4', 'pageSize': '10',
                           '_order': 'cc.data_update_time desc', 'keyword': ''}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            if self.is_api:
                url = self.base_url.format(page_num=i)
                session.get(url=url, headers=self.headers, proxies=self.proxies)
            else:
                self.params['page'] = str(i)
                params = self.params
                session.post(url=self.base_url, data=params, headers=self.headers,
                             proxies=self.fiddler_proxies, verify=False)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                if self.is_api:
                    page_data = self.extract_api_page_data(json_data)
                else:
                    # 处理API数据
                    page_data = self.extract_page_data(json_data, '')
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, urls_list):

        session_page = SessionPage()
        items_list = []

        for item in urls_list:
            url = 'https://kf.zjkzwfw.gov.cn/extranet/rest/dataOpen/getResourceDetail'
            params = {'token': None, 'usertype': None, 'params': {'rowGuid': item['rowGuid'], 'type': '1'}}
            params = self.encode_params(params)

            session_page.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            item_data = self.extract_page_data(session_page.response.text, item)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results:
            # 提取和构建所需信息
            rowGuid = item.get('cata_id', '')
            data_amount = item.get('openresdataCount', '')
            id_lists.append({
                'rowGuid': rowGuid,
                'data_amount': data_amount
            })

        return id_lists

    def extract_page_data(self, json_data, upper_item):
        results = json.loads(json_data).get('data', '')  # 更新数据路径到正确的列表
        models = []

        for item in results:
            title = item.get('cata_title', '')
            subject = item.get('cataLogGroups', {})[-1].get('group_name', '')  # 使用标签作为主题
            description = item.get('description', '')
            source_department = item.get('org_name', '')

            release_time = item.get('released_time', '')
            update_time = item.get('update_time', '')

            open_conditions = item.get('open_type', '无条件开放')  # 假设 open_type 字段直接描述开放条件
            data_volume = item.get('total_storage', None)  # 假设 total_storage 是数据量
            file_types = ['XLS', 'XML', 'JSON', 'CSV'] if item.get('resource_format_type', '') == '2' \
                                                          or item.get('resource_format_type', '') == '3' else ['API']
            is_api = 'True' if 'API' in file_types else 'False'

            access_count = item.get('conf_view_num', None)
            download_count = item.get('DOWNLOADS', None)
            api_call_count = None  # JSON中未提供API调用次数信息
            link = f'https://data.wuxi.gov.cn/data/catalog/catalogDetail.htm?cata_id={item.get("cata_id", "")}'  # 假设没有具体的链接信息

            update_cycle = self.format_update_cycle(item.get('conf_update_cycle', ''))

            model = DataModel(title, subject, description, source_department, release_time, update_time,
                              open_conditions, data_volume, is_api, file_types, access_count, download_count,
                              api_call_count, link, update_cycle, self.name)

            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data)
        results = json_data['data']  # 更新数据路径到正确的列表
        models = []

        for item in results:
            title = item.get('service_name', '')
            subject = item.get('service_theme', '')
            description = item.get('service_desc', '')
            source_department = item.get('org_name', '')

            release_time = item.get('online_time', '')
            update_time = item.get('create_time', '')

            # 检查是否开放和需要用户授权，来决定开放条件
            open_conditions = "无条件开放" if item.get('need_user_authorize', 1) == 0 else "需要用户授权"
            data_volume = item.get('total_visits_count', None)  # 访问次数作为数据量的一种指标
            file_type = ["API"]  # 由于数据服务类型，假设文件类型为API
            is_api = 'True'

            access_count = item.get('total_visits_count', None)
            download_count = item.get('total_apply_count', None)
            api_call_count = None  # 使用访问次数作为API调用次数

            link = f'https://data.wuxi.gov.cn/data/dev/developer/serviceDetail.htm?service_id={item.get("service_id", "")}'  # 使用服务的上下文路径作为链接
            update_cycle = ''

            model = DataModel(title, subject, description, source_department, release_time, update_time,
                              open_conditions, data_volume, is_api, file_type, access_count, download_count,
                              api_call_count, link, update_cycle, self.name)
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
            1: '不定时',
            2: '每日',
            3: '每周',
            4: '每季度',
            5: '每半年',
            6: '每年',
            7: '每年',
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

    @staticmethod
    def encode_params(params):
        """
        将请求参数编码为URL格式。

        参数:
        params (dict): 请求参数字典。

        返回:
        str: 编码后的URL参数。
        """
        # 将 JSON 字典转换为 URL 编码的表单数据

        encoded_data = json.dumps(params)

        # URL 编码
        encoded_data = urllib.parse.quote(encoded_data)

        return encoded_data


if __name__ == '__main__':
    page = WuxiCrawler(is_headless=True)
    page.run()
