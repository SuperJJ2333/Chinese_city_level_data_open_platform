import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class XuzhouCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '徐州市',
                     'province': 'Jiangsu',
                     'total_items_num': 2930 + 1500,
                     'each_page_count': 10,
                     'base_url': 'http://data.gxj.xz.gov.cn/exchangeopengateway/v1.0/mh/sjml/getMlxxList'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json;charset=UTF-8",
                        "Cookie": "_gscu_1870995672=18615311zmlgct73; _gscbrs_1870995672=1; _gscs_1870995672=20377879cf84fp73|pv:1; _appId=ab5c1a8d3779f537ada364eb6a9faa1a",
                        "Host": "data.gxj.xz.gov.cn", "Origin": "http://data.gxj.xz.gov.cn",
                        "Proxy-Connection": "keep-alive", "Referer": "http://data.gxj.xz.gov.cn/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "appCode": "ab5c1a8d3779f537ada364eb6a9faa1a", "token": "undefined"}

        self.params = '{"pageNo":2,"pageSize":10,"zylx":["01","02","03"],"ssbmId":["00"],"mlmc":"","gxsjPx":2,"llslPx":"","sqslPx":"","ssztlmId":["00"],"ssjclmId":["00"],"ssqtlmId":["00"],"kflx":["00"]}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNo'] = str(i)
            params = self.params

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                # 提取API数据
                urls_list = self.extract_data(json_data)
                # 处理API数据
                page_data = self.process_page(urls_list)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, urls_list):

        session_page = SessionPage()
        items_list = []

        for item in urls_list:
            url = f'http://data.gxj.xz.gov.cn/exchangeopengateway/v1.0/mlxx/getSjmlxq?mlbh={item.get("rowGuid")}&appCode=e761216543826b072e9dfc995d2bd57d'

            headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
                       "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                       "Cookie": "_gscu_1870995672=18615311zmlgct73; _gscbrs_1870995672=1; _appId=ab5c1a8d3779f537ada364eb6a9faa1a; _gscs_1870995672=20377879cf84fp73|pv:3",
                       "Host": "data.gxj.xz.gov.cn", "Proxy-Connection": "keep-alive",
                       "Referer": "http://data.gxj.xz.gov.cn/",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                       "token": "undefined"}

            session_page.get(url=url, headers=headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results['rows']:
            # 提取和构建所需信息
            rowGuid = item.get('mlbh', '').replace('/', '%2F')
            id_lists.append({
                'rowGuid': rowGuid,
            })

        return id_lists

    def extract_page_data(self, json_data, url):
        item = json.loads(json_data).get('data')  # 更新数据路径到单个数据对象

        title = item.get('mlmc', '')
        description = item.get('xxzymc', '')
        subject = item.get('ssqtlmmc', '')
        source_department = item.get('xxzytgf', '')

        # 将时间戳转换为可读的日期格式
        release_time = datetime.fromtimestamp(int(item.get('fbrq')) / 1000.0).strftime('%Y-%m-%d') if item.get(
            'fbrq') else None
        update_time = datetime.fromtimestamp(int(item.get('gxrq')) / 1000.0).strftime('%Y-%m-%d') if item.get(
            'gxrq') else None

        # 从提供的信息中获取开放类型和更新周期
        open_conditions = self.format_update_cycle(item.get('kftj', ''))
        update_cycle = item.get('gxzq', '')

        access_count = item.get('llsl', None)
        download_count = item.get('sqsl', None)

        is_api = 'False'
        file_types = []
        data_volume = None
        api_call_count = None
        link = url

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data.strip('"'))
        results = json_data['data']
        models = []

        for item in results['result']:
            title = item.get('zymc', '')
            subject = item.get('filedName', '')
            description = item.get('zy', '')
            source_department = item.get('tgdwmc', '')

            release_time = datetime.strptime(item.get('cjsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('gxsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')

            open_conditions = ''
            data_volume = 0
            is_api = 'True'
            file_type = [item.get('fjhzm', '')]

            access_count = item.get('fws', 0)
            download_count = 0
            api_call_count = item.get('xzs', 0)  # Assuming 0 since not provided
            link = ''
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
            7: '实时',
            1: '每日',
            2: '每周',
            3: '每月',
            4: '每季度',
            6: '每半年',
            5: '每年',
            0: '自定义',
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
    page = XuzhouCrawler(is_headless=True)
    page.run()
