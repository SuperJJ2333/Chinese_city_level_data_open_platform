import concurrent
import json
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class DeyangCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '德阳市',
                     'province': 'Sichuan',
                     'total_items_num': 1325 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://www.dysdsj.cn/exchangeopengateway/v1.0/mh/sjml/getMlxxList'
                     }
        api_city_info = {'name': '德阳市_api',
                         'province': 'Sichuan',
                         'total_items_num': 1805 + 1000,
                         'each_page_count': 10,
                         'base_url': 'https://www.dysdsj.cn/exchangeopengateway/v1.0/mh/sjml/getMlxxList',
                         'is_api': 'True',
                         }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"210","Content-Type":"application/json;charset=UTF-8","Cookie":"BFreeDialect=0; _appId=482188fc774b49b41c4b4e3b5aba81de","Host":"www.dysdsj.cn","Origin":"https://www.dysdsj.cn","Referer":"https://www.dysdsj.cn/","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","appCode":"482188fc774b49b41c4b4e3b5aba81de","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\"","token":"null"}

        if self.is_api:
            self.params = '{"pageNo":1,"pageSize":10,"ssbmId":["00"],"zylx":["03"],"mlmc":"","gxsjPx":2,"llslPx":"","sqslPx":"","pfPx":"","sjfwXzslPx":"","ssztlmId":["00"],"ssjclmId":["00"],"ssqtlmId":["00"],"kflx":["00"]}'
        else:
            self.params = '{"pageNo":2,"pageSize":10,"zylx":["01","02"],"ssbmId":["00"],"mlmc":"","gxsjPx":2,"llslPx":"","sqslPx":"","pfPx":"","xzslPx":"","ssztlmId":["00"],"ssjclmId":["00"],"ssqtlmId":["00"],"kflx":["00"],"wjlx":["00"]}'
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

            json_data = None
            num = 1

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    num += 1
                    time.sleep(1)
                    if num > 5:
                        self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')
                        break
                    elif num == 2:
                        session.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            if json_data:
                # 提取API数据
                urls_list = self.extract_data(json_data)
                # 处理API数据
                page_data = self.process_multi_page(urls_list)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')
            else:
                continue

        return views_list

    def process_page(self, urls_list):

        session_page = SessionPage()
        items_list = []

        for item in urls_list:
            if self.is_api:
                url = f'https://www.dysdsj.cn/exchangeopengateway/v1.0/mlxx/getZyfwxq?mlbh={item.get("rowGuid")}&appCode=2bc8f11e6aa29bd8f7b30bf5822c1adb'
                headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                           "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                           "Connection": "keep-alive",
                           "Cookie": "BFreeDialect=0; _appId=2bc8f11e6aa29bd8f7b30bf5822c1adb", "Host": "www.dysdsj.cn",
                           "Referer": "https://www.dysdsj.cn/", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                           "Sec-Fetch-Site": "same-origin",
                           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                           "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                           "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "token": "null"}
            else:
                url = f'https://www.dysdsj.cn/exchangeopengateway/v1.0/mlxx/getSjmlxq?mlbh={item.get("rowGuid")}&appCode=728272e00bb9ca19c7caa27c268aa5b6'

                headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                           "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                           "Connection": "keep-alive",
                           "Cookie": "BFreeDialect=0; _appId=728272e00bb9ca19c7caa27c268aa5b6", "Host": "www.dysdsj.cn",
                           "Referer": "https://www.dysdsj.cn/", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                           "Sec-Fetch-Site": "same-origin",
                           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                           "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                           "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "token": "null"}

            session_page.get(url=url, headers=headers, proxies=self.proxies)

            if self.is_api:
                item_data = self.extract_api_page_data(session_page.response.text, url)
            else:
                item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    def process_multi_page(self, urls_list):
        items_list = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(self.process_single_page, item): item for item in urls_list}

            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    item_data = future.result()
                    items_list.append(item_data)
                except Exception as e:
                    self.logger.error(f'处理单页数据失败 - {e}')

        return items_list

    def process_single_page(self, item):
        session_page = SessionPage()

        if self.is_api:
            url = f'https://www.dysdsj.cn/exchangeopengateway/v1.0/mlxx/getZyfwxq?mlbh={item.get("rowGuid")}&appCode=482188fc774b49b41c4b4e3b5aba81de'
            headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                       "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                       "Connection": "keep-alive",
                       "Cookie": "BFreeDialect=0; _appId=2bc8f11e6aa29bd8f7b30bf5822c1adb", "Host": "www.dysdsj.cn",
                       "Referer": "https://www.dysdsj.cn/", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                       "Sec-Fetch-Site": "same-origin",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                       "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                       "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "token": "null"}

        else:
            url = f'https://www.dysdsj.cn/exchangeopengateway/v1.0/mlxx/getSjmlxq?mlbh={item.get("rowGuid")}&appCode=482188fc774b49b41c4b4e3b5aba81de'

            headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                       "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Connection": "keep-alive",
                       "Cookie": "BFreeDialect=0; _appId=482188fc774b49b41c4b4e3b5aba81de", "Host": "www.dysdsj.cn",
                       "Referer": "https://www.dysdsj.cn/", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                       "Sec-Fetch-Site": "same-origin",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                       "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                       "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "token": "null"}

        session_page.get(url=url, headers=headers, proxies=self.proxies)

        if self.is_api:
            item_data = self.extract_api_page_data(session_page.response.text, url)
        else:
            item_data = self.extract_page_data(session_page.response.text, url)

        return item_data

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

        if not item:
            self.logger.warning(f'解析API数据失败 - {url}')
            return {}

        title = item.get('mlmc', '')
        description = item.get('xxzymc', '')
        subject = item.get('ssztlmmc', '')
        source_department = item.get('xxzytgf', '')

        # 将时间戳转换为可读的日期格式
        release_time = datetime.fromtimestamp(int(item.get('fbrq')) / 1000.0).strftime('%Y-%m-%d') if item.get(
            'fbrq') else None
        update_time = datetime.fromtimestamp(int(item.get('gxrq')) / 1000.0).strftime('%Y-%m-%d') if item.get(
            'gxrq') else None

        # 从提供的信息中获取开放类型和更新周期
        open_conditions = item.get('kftj', '')
        update_cycle = self.format_update_cycle(item.get('gxzq', ''))

        access_count = item.get('llsl', None)
        download_count = item.get('sqsl', None)

        is_api = 'False'
        file_types = [item.get('zygs', '')]
        data_volume = None
        api_call_count = item.get('sqsl', None)  # Assuming 0 since not provided
        link = url

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data, url):
        item = json.loads(json_data).get('data')  # 更新数据路径到单个数据对象

        if not item:
            self.logger.warning(f'解析API数据失败 - {url}')
            return {}

        title = item.get('mlmc', '')
        description = item.get('ztfl', '')
        subject = item.get('ssqtlmmc', '')
        source_department = item.get('xxzytgf', '')

        # 将时间戳转换为可读的日期格式
        release_time = item.get('fbsj', '')
        update_time = item.get('gxsj', '')

        # 从提供的信息中获取开放类型和更新周期
        open_conditions = item.get('gxsx', '')
        update_cycle = self.format_update_cycle(item.get('gxzq', ''))

        access_count = item.get('llcs', None)
        download_count = item.get('sqsl', None)

        is_api = 'True'
        file_types = ['接口']
        data_volume = None
        api_call_count = item.get('sqsl', None)  # Assuming 0 since not provided
        link = url

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

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
            2: '每周',
            3: '每月',
            4: '每季度',
            5: '每年',
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
    page = DeyangCrawler(is_headless=True)
    page.run()
