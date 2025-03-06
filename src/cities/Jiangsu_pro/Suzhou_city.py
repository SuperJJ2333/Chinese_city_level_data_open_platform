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
        city_info = {'name': '苏州市',
                     'province': 'Jiangsu',
                     'total_items_num': 5638 + 1000,
                     'each_page_count': 20,
                     'base_url': 'https://data.suzhou.gov.cn/dii/catalog/cms/open/getResourcePage?size=20&current={page_num}'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json",
                        "Cookie": "wzws_sessionid=gmEzMzFmZaBmcWtsgTVmNDU5MYAyMjEuNC4zMi4yNA==; arialoadData=false",
                        "Host": "data.suzhou.gov.cn", "Origin": "https://data.suzhou.gov.cn",
                        "Referer": "https://data.suzhou.gov.cn/", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = '{"resourceCode":"","openAttribute":"","resourceName":"","order":"","sortColumn":"","deptId":"","labelIds":[],"subjectTypeId":"","industryTypeId":"","deptTypeId":"","mountType":"","current":2,"size":20}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['current'] = str(i)
            url = self.base_url.format(page_num=i)
            params = self.params

            session.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
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
            url = f'http://data.gxj.xz.gov.cn/exchangeopengateway/v1.0/mlxx/getSjmlxq?mlbh={item.get("rowGuid")}&appCode=e761216543826b072e9dfc995d2bd57d'

            headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
                       "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Connection": "keep-alive",
                       "Cookie": "_gscu_1870995672=18615311zmlgct73; _gscbrs_1870995672=1; _appId=e761216543826b072e9dfc995d2bd57d; _gscs_1870995672=186352672yt85q16|pv:5",
                       "Host": "data.gxj.xz.gov.cn", "Referer": "http://data.gxj.xz.gov.cn/",
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
        item = json.loads(json_data)  # 更新数据路径到单个数据对象

        data = item['data']['records']  # 根据新的数据路径调整
        models = []

        for item in data:
            title = item.get('resourceName', '')
            subject = item.get('resourceType', '')
            description = item.get('resourceIntroduction', '')
            source_department = item.get('createrDeptName', '')

            update_time = item.get('dataUpdateTime', '')
            release_time = item.get('createTime', '')

            open_conditions = "无条件开放" if item.get('openAttribute', '1') == '1' else "有条件开放"

            update_cycle = self.format_update_cycle(item.get('updateFrequency', ''))
            access_count = item.get('viewCount', None)
            download_count = item.get('downloadCount', None)
            api_call_count = item.get('applyCount', None)

            data_volume = item.get('dataVolume', None)

            file_types = [item.get('resourceType', '')]

            is_api = 'True' if api_call_count != 0 else 'False'

            link = f'https://data.suzhou.gov.cn/#/catalog/{item.get("id", "")}'

            model = DataModel(title, subject, description, source_department, release_time, update_time,
                              open_conditions, data_volume, is_api, file_types, access_count, download_count,
                              api_call_count, link, update_cycle)

            models.append(model.to_dict())

        return models

    @staticmethod
    def extract_api_page_data(json_data):
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
                              api_call_count, link, update_cycle)
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
            1: '实时',
            2: '每日',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
            8: '不定时',
            9: '其它',
            10: '每半月',

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
