import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YichangCrawler(PageBase):
    """
    一级请求：post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '宜昌市',
                     'province': 'Hubei',
                     'total_items_num': 522 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://data.yichang.gov.cn/athena/catalog/page'
                     }

        api_city_info = {'name': '宜昌市_api',
                         'province': 'Hubei',
                         'total_items_num': 125,
                         'each_page_count': 10,
                         'base_url': 'https://data.yichang.gov.cn/athena/interface/getInterfaceDataByPage',
                         'is_api': 'True'
                         }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {
            "Cookie": "Hm_lvt_4ae480935d61ee0ea0a491a5215a50f2=1718200870; Hm_lpvt_4ae480935d61ee0ea0a491a5215a50f2=1718200870",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0", }

        self.params = '{"pageNum":2,"pageSize":10,"deptId":"","domainId":"","orderField":"2","isDownload":"","uncode":"","scene":"","searchString":"","content":"","orderDefault":"","isCatalog":"","searchType":"","openType":""}'
        self.params = json.loads(self.params)

    def run(self):
        if self.is_api:
            self.total_data = self.process_api_page()
        else:
            self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num - 1):
            self.params['pageNum'] = i
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
            url = f'https://data.yichang.gov.cn/athena/catalog/getDetails?dataId={item.get("rowGuid")}'

            session_page.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    def process_api_page(self):

        views_list = []
        session = self.session

        self.params = {'orderField': '2', 'pageNum': '1', 'pageSize': '10'}

        self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                        "Cookie": "JSESSIONID=49e376c3-6ca6-41ae-bace-87fefc1cefe4; Hm_lvt_4ae480935d61ee0ea0a491a5215a50f2=1718200870; Hm_lpvt_4ae480935d61ee0ea0a491a5215a50f2=1718266095",
                        "Host": "data.yichang.gov.cn", "If-Modified-Since": "0",
                        "Origin": "https://data.yichang.gov.cn",
                        "Referer": "https://data.yichang.gov.cn/kf/open/interface/interfaceList",
                        "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        for i in range(1, self.total_page_num):
            self.params['pageNum'] = i
            params = self.params

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, data=params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                # 处理API数据
                page_data = self.extract_api_page_data(json_data)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results['rows']:
            # 提取和构建所需信息
            rowGuid = item.get('iid', '')
            id_lists.append({
                'rowGuid': rowGuid,
            })

        return id_lists

    def extract_page_data(self, json_data, url):
        data = json.loads(json_data)  # 假设 json_data 是 JSON 格式的字符串
        results = data['data']  # 根据新的数据路径调整

        item = results

        title = item.get('title', '')
        subject = item.get('domainStr', '')
        description = item.get('content', '')
        source_department = item.get('deptName', '')

        release_time = datetime.strptime(item.get('createDate', ''), '%Y-%m-%d').strftime('%Y-%m-%d') \
            if item.get('createDate') else ''
        update_time = datetime.strptime(item.get('dataUpdateDate', ''), '%Y-%m-%d').strftime('%Y-%m-%d') \
            if item.get('dataUpdateDate') else ''

        open_conditions = item.get('openCondition', '') if item.get('openCondition') else ''
        data_volume = item.get('dataSize', None)
        is_api = 'True' if item.get('interfaceId', '0') is not None else 'False'
        file_types = [item.get('downloadDataInfo', []).get('dataType')] if item.get('downloadDataInfo') else []

        access_count = item.get('visitCount', None)
        download_count = item.get('useCount', None)
        api_call_count = item.get('invokes', None)
        link = url  # 假设没有具体的链接信息

        update_cycle = self.format_update_cycle(item.get('updateFreq', ''))

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)
        models = model.to_dict()

        return models

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data)
        data = json_data['data']['list']  # 直接访问列表
        models = []

        for item in data:
            title = item.get('title', '')
            subject = item.get('domainStr', '')  # 主题不在数据中，留空
            description = item.get('content', '')
            source_department = item.get('source', '').strip()

            release_time = item.get('createDate', '')
            update_time = item.get('dataUpdateDate', '') if item.get('dataUpdateDate') else item.get('modifyDate', '')

            open_conditions = ''  # 开放条件未提供
            data_volume = item.get('dataSize', None)  # 假设如果没有提供，则为0
            is_api = 'True'  # API调用不明确，假设为False
            file_type = ['接口']  # 文件类型不明确，留空数组

            access_count = item.get('visitCount', None)
            download_count = item.get('useCount', None)
            api_call_count = item.get('invokes', None)
            link = f'https://data.yichang.gov.cn/kf/open/interface/detail/{item.get("iid")}'  # 链接未提供
            update_cycle = self.format_update_cycle(item.get('updateFreq', ''))  # 更新周期未提供

            model = DataModel(title, subject, description, source_department, release_time, update_time,
                              open_conditions, data_volume, is_api, file_type, access_count, download_count,
                              api_call_count, link, update_cycle, self.name.split('_')[0])
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
            0: '每日',
            1: '每周',
            2: '每月',
            3: '每季度',
            4: '每半年',
            5: '每年',
            6: '不定期',
            7: '不更新'
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
    page = YichangCrawler(is_headless=True)
    page.run()
