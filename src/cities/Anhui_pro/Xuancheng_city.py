import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class XuanchengCrawler(PageBase):
    """
    post 请求获取api数据
    直接在目录页获取目的数据
    """
    def __init__(self, is_headless=True):
        city_info = {'name': '宣城市',
                     'province': 'Anhui',
                     'total_items_num': 501 + 200,
                     'each_page_count': 10,
                     'base_url': 'http://60.173.74.11:5678/dataopen-web/data/findByPage'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cookie": "JSESSIONID=1F60BEEC781A349A69050BEE76CF5F9B; __jsluid_s=d4913291e8caeeb03cf8fe23aae051d8; __jsl_clearance_s=1717420453.517|0|mxVlEXxzZagDKM83NjnmW0q4voQ%3D; hefei_gova_SHIROJSESSIONID=edf17941-814a-4df9-9c10-1efac5268c7c; arialoadData=false",
            "Host": "www.hefei.gov.cn",
            "Referer": "https://www.hefei.gov.cn/open-data-web/data/list-hfs.do?pageIndex=2",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }

        self.params = {'pageSize': '10', 'pageNum': '2'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNum'] = str(i)
            response.post(url=self.base_url, headers=self.headers, data=self.params, proxies=self.proxies)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_page_data(json_data)
                views_list.extend(extracted_data)
                self.logger.info(f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def extract_page_data(self, json_data):
        # 假设json_data是JSON格式的字符串
        data = json_data
        results = data['data']['rows']  # 根据你的JSON数据结构调整路径
        models = []

        for item in results:
            title = item.get('catalogName', '')
            subject = item.get('belongFieldName', '')
            description = item.get('summary', '')
            source_department = item.get('providerDept', '')

            release_time = item.get('publishTime', '')
            update_time = item.get('updateTime', '')

            open_conditions = item.get('shareTypeName', '')
            data_volume = None  # 此信息可能需要从其他地方获取
            is_api = 'True' if 'API' in item.get('formats', []) else 'False'
            file_type = item.get('formats', [])

            access_count = item.get('llcs', None)
            download_count = item.get('xzcs', None)
            api_call_count = item.get('dycs', None)
            link = ''  # 链接未在JSON数据中提供
            update_cycle = '每年'

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
            1: '每日',
            2: '每月',
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
    page = XuanchengCrawler(is_headless=True)
    page.run()
