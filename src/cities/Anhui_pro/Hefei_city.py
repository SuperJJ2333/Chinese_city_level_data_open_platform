import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class HefeiCrawler(PageBase):
    """
    get请求获取api数据
    直接在目录页获取目的数据
    """
    def __init__(self, is_headless=True):
        city_info = {'name': '合肥市',
                     'province': 'Anhui',
                     'total_items_num': 326,
                     'each_page_count': 10,
                     'base_url': 'http://data.ahzwfw.gov.cn:8000/dataopen-web/data/findByPage'}

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

        self.city_type = [{'Maanshan': ['3405', 40]}, {'Tonglin': ['3407', 22]}]

    def run(self):
        for city_type in self.city_type:
            self.name = city_type.keys()
            self.page_num = city_type.values()

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        response = self.page


        for i in range(1, self.total_page_num):
            url = self.base_url.format(page_num=i)
            response.get(url=url)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_data(json_data)
                views_list.extend(extracted_data)
                self.logger.info(f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

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
            data_volume = None  # 假设默认为0
            is_api = 'False'  # 假设默认为'否'
            file_type = [item.get('fjhzm', '')]
            access_count = item.get('fws', None)
            download_count = item.get('xzs', None)  # 假设默认为0
            api_call_count = None  # 假设默认为0
            link = ''  # 未提供
            update_cycle = self.format_update_cycle(item.get('gxpl', ''))

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
    page = HefeiCrawler(is_headless=True)
    page.run()
