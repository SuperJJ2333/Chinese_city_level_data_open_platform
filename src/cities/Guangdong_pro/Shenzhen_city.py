import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ShenzhenCrawler(PageBase):
    """
    post 请求获取api数据
    一级跳转：直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '深圳市',
                     'province': 'Guangdong',
                     'total_items_num': 3985 + 500,
                     'each_page_count': 20,
                     'base_url': 'https://opendata.sz.gov.cn/data/dataSet/findPage'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie": "JSESSIONID=38f37ac9-6e1f-47c3-9db7-033b6285d169", "Host": "opendata.sz.gov.cn",
                        "Origin": "https://opendata.sz.gov.cn",
                        "Referer": "https://opendata.sz.gov.cn/data/dataSet/toDataSet", "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'pageNo': '2', 'pageSize': '20', 'sortType': 'DESC'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNo'] = str(i)
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
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def extract_page_data(self, json_data):
        results = json.loads(json_data['dataList'])
        models = []

        for item in results['list']:
            location = self.name
            title = item.get('resTitle', '')
            subject = item.get('domainName', '')
            description = item.get('resTitle', '')  # 假设资源标题作为描述
            source_department = item.get('officeName', '')

            # 发布时间和更新时间
            release_time = datetime.strptime(item.get('publishDate', ''), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d') \
                if item.get('publishDate') else None
            update_time = datetime.strptime(item.get('updateDate', ''), '%Y-%m-%d %H:%M').strftime('%Y-%m-%d') \
                if item.get('updateDate') else None

            open_conditions = item.get('openLevelName', '')
            data_volume = item.get('recordTotal', None)
            is_api = 'True' if int(item.get('invokedCount', 0)) > 0 else 'False'
            file_type = item.get('suffixes', '').split(',') if item.get('suffixes') else []

            access_count = item.get('visits', None)
            download_count = item.get('downloads', None)
            api_call_count = item.get('invokedCount', None)
            link = f"https://opendata.sz.gov.cn/data/dataSet/toDataDetails/{item.get('resId').replace('/', '_')}"  # 示例链接，实际情况可能需要根据具体情况调整
            update_cycle = item.get('updateCycle', '')

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, location)
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
            1: '不定期',
            2: '每天',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
            8: '实时'
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
    page = ShenzhenCrawler(is_headless=True)
    page.run()
