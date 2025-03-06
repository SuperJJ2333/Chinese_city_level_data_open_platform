import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuizhouCrawler(PageBase):
    """
    post 请求获取api数据
    二级跳转：post：在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': 'Guizhou',
                     'province': 'Guizhou',
                     'total_items_num': 3985 + 500,
                     'each_page_count': 12,
                     'base_url': 'https://data.guizhou.gov.cn/api/search/data/getDataListByParams'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json;charset=UTF-8", "Host": "data.guizhou.gov.cn",
                        "Origin": "https://data.guizhou.gov.cn", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        }

        self.params = '{"orderBy":0,"topicId":[],"orgId":[],"divisionId":"520100","pageSize":12,"scenarioId":"","resourceType":"","resourceFormats":[],"openAttribute":"","searchText":[],"pageIndex":1,"thematicId":""}'

        self.params = json.loads(self.params)

        self.city_code = {
            "贵阳市": 520100,
            "六盘水市": 520200,
            "遵义市": 520300,
            "安顺市": 520400,
            "毕节市": 520500,
            "铜仁市": 520600
        }

    def run(self):
        session = self.session
        for city_name, city_code in self.city_code.items():
            self.name = city_name
            self.params['divisionId'] = str(city_code)

            # 发送post请求
            session.post(url=self.base_url, headers=self.headers, json=self.params, proxies=self.proxies)
            # 计算总页数
            data = session.response.text
            json_data = json.loads(data)
            self.total_items_num = json_data['total']
            self.total_page_num = self.count_page_num()

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['pageIndex'] = i
            response.post(url=self.base_url, headers=self.headers, json=self.params, proxies=self.proxies)

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
                    f'{self.name} - 第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, resId):
        """
        1. 发送post请求，获取api数据
        2. 解析api数据，获取开放条件和数据量
        :param resId:
        :return:
        """

        url = 'https://data.guizhou.gov.cn/api/search/data/getDataDetailByDataId'

        params = '{"id":"88a912d8-bd97-4900-aee2-87929ba23a58"}'

        params = json.loads(params)
        params['id'] = resId
        session = SessionPage()

        session.post(url=url, headers=self.headers, json=params, proxies=self.proxies)

        response = session.json

        json_data = response

        item = json_data['data']

        data_volume = item.get('dataCapacity', None)
        update_cycle = item.get('frequency', '')

        return {'data_volume': data_volume, 'update_cycle': update_cycle}

    def extract_page_data(self, json_data):
        data = json_data
        results = data['data'] # 根据新的数据路径调整
        models = []

        for item in results:
            title = item.get('name', '')
            subject = item.get('topicName', '')
            description = item.get('description', '')
            source_department = item.get('orgName', '')

            release_time = time.strftime('%Y-%m-%d', time.localtime(item.get('createTime') / 1000)) if item.get(
                'createTime') else ''
            update_time = time.strftime('%Y-%m-%d', time.localtime(item.get('updateTime') / 1000)) if item.get(
                'updateTime') else ''

            open_conditions = '有条件开放' if item.get('openAttribute', '') == 1 else "无条件开放",
            # data_volume = item.get('dataCapacity', '')
            is_api = 'True' if 'json' in (item.get('resourceFormats') or []) else 'False'
            file_type = item.get('resourceFormats', [])

            access_count = item.get('views', None)
            download_count = item.get('calls', None)
            api_call_count = item.get('calls', None)
            link = f'https://data.guizhou.gov.cn/guiyang/open-data/{item.get("id")}'  # 假设没有具体的链接信息
            # update_cycle = self.format_update_cycle(item.get('frequency', ''))
            location = self.name

            # 发送post请求，获取api数据
            detailed_data = self.process_page(item.get('id'))

            update_cycle = self.format_update_cycle(detailed_data['update_cycle'])
            data_volume = detailed_data['data_volume']

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
            9: "归档不更新",
            8: "不定期更新",
            7: "其他",
            6: "每半年",
            5: "实时",
            4: "每天",
            3: "每周",
            2: "每月",
            1: "每季度",
            0: "每年"
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
    page = GuizhouCrawler(is_headless=True)
    page.run()
