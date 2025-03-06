import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZigongCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '自贡市',
                     'province': 'Sichuan',
                     'total_items_num': 8849,
                     'each_page_count': 10,
                     'base_url': 'https://data.zg.cn//catalog_schema/findRightSchema?offset={page_num}&limit=10&groupCode=&orderBy=0&catalogTitle=&dateType=&catalogFormatType=&catalogShareWay=&syncStatus=&shareType=&openType='}

        api_city_info = {'name': '自贡市_api',
                         'province': 'Jiangxi',
                         'total_items_num': 1093 + 100,
                         'each_page_count': 10,
                         'base_url': 'https://data.zg.cn//catalog_schema/findApiSchema?offset={page_num}&limit=10&orderBy=0&catalogTitle=&dateType=&catalogFormatType=&catalogShareWay=&syncStatus=&shareType=&openType=',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Connection": "keep-alive", "Content-Type": "application/json", "Host": "data.zg.cn",
                        "Referer": "https://data.zg.cn/snww/sjzy/index.html", "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}
        if self.is_api:
            self.params = '{"catalogTitle":"","categoryDetailId":"531","pageNum":1,"pageSize":10,"orderName":"","orderSort":"","dictRefList":[{"value":"1","dictRef":"openLimit"},{"value":"API","dictRef":"physicalResourceType"}]}'
        else:
            self.params = {'offset': '10', 'limit': '10', 'orderBy': '0'}
        # self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num - 1):
            url = self.base_url.format(page_num=i * 10)
            session.get(url=url, proxies=self.proxies, headers=self.headers, verify=False)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.html
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                if self.is_api:
                    extracted_data = self.extract_data(json_data)
                    processed_data = self.process_data(extracted_data)
                else:
                    extracted_data = self.extract_data(json_data)
                    processed_data = self.process_data(extracted_data)
                views_list.extend(processed_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_data(self, extracted_data):

        session = SessionPage()
        item_data = []

        if not extracted_data:
            return []

        for item in extracted_data:
            if self.is_api:
                url = f'https://data.zg.cn//catalog_data_interface/findViewById?id={item["id"]}'
            else:
                url = f'https://data.zg.cn//catalog_schema/findView?id={item["id"]}'
            session.get(url=url, headers=self.headers, proxies=self.proxies, verify=False)

            if self.is_api:
                item_data.append(self.extract_api_page_data(session.response.text, url))
            else:
                item_data.append(self.extract_page_data(session.response.text, url))

        return item_data

    def extract_data(self, json_data):
        data = json.loads(json_data)
        data_list = []

        result = data.get('data', []).get('rows', [])

        for item in result:
            item_id = item.get('id', '')

            data_list.append({
                'id': item_id,
            })

        return data_list

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)

        item = data['data'][0]  # 获取数据列表

        title = item.get('catalogTitle', '')
        subject = item.get('industryName', '')  # 根据数据使用相应的字段名
        description = item.get('catalogDesc', '')
        source_department = item.get('supplyOrg', '')

        # 处理时间戳转换为可读的日期格式
        release_time = datetime.fromtimestamp(item['createdTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
            if item.get('createdTime') else None
        update_time = datetime.fromtimestamp(item['modifiedTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
            if item.get('modifiedTime') else None

        open_conditions = '无条件开放' if item.get('openType', '') == '0' else '有条件开放'  # # 开放类型，可能需要映射到具体说明
        data_volume = item.get('catalogStatistic', {}).get('data_count', None)  # 数据量
        is_api = 'True' if item.get('catalogFormat', '') == 'API' else 'False'  # 通过某个字段判断是否为API
        file_type = [item.get('catalogFormatType', '')]

        access_count = item.get('clickCount', None)
        download_count = item.get('downloadCount', None)
        api_call_count = item.get('callCount', None)
        link = item_url  # 假设没有具体的链接信息
        update_cycle = item.get('updateCycle', '')

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data, url):
        # 提取数据列表
        item = json.loads(json_data).get('data', [])

        if not item:
            return {}

        title = item.get('catalogTitle', '')
        subject = item.get('industryName', '')  # 此示例中没有具体行业名称，字段假设
        description = item.get('catalogDesc', '')
        source_department = item.get('supplyOrg', '')

        # 时间戳转换为可读的日期格式
        release_time = datetime.fromtimestamp(item['createdTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
            if item.get('createdTime') else None
        update_time = datetime.fromtimestamp(item['modifiedTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S') \
            if item.get('modifiedTime') else None

        open_conditions = '无条件开放' if item.get('isOpen') == '1' else '有条件开放'
        data_volume = item.get('clickCount', None)  # 使用点击数代替数据量字段
        is_api = 'True' if item.get('serviceType') == 'webservice' else 'False'
        file_type = [item.get('catalogFormatType', '')]

        access_count = item.get('clickCount', None)
        download_count = item.get('downloadCount', None)
        api_call_count = item.get('callCount', None)
        link = url  # 假设没有具体的链接信息
        update_cycle = item.get('updateCycle', '')

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

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
    page = ZigongCrawler(is_headless=True)
    page.run()
