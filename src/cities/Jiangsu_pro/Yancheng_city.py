import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YanchengCrawler(PageBase):
    """
    一级跳转：get请求：获取json数据，在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '盐城市',
                     'province': 'Jiangsu',
                     'total_items_num': 4018 + 1500,
                     'each_page_count': 100,
                     'base_url': 'https://www.yancheng.gov.cn/opendata/ump_gateway/open/data_service/listPageCatalogVo?pageNumber={page_num}&pageSize=100&categoryType=INDUSTRY_CLASS&searchParam=%257B%2522COND%2522%3A%255B%257B%2522columnName%2522%3A%2522catalogState%2522%2C%2522relOpt%2522%3A%2522%3D%2522%2C%2522value%2522%3A25%2C%2522valueType%2522%3A%2522STRING%2522%257D%255D%2C%2522ORDER%2522%3A%255B%257B%2522columnName%2522%3A%2522PUBLISH_TIME%2522%2C%2522dir%2522%3A%2522DESC%2522%257D%255D%257D'
                     }

        api_city_info = {'name': '盐城市_api',
                         'province': 'Jiangsu',
                         'total_items_num': 138,
                         'each_page_count': 10,
                         'base_url': 'https://www.yancheng.gov.cn/opendata/ump_gateway/open/data_service/listPageCatalogVo?pageNumber={page_num}&pageSize=10&categoryType=INDUSTRY_CLASS&searchParam=%257B%2522COND%2522%3A%255B%257B%2522columnName%2522%3A%2522serviceCatalog%2522%2C%2522relOpt%2522%3A%2522%3D%2522%2C%2522value%2522%3A1%2C%2522valueType%2522%3A%2522STRING%2522%257D%2C%257B%2522columnName%2522%3A%2522catalogState%2522%2C%2522relOpt%2522%3A%2522%3D%2522%2C%2522value%2522%3A25%2C%2522valueType%2522%3A%2522STRING%2522%257D%255D%2C%2522ORDER%2522%3A%255B%257B%2522columnName%2522%3A%2522PUBLISH_TIME%2522%2C%2522dir%2522%3A%2522DESC%2522%257D%255D%257D',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Connection": "keep-alive", "Content-Type": "application/json;charset=UTF-8",
                        "Cookie": "name=value; __jsluid_s=ae51ce88d1b3e885579c7fabc820b66b",
                        "Host": "www.yancheng.gov.cn", "Referer": "https://www.yancheng.gov.cn/opendata/openHome",
                        "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'page': '2', 'limit': '10', 'field': 'data_Update_Time'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url.format(page_num=i)
            session.get(url=url, proxies=self.proxies, headers=self.headers)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.html
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                if self.is_api:
                    processed_data = self.extract_api_page_data(json_data)
                else:
                    processed_data = self.extract_page_data(json_data, '')
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
            url = f'https://data.jingmen.gov.cn/prod-api/openResource/rest/findView?id={item["id"]}'
            session.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session.response.text, url))

        return item_data

    def extract_data(self, json_data):
        data = xmltodict.parse(json_data)
        data_list = []

        result = data.get('BusinessMessage', []).get('data', [])

        if result['content'] is None:
            return []

        for item in result['content']['content']:
            if isinstance(item, dict):
                item_id = item.get('id', '')
            elif isinstance(item, str):
                item = result['content']['content']
                item_id = item.get('id', '')

                data_list.append({
                    'id': item_id,
                })
                continue
            else:
                item_id = item.get('id', '')

            data_list.append({
                'id': item_id,
            })

        return data_list

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)
        results = data['resultData']['list']  # 根据新的数据路径调整
        models = []

        for item in results:
            title = item.get('catalogName', '')
            subject = item.get('catalogKeywords', '')  # 如果存在
            description = item.get('catalogAbstract', '')
            source_department = item.get('deptName', '')

            release_time = item.get('createTime', '')
            update_time = item.get('publishTime', '')  # 使用新的字段名称
            open_conditions = '无条件开放' if item.get('shareType', 0) == 0 else '有条件开放'  # 使用新的字段名称
            data_volume = item.get('dataCount', None)
            is_api = 'True' if item.get('interfaceType', 0) == 1 else 'False'
            file_type = ['json'] if item.get('isJson', 0) == 1 else ['excel'] if item.get('isExcel', 0) == 1 else [
                'csv'] if item.get('isCsv', 0) == 1 else []

            access_count = item.get('viewCount', None)
            download_count = item.get('downloadCount', None)
            api_call_count = None  # JSON数据中未提供API调用次数
            link = f'https://www.yancheng.gov.cn/opendata/openCatalog/deatil/{item.get("catalogPk", "")}'  # 假设没有具体的链接信息
            update_cycle = item.get('updateCycle', '')

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        # 提取数据列表
        data = json.loads(json_data)
        results = data['resultData']['list']  # 使用正确的路径获取数据列表
        models = []

        for item in results:
            title = item.get('catalogName', '')
            subject = item.get('industryName', '')  # 使用行业名称作为主题
            description = item.get('catalogAbstract', '')
            source_department = item.get('deptName', '')

            release_time = item.get('createTime', '')
            update_time = item.get('publishTime', '')  # 使用新的字段名称
            open_conditions = item.get('openCondition', '')
            data_volume = item.get('dataCount', None)

            # 根据是否有接口判断是否为API
            is_api = 'True'

            # 确定文件类型
            file_type = []
            if item.get('isJson', 0) == 1:
                file_type.append('json')
            if item.get('isExcel', 0) == 1:
                file_type.append('excel')
            if item.get('isCsv', 0) == 1:
                file_type.append('csv')

            access_count = item.get('viewCount', None)
            download_count = item.get('downloadCount', None)
            api_call_count = None  # JSON数据中未提供API调用次数
            link = f'https://www.yancheng.gov.cn/opendata/interface/detail/{item.get("catalogPk", "")}'  # 假设没有具体的链接信息
            update_cycle = item.get('updateCycle', '')

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
    page = YanchengCrawler(is_headless=True)
    page.run()
