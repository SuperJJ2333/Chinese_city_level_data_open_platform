import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuanganCrawler(PageBase):
    """
    一级跳转：get请求：获取json数据，在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '广安市',
                     'province': 'Sichuan',
                     'total_items_num': 9809 + 1000,
                     'each_page_count': 100,
                     'base_url': 'https://www.gadata.net.cn:80/s?buddle.cludove=opendoor&catalog.cludove=gai.resource.search.view&exchange=true&upload=true&interface=true&recordNumber.cludove=100&pageNumber.cludove={page_num}'
                     }
        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"dGl0bGU==JXU1RTdGJXU1Qjg5JXU1RTAyJXU1MTZDJXU1MTcxJXU2NTcwJXU2MzZFJXU1RjAwJXU2NTNFJXU1RTczJXU1M0Yw; JSESSIONID=830C52301A9E1F76150926D9656AB98F; YWNjb3VudF9pbmZv=JTdCJTIyYV9uJTIyJTNBJTIyJTIyJTJDJTIycF9uJTIyJTNBJTIyJTIyJTJDJTIyb19jJTIyJTNBJTIyJTIyJTJDJTIycGhvbmUlMjIlM0ElMjIlMjIlMkMlMjJ1c2VyX3R5cGUlMjIlM0ElMjIlMjIlMkMlMjJpX2QlMjIlM0FmYWxzZSU3RA==","Host":"www.gadata.net.cn:80","Referer":"https://www.gadata.net.cn:80/opendoor/base/zh-cn/code/resource_catalog.html","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.params = {'page': '2', 'limit': '10', 'field': 'data_Update_Time'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url.format(page_num=i)
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

    def extract_data(self, uuid):

        session = SessionPage()
        url = f'https://www.nanchong.gov.cn/data/resCatalog/getinfoitembycode?resourcecode={uuid}'

        session.get(url=url, proxies=self.proxies)

        json_data = session.response.text
        json_data = json.loads(json_data).get('data', '')[0]

        open_conditions = json_data.get('UPDATECYCLENAME', '')
        update_cycle = json_data.get('sharingtype', '')

        return open_conditions, update_cycle

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)
        if data is None:
            return []
        results = data['list_data']  # 根据新的数据路径调整
        models = []

        for item in results:
            title = item.get('resource_title', '')
            subject = item.get('resource_category_name', '')
            description = item.get('resource_abstract', '')
            source_department = item.get('provider_name', '')

            release_time = item.get('publish_date', '')
            update_time = item.get('last_modify', '')

            open_conditions = '无条件开放' if item.get('is_open', 'false').lower() == 'true' else '有条件开放'
            data_volume = item.get('total_number', None)
            file_type = ['xlsx', 'xml', 'json', 'csv', 'rdf']
            is_api = 'True' if 'json' in file_type else 'False'

            access_count = item.get('visit_num', None)
            download_count = item.get('use_num', None)
            api_call_count = item.get('interface_num', None)
            link = f'https://www.gadata.net.cn:80/opendoor/base/zh-cn/code/detail.html?resource_code={item.get("code", "")}'  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('update_period', ''))

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        # 提取数据列表
        results = xmltodict.parse(json_data).get('BusinessMessage', {}).get('data', {}).get('content', {}).get(
            'content', [])
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('topicClassify', '')
            description = item.get('serviceName', '')  # 使用 serviceName 作为描述如果 resourceName 不足够描述性
            source_department = item.get('organName', '')
            release_time = item.get('createTime', '')
            update_time = item.get('updateTime', '')
            open_conditions = '有条件开放' if item.get('isOpen', '') == '1' else '无条件开放'
            data_volume = item.get('resourceMount', '0')  # 数据量信息，如果没有提供默认为 '0'

            access_count = item.get('browseCount', 0)
            download_count = item.get('applyCount', 0)  # 假设 applyCount 可能意味着下载
            api_call_count = item.get('serviceCount', 0)  # 使用 serviceCount 作为 API 调用次数
            link = item.get('preUrl', '')  # 使用 preUrl 作为链接地址，如果有的话

            file_type = ['接口']  # 文件类型，如果没有默认为空
            is_api = 'True'  # 假设 serviceType 为 '4' 表示 API 类型
            update_cycle = '每日'

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
                    'real': "实时",
                    'day': "每日",
                    'week': "每周",
                    'month': "每月",
                    'quarter': "每季度",
                    'year': "每年"
                }

        # 处理空字符串或长度为0的情况
        if it == '' or len(it) == 0:
            return ''

        # 根据字典返回对应的更新周期描述
        return cycle_dict.get(it, it)  # 如果没有匹配的键，返回原始输入


if __name__ == '__main__':
    page = GuanganCrawler(is_headless=True)
    page.run()