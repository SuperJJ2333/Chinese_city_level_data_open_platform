import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class JinmenCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '荆门市',
                     'province': 'Hubei',
                     'total_items_num': 121 + 174,
                     'each_page_count': 10,
                     'base_url': 'https://data.jingmen.gov.cn/prod-api/openResource/rest/page?resourceName=&topicClassify=&dataRelam=&organName=%E6%B2%99%E6%B4%8B%E5%8E%BF,%E9%92%9F%E7%A5%A5%E5%B8%82,%E4%BA%AC%E5%B1%B1%E5%B8%82,%E4%B8%9C%E5%AE%9D%E5%8C%BA,%E8%8D%86%E9%97%A8%E9%AB%98%E6%96%B0%E5%8C%BA%C2%B7%E6%8E%87%E5%88%80%E5%8C%BA,%E6%BC%B3%E6%B2%B3%E6%96%B0%E5%8C%BA,%E5%B1%88%E5%AE%B6%E5%B2%AD%E7%AE%A1%E7%90%86%E5%8C%BA&industryType=&isOpen=&updateTimeSort=&applyCountSort=&browseCountSort=&scoreCountSort=&downCountSort=&page={page_num}&size=10'}

        api_city_info = {'name': '荆门市_api',
                         'province': 'Hubei',
                         'total_items_num': 22 + 174,
                         'each_page_count': 10,
                         'base_url': 'https://data.jingmen.gov.cn/prod-api/openService/rest/page?serviceName=&topicClassify=&dataRealm=&organName=&industryType=&servicePublishTimeSort=&applyCountSort=&browseCountSort=&scoreCountSort=&downCountSort=&page={page_num}&size=10',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {
            "Cookie": "Hm_lvt_d921f686cee12cfb5ec38ea5b128d6ae=1718374028; Hm_lpvt_d921f686cee12cfb5ec38ea5b128d6ae=1718374046",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",

        }

        self.city_type = [{'Maanshan': ['3405', 40]}, {'Tonglin': ['3407', 22]}]

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
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
        data = xmltodict.parse(json_data)

        data = data.get('BusinessMessage').get('data')

        title = data.get('resourceName', '')
        subject = data.get('topicClassify', '')
        description = data.get('resourceRes', '')
        source_department = data.get('organName', '')

        release_time = data.get('publishTime', '')
        update_time = data.get('updateTime', '')

        open_conditions = data.get('openCondition', '')
        data_volume = data.get('storageCount', None)  # 假设数据量信息存在于storageCount字段

        file_type = [fmt.strip() for fmt in data.get('resourceShapeType', '').split(',')] if data.get(
            'resourceShapeType') else []

        access_count = data.get('browseCount', None)
        download_count = data.get('downloads', None)
        api_call_count = data.get('serviceCount', None)  # 假设服务计数作为API调用次数

        is_api = 'True' if api_call_count and int(api_call_count) > 0 else 'False'

        link = item_url  # 假设没有具体的链接信息可用

        update_cycle = data.get('updateCycle', '')

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data):
        # 提取数据列表
        results = xmltodict.parse(json_data).get('BusinessMessage', {}).get('data', {}).get('content', {}).get('content', [])
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('topicClassify', '')
            description = item.get('serviceName', '')  # 使用 serviceName 作为描述如果 resourceName 不足够描述性
            source_department = item.get('organName', '')
            release_time = item.get('createTime', '')
            update_time = item.get('updateTime', '')
            open_conditions = '有条件开放' if item.get('isOpen', '') == '1' else '无条件开放'
            data_volume = item.get('resourceMount', None) # 数据量信息，如果没有提供默认为 '0'

            access_count = item.get('browseCount', None)
            download_count = item.get('applyCount', None)  # 假设 applyCount 可能意味着下载
            api_call_count = item.get('serviceCount', None)  # 使用 serviceCount 作为 API 调用次数
            link = item.get('preUrl', '')  # 使用 preUrl 作为链接地址，如果有的话

            file_type = ['接口']  # 文件类型，如果没有默认为空
            is_api = 'True'   # 假设 serviceType 为 '4' 表示 API 类型
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
    page = JinmenCrawler(is_headless=True)
    page.run()
