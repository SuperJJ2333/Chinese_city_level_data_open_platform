import concurrent
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class MianyangCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '绵阳市',
                     'province': 'Sichuan',
                     'total_items_num': 9243 + 1000,
                     'each_page_count': 10,
                     'base_url': 'http://data.mianyang.cn/dataDirectory/getShowMsg.jspx?dir_code=&dir_name=&dir_trade=&dir_codetheme=&dir_sharetype=&isopen=&startTime=&delTime=&startNum={page_num}&queryNum=8&id=&type=2&mysort=&totalNum=9243&limitNum=8&elemId=pagination'
                     }


        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {"Accept":"*/*","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; JSESSIONID=1B79DB8BC32F48B7EE11835EF2582A4A","Host":"data.mianyang.cn","Referer":"http://data.mianyang.cn/ly/index.jhtml","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest"}

        self.city_type = [{'Maanshan': ['3405', 40]}, {'Tonglin': ['3407', 22]}]

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            url = self.base_url.format(page_num=i * 8)
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
                    processed_data = self.process_multi_pages(extracted_data)
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
            url = f'http://data.mianyang.cn/dataDirectory/showBasicMsg.jspx?id={item["id"]}'
            session.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session.response.text, url))

        return item_data

    def process_multi_pages(self, urls_list):
        items_list = []

        with ThreadPoolExecutor(max_workers=5) as executor:

            future_to_item = {executor.submit(self.process_single_page, item): item for item in urls_list}

            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    item_data = future.result()
                    items_list.append(item_data)
                except Exception as e:
                    self.logger.error(f'处理单页数据失败 - {e}')

        return items_list

    def process_single_page(self, item):
        session = SessionPage()

        url = f'http://data.mianyang.cn/dataDirectory/showBasicMsg.jspx?id={item["id"]}'
        session.get(url=url, headers=self.headers, proxies=self.proxies)

        item_data = self.extract_page_data(session.response.text, url)

        return item_data

    def extract_data(self, json_data):
        data = json.loads(json_data)
        data_list = []

        result = data.get('elementthing', []).get('listPage', [])

        if result['list'] is None:
            return []

        for item in result['list']:

            item_id = item.get('id', '')

            data_list.append({
                'id': item_id,
            })

        return data_list

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)

        data = data.get('elementthing').get('showBasicList')

        if data is None:
            self.logger.warning(f'解析数据失败 - {item_url}')
            return {}

        title = data.get('dir_name', '')
        subject = data.get('sszt', '')
        description = title
        source_department = data.get('dir_office', '')

        release_time = data.get('dir_issuertime', '')
        update_time = data.get('gmt_modified', '')

        open_conditions = '无条件开放' if data.get('dir_statistics_data', '')[0].get('isopen', '') == "1" else '有条件开放'
        data_volume = data.get('dir_statistics_data', '')[0].get('db_opennum', '')  # 假设数据量信息存在于storageCount字段

        file_type = []

        access_count = None
        download_count = data.get('dir_statistics_data', '')[0].get('db_downloadnum', '')  # 假设数据量信息存在于storageCount字段

        is_api = 'False'

        api_call_count = data.get('dir_statistics_data', '')[0].get('service_time_All', None)

        link = item_url  # 假设没有具体的链接信息可用

        update_cycle = self.format_update_cycle(data.get('qh_level', ''))  # 假设更新周期信息存在于 dir_updatecycle 字段

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
            data_volume = item.get('resourceMount', '0')  # 数据量信息，如果没有提供默认为 '0'

            access_count = item.get('browseCount', None)
            download_count = item.get('applyCount', None)  # 假设 applyCount 可能意味着下载
            api_call_count = item.get('serviceCount', 0)  # 使用 serviceCount 作为 API 调用次数
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
    page = MianyangCrawler(is_headless=True)
    page.run()
