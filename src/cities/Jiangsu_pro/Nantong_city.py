import json
import time
from datetime import datetime

import requests
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class NantongCrawler(PageBase):
    """
    一级跳转：get请求：获取json数据，在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '南通市',
                     'province': 'Jiangsu',
                     'total_items_num': 697 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://data.nantong.gov.cn/api/anony/portalResource/findResourceByPage?page={page_num}&size=10&sortType=&sortStyle=&themeType=&industryType=&applicationType=&isOpenToSociety=&resType=6&companyId=&assessmentUser=&comType=1'}

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"__jsluid_s=88f61873fad4527e262d7c508b03e5d1; SESSION=e30a82b3-2361-49a2-b7fb-c297eaa07c3b; imgurl=\"http://null/dssg/\"; SF_cookie_41=38054083; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}","Host":"data.nantong.gov.cn","Referer":"https://data.nantong.gov.cn/","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.params = {'page': '2', 'limit': '10', 'field': 'data_Update_Time'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num - 1):
            url = self.base_url.format(page_num=i)
            response = requests.get(url=url, proxies=self.fiddler_proxies, headers=self.headers, verify=False)

            while True:
                try:
                    json_data = response.text if response.text else response.content
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
            if not item['url'].startswith('http'):
                frame_url = f'https://data.nantong.gov.cn/api/anony/portalResource/getResourceById?id={item["url"]}'
            else:
                frame_url = item['url']
            response = requests.get(url=frame_url, headers=self.headers, proxies=self.fiddler_proxies, verify=False)

            item_data.append(self.extract_page_data(response.text, item))

        return item_data

    def extract_data(self, json_data):
        data_list = json.loads(json_data).get('data', [])

        url_list = []

        for item in data_list['content']:
            frame_url = item.get('id', '')

            url_list.append({
                'url': frame_url,
            })

        return url_list

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)

        # 假设相关信息直接在 'data' 键中
        item = data.get('data', {})

        # 提取和构建所需信息
        title = item.get('resName', '')
        subject = item.get('themeTypeName', '')  # 使用新提供的字段名称
        description = item.get('abstracts', '')
        source_department = item.get('companyName', '')

        release_time = item.get('publishTime', '')
        update_time = item.get('updateTime', '')

        open_conditions = '有条件共享' if item.get('resLevel', '') == '1' else '无条件共享'  # JSON数据中未提供
        data_volume = None  # 假设默认为0
        is_api = 'False'  # 假设默认为'否'
        file_type = ['XLS', 'XML', 'JSON', 'CSV']
        access_count = item.get('scanNum', None)
        download_count = item.get('downNum', None)
        api_call_count = None  # 假设默认为0
        link = ''  # 未提供
        update_cycle = item.get('updateCycle', '')

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

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
    page = NantongCrawler(is_headless=True)
    page.run()
