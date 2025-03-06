import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YongzhouCrawler(PageBase):
    """
    xpath：get请求获取api数据
    二级跳转：get请求：xpath: 获取json数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '永州市',
                     'province': 'Hunan',
                     'total_items_num': 121 + 200,
                     'each_page_count': 10,
                     'base_url': 'https://www.yzcity.gov.cn/cnyz/{type_id}/sjkf_list.shtml'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "Cookie": "HA_STICKY_ms1=web.114; arialoadData=true; ariawapChangeViewPort=true; _yfxkpy_ssid_10003637=%7B%22_yfxkpy_firsttime%22%3A%221718463727394%22%2C%22_yfxkpy_lasttime%22%3A%221718463727394%22%2C%22_yfxkpy_visittime%22%3A%221718466647270%22%2C%22_yfxkpy_domidgroup%22%3A%221718466647270%22%2C%22_yfxkpy_domallsize%22%3A%22100%22%2C%22_yfxkpy_cookie%22%3A%2220240615230207398943312550016945%22%7D",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",

        }

        self.city_type = {'市民政局': 'sjmzj', '市自然资源局': 'szrzyj', '市住房公积金管理中心': 'szfgjj'}

    def run(self):
        for type_name, type_id in self.city_type.items():
            self.logger.info(f'开始获取{type_name}数据')
            self.city_id = type_id
            self.total_items_num = 10
            self.total_page_num = self.count_page_num()
            self.url = self.base_url.format(type_id=type_id, page_num='{page_num}')

            self.total_data.extend(self.process_views())

        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            url = self.url
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
                    extracted_data = self.extract_data(session)
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
                frame_url = f'https://www.yzcity.gov.cn/{item["url"]}'
            else:
                frame_url = item['url']
            session.get(url=frame_url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session, item))

        return item_data

    def extract_data(self, session):
        data_list = []

        frames = session.eles('x://*[@id="tbg"]/div/div/div[2]/div[2]/div[1]/div')

        for item in frames:
            frame_url = item.ele('x://div[3]/a[1]').attr('href')

            data_list.append({
                'url': frame_url,
            })

        return data_list

    def extract_page_data(self, session_page, item_url):
        # 假定session_page是已经加载了HTML内容的对象
        # Assuming session_page is an object that has loaded HTML content
        title = session_page.ele('x://div[@class="title_cen"]/ucaptitle').text
        subject = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="信息资源提供方："]/span[@class="value"]').text
        description = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="信息资源摘要："]/span[@class="value"]').text
        source_department = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="信息资源提供方："]/span[@class="value"]').text  # As the subject and source department are from the same element

        # These fields are not available in the provided HTML, so set defaults or parse appropriately
        release_time = ''
        update_time = release_time  # No update time available in the document
        open_conditions = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="共享类型："]/span[@class="value"]').text  # As the subject and source department are from the same element

        data_volume = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="数据长度："]/span[@class="value"]').text  # As the subject and source department are from the same element

        file_type = [session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="信息资源格式类型："]/span[@class="value"]').text]  # As the subject and source department are from the same element

        is_api = 'True' if 'json' in file_type else 'False'
        access_count = None  # Not available
        download_count = None  # Not available
        api_call_count = None  # Assume 0 since not specified

        link = session_page.url  # Assume this method exists to get the current page URL

        update_cycle = session_page.ele(
            'x://div[@class="c_detail"]/div[span[@class="name"]="更新周期："]/span[@class="value"]').text  # As the subject and source department are from the same element

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

            access_count = item.get('browseCount', 0)
            download_count = item.get('applyCount', 0)  # 假设 applyCount 可能意味着下载
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
    page = YongzhouCrawler(is_headless=True)
    page.run()
