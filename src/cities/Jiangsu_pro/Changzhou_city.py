import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage
from DrissionPage._elements.none_element import NoneElement

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ChangzhouCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：xpath get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '常州市',
                     'province': 'Jiangsu',
                     'total_items_num': 3357 + 1000,
                     'each_page_count': 7,
                     'base_url': 'https://www.changzhou.gov.cn/opendata/open/index/datadirectory/city'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "Cookie": "HttpOnly; 4EB8C8A9AA13CC7207C7C8D8F3E46605=7dc53b9d-8362-4e7a-a0ca-9f3b82a9d288; HttpOnly; JSESSIONID=F41CF08094F4F2C5A3900735ACECDF6E",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",

        }

        self.city_type = {'公共资源交易_29': 5, '资源环境_28': 18, '民生服务_25': 29, '综合政务_16': 14, '统计信息_12': 17, '道路交通_9': 79, '人事信息_8': 37, '教育科技_7': 28, '城市建设_5': 63, '交通出行_2': 56}

    def run(self):
        self.total_data.extend(self.process_views())
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.page
        url = self.base_url
        session.get(url=url)

        for i in range(1, self.total_page_num):

            if True:
                if self.is_api:
                    processed_data = self.extract_api_page_data('')
                else:
                    extracted_data = self.extract_data(session)
                    processed_data = self.process_data(extracted_data)
                views_list.extend(processed_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

            time.sleep(1)
            next_page = session.ele('x://*/div[@name="whj_nextPage"]')
            next_page.click(by_js=True)

        return views_list

    def process_data(self, extracted_data):

        session = SessionPage()
        item_data = []

        if not extracted_data:
            return []

        for item in extracted_data:
            if not item['url'].startswith('https'):
                frame_url = f'https://www.changzhou.gov.cn{item["url"]}'
            else:
                frame_url = item['url']
            session.get(url=frame_url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session, item))

        return item_data

    def extract_data(self, session):
        data_list = []

        frames = session.eles('x://*[@id="one-item"]/div/table/tbody')

        for item in frames:
            try:
                frame_url = item.ele('x://tr[1]/td/div[1]/a').attr('href')
            except Exception:
                continue

            if not frame_url.startswith('https'):
                frame_url = f'https://www.changzhou.gov.cn{frame_url}'
            else:
                frame_url = frame_url
            try:
                download_amount = item.ele('x://tr[2]/td/div[2]/span').text.strip()
                access_count = item.ele('x://tr[2]/td/div[3]/span').text.strip()
                data_amount = item.ele('x://tr[2]/td/div[4]/span').text.strip()
            except Exception:
                download_amount = 0
                access_count = 0
                data_amount = 0

            data_list.append({
                'url': frame_url,
                'download_amount': download_amount,
                'access_count': access_count,
                'data_amount': data_amount,
            })

        return data_list

    def extract_page_data(self, session_page, item):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://div[@class="tit5"]').text
        subject = session_page.ele('x://body/div[1]/div[2]/div[1]/div[2]/p[4]/span[1]').text
        description = session_page.ele('x://div[@class="sjml-box3"]/p[2]/span').text
        source_department = session_page.ele('x://body/div[1]/div[2]/div[1]/div[2]/p[10]/span').text

        release_time = session_page.ele('x:/html/body/div[1]/div[2]/div[1]/div[2]/p[8]/span').text
        update_time = session_page.ele('x:/html/body/div[1]/div[2]/div[1]/div[2]/p[9]/span').text

        open_conditions = session_page.ele('x:/html/body/div[1]/div[2]/div[1]/div[2]/p[5]/span').text
        update_cycle = session_page.ele('x:/html/body/div[1]/div[2]/div[1]/div[2]/p[6]/span').text
        data_volume = session_page.ele('x:/html/body/div[1]/div[2]/div[1]/div[2]/p[14]/span').text

        access_count = item.get('access_count', None)
        download_count = item.get('download_amount', None)
        api_call_count = None

        api_frame = session_page.ele("'x://*/li[@class='selectTag']/a'")
        try:
            is_api = 'True' if 'API' in api_frame.text else 'False'
        except Exception:
            is_api = 'False'
        file_type = ['xlsx'] if is_api == 'False' else ['API']  # 默认为数据类型

        link = session_page.url

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
    page = ChangzhouCrawler(is_headless=False)
    page.run()
