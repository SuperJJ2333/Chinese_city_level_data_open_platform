import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YueyangCrawler(PageBase):
    """
    XPATH：get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '岳阳市',
                     'province': 'Hunan',
                     'total_items_num': 121 + 200,
                     'each_page_count': 6,
                     'base_url': 'https://www.yueyang.gov.cn/webapp/yydsj/dataListIframe.jsp?type=1&id={type_id}&sort=1&formId=&dataInfo.offset={page_num}&dataInfo.desc=false'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "Cookie": "HttpOnly; 4EB8C8A9AA13CC7207C7C8D8F3E46605=7dc53b9d-8362-4e7a-a0ca-9f3b82a9d288; HttpOnly; JSESSIONID=F41CF08094F4F2C5A3900735ACECDF6E",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",

        }

        self.city_type = {'公共资源交易_29': 5, '资源环境_28': 18, '民生服务_25': 29, '综合政务_16': 14, '统计信息_12': 17, '道路交通_9': 79, '人事信息_8': 37, '教育科技_7': 28, '城市建设_5': 63, '交通出行_2': 56}

    def run(self):
        for type_name, type_id in self.city_type.items():
            self.logger.info(f'开始获取{type_name}数据')
            self.city_id = type_id
            self.total_items_num = int(type_name.split('_')[-1])
            self.total_page_num = self.count_page_num()
            self.url = self.base_url.format(type_id=type_id, page_num='{page_num}')

            self.total_data.extend(self.process_views())

        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            url = self.url.format(page_num=i * 6)
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
                frame_url = f'https://www.yueyang.gov.cn/webapp/yydsj/{item["url"]}'
            else:
                frame_url = item['url']
            session.get(url=frame_url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session, item))

        return item_data

    def extract_data(self, session):
        data_list = []

        frames = session.eles('x://body/div[@class="szkf-box-list"]')

        for item in frames:
            frame_url = item.ele('x://div[1]/a').attr('href')

            data_list.append({
                'url': frame_url,
            })

        return data_list

    def extract_page_data(self, session_page, item_url):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://*[@id="szfContent"]/div/div/div[2]/h1').text
        subject = session_page.ele(
            'x://tr[td[contains(text(), "国家主题分类")]]/td[2]').text
        description = session_page.ele(
            'x://tr[td[contains(text(), "摘要")]]/td[2]').text
        source_department = session_page.ele(
            'x://tr[td[contains(text(), "数据提供方单位")]]/td[2]').text

        release_time = session_page.ele(
            'x://tr[td[contains(text(), "首次发布时间")]]/td[2]').text
        update_time = session_page.ele(
            'x://tr[td[contains(text(), "更新时间")]]/td[2]').text

        open_conditions = session_page.ele(
            'x://tr[td[contains(text(), "公开属性")]]/td[2]').text
        data_volume = None  # 页面中未提供数据量信息
        file_type = session_page.ele(
            'x://tr[td[contains(text(), "数据格式")]]/td[2]').text.split(',')
        is_api = 'False' if 'API' not in file_type else 'True'

        access_count = session_page.ele(
            'x://tr[td[contains(text(), "访问量")]]/td[2]').text
        download_count = session_page.ele(
            'x://tr[td[contains(text(), "下载次数")]]/td[2]').text
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = session_page.ele(
            'x://tr[td[contains(text(), "更新频率")]]/td[2]').text

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
    page = YueyangCrawler(is_headless=True)
    page.run()
