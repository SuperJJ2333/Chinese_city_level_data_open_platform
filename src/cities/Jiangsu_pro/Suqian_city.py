import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class SuqianCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '宿迁市',
                     'province': 'Jiangsu',
                     'total_items_num': 6063 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://data.suqian.gov.cn/oportal/catalog/index?page={page_num}',
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Connection": "keep-alive",
            "Cookie": "SESSION_WEB_CACHE_KEY=07e6082aa81e460da57422c104fd83e2; arialoadData=false; ftdatasuqiangovcn=0",
            "Host": "data.suqian.gov.cn", "Referer": "https://data.suqian.gov.cn/oportal/catalog/index?page=1",
            "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1", "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
            "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'page': '3', 'orgId': 'null', 'orgDirId': 'null', 'domainId': '0', 'resourceType': 'DATAQUERY',
                       'sortWord': 'modifyDate', 'direction': 'down', 'listPageNum': '3'}

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
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}/{self.total_page_num}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                page_data = self.extract_data(session, i)
                if self.is_api:
                    views_list.extend(page_data)
                else:
                    detailed_data = self.process_page(page_data)
                    views_list.extend(detailed_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)
                if self.is_api:
                    item_data = self.extract_api_page_data(session_page, item)
                else:
                    item_data = self.extract_page_data(session_page, item)
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://*[@id="app"]/div[7]/div[2]/div[3]/ul/li')

        if self.is_api:
            page_data = self.extract_api_page_data(session_page, '')
            data_list.extend(page_data)
            return data_list

        # 循环遍历每一个frame
        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'http://opendata.taizhou.gov.cn{frame_url}'
                else:
                    frame_url = frame_url

                file_type = [file.text for file in frame.eles('x://div[4]/div[3]/ul/li')]

                update_time = frame.ele('x://div[3]/div[3]/span[2]').text

                data_list.append({
                    'url': frame_url,
                    'file_type': file_type,

                    'update_time': update_time,
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page, item):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://*[@id="app"]/div[7]/div[1]/div/div/div/div[1]/ul[1]/li/h4').text
        subject = session_page.ele(
            'x://li[contains(text(),"重点领域")]/span[@class="text-primary"]').text
        description = session_page.ele(
            'x://td[contains(text(),"描述")]/following-sibling::td').text
        source_department = session_page.ele(
            'x://li[contains(text(),"来源部门")]/span[@class="text-primary"]').text

        release_time = item.get('update_time', '')
        update_time = release_time

        open_conditions = session_page.ele(
            'x://li[contains(text(),"开放类型")]/span[@class="text-primary"]').text
        data_volume = session_page.ele('x://td[contains(text(),"文件数")]/following-sibling::td').text
        file_type = item['file_type']
        is_api = 'True' if 'JSON' in file_type else 'False'

        access_count = session_page.ele(
            'x://li[contains(@class,"text-ligth-gray")][1]/img/following-sibling::text()')
        download_count = session_page.ele(
            'x://li[contains(@class,"text-ligth-gray")][2]/img/following-sibling::text()')
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = session_page.ele(
            'x://td[contains(text(),"更新频率")]/following-sibling::td').text

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    @staticmethod
    def extract_api_page_data(session_page, item):
        frames = session_page.eles('x://*[@id="app"]/div[7]/div[2]/div[3]/ul/li')
        models = []
        for frame in frames:
            title = frame.ele('x://div[1]/a').text
            subject = frame.ele('x://div[3]/div[2]/span[2]').text
            description = frame.ele('x://*[@class="describe"]').text
            source_department = frame.ele('x://div[3]/div[1]/span[2]').text

            release_time = frame.ele('x://div[3]/div[3]/span[2]').text
            update_time = frame.ele('x://div[3]/div[4]/span[2]').text

            open_conditions = frame.ele('x://div[3]/div[5]/span[2]').text
            data_volume = None  # 页面中未提供数据量信息
            file_type = ['接口']
            is_api = 'True'

            access_count = frame.ele('x://div[4]/div[2]').text
            download_count = None
            api_call_count = None  # 页面中未提供API调用次数信息
            link = session_page.url

            update_cycle = ''

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle)
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
            2: '每日',
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
    page = SuqianCrawler(is_headless=True)
    page.run()
