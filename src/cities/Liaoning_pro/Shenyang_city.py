import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ShenyangCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '沈阳市',
                     'province': 'Liaoning',
                     'total_items_num': 2075 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://data.shenyang.gov.cn/oportal/catalog/index?page={page_num}'
                     }

        api_city_info = {'name': '沈阳市',
                         'province': 'Liaoning',
                         'total_items_num': 2075 + 1000,
                         'each_page_count': 10,
                         'base_url': 'https://data.shenyang.gov.cn/oportal/catalog/index?page={page_num}'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Connection": "keep-alive",
            "Cookie": "clwz_blc_pst_datax2eshenyangx2egovx2ecn=3375638538.20480; __utrace=f93d1ce8935faae4748100d66936e88a; __51cke__=; OPENSESSION=95e4433e-7770-4e8f-ab1d-531bfe7b31e5; __tins__21348701=%7B%22sid%22%3A%201718978142128%2C%20%22vd%22%3A%203%2C%20%22expires%22%3A%201718979956047%7D; __tins__21348707=%7B%22sid%22%3A%201718978142131%2C%20%22vd%22%3A%203%2C%20%22expires%22%3A%201718979956050%7D; __51laig__=6",
            "Host": "data.shenyang.gov.cn", "Referer": "https://data.shenyang.gov.cn/oportal/catalog/",
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

                item_data = self.extract_page_data(session_page, item['file_type'])
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://*[@id="app"]/div[6]/div[2]/div[3]/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'http://dt.gov.cn{frame_url}'
                else:
                    frame_url = frame_url

                file_type = [file.text.lower() for file in frame.eles('x://div[4]/div[3]/ul/li')]

                data_list.append({
                    'url': frame_url,
                    'file_type': file_type
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):

        frame = session_page.ele('x://*[@id="app"]/div[6]/div[2]/div/ul/li[1]/table')

        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://*[@id="app"]/div[6]/div[1]/div/div/div/div[1]/ul[1]/li/h4').text
        subject = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[2]/span').text
        description = frame.ele('x://tr[7]/td[2]').text
        source_department = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[1]/span').text

        release_time = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[3]/span').text
        update_time = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[4]/span').text

        open_conditions = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[5]/span').text
        data_volume = frame.ele('x://tr[1]/td[4]').text
        file_type = files_type
        is_api = 'True' if '接口' in file_type else 'False'

        access_count = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[6]').text
        download_count = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[7]').text
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = frame.ele('x://tr[4]/td[2]').text

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    @staticmethod
    def extract_api_page_data(json_data):
        json_data = json.loads(json_data)

        # 假设所有相关信息都在 'smcDataSetList' 键中
        results = json_data.get('result', [])
        models = []

        for item in results['data']:
            # 提取和构建所需信息
            title = item.get('name', '')
            subject = item.get('appTypeName', '')
            description = item.get('summary', '')
            source_department = item.get('companyName', '')

            release_time = datetime.strptime(item.get('lastUpdateTime', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('lastUpdateTime', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

            open_conditions = ''  # 假设默认为'无'
            data_volume = item.get('dataCount', '')  # 假设默认为0
            is_api = 'True'
            file_type = ['json']

            access_count = item.get('viewCount', 0)
            download_count = item.get('downCount', 0)
            api_call_count = 0
            link = f'http://open.huaibeidata.cn:1123/#/interface/detail/{item["id"]}'  # 未提供
            update_cycle = item.get('updateCycle', '')

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
    page = ShenyangCrawler(is_headless=True)
    page.run()
