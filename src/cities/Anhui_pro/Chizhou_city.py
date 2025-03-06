import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ChizhouCrawler(PageBase):
    """
    xpath 获取网页数据
    目的数据：目录页里
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '池州市',
                     'province': 'Anhui',
                     'total_items_num': 1761,
                     'each_page_count': 6,
                     'base_url': 'https://www.chizhou.gov.cn/OpenData/sjml?page={page_num}'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
                                  "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Encoding": "gzip, deflate, br, zstd", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,"
                                                                                         "en-GB;q=0.7,en-US;q=0.6",
                        "Cookie": "__51vcke__3H45Rx8JOjcZ5VeL=aa1cb8a1-5ce7-5e31-833c-94f19184f5e9; "
                                  "__51vuft__3H45Rx8JOjcZ5VeL=1716373540840; "
                                  "Hm_lvt_88d3314545011934a215cb84e7dcbaa7=1716373541; __51uvsct__3H45Rx8JOjcZ5VeL=2; "
                                  "arialoadData=true; ariawapChangeViewPort=false; "
                                  "__vtins__3H45Rx8JOjcZ5VeL=%7B%22sid%22%3A%20%2257e9d306-2d15-5355-ac94-9637778a7bba"
                                  "%22%2C%20%22vd%22%3A%207%2C%20%22stt%22%3A%2062856%2C%20%22dr%22%3A%206338%2C%20"
                                  "%22expires%22%3A%201717677919824%2C%20%22ct%22%3A%201717676119824%7D",
                        "Host": "www.chizhou.gov.cn", "Referer": "https://www.chizhou.gov.cn/OpenData/sjml",
                        "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin",
                        "Sec-Fetch-User": "?1", "Upgrade-Insecure-Requests": "1",
                        "User-Agent": "Mozilla/5.0 (Windows NT "
                                      "10.0; Win64; x64) "
                                      "AppleWebKit/537.36 ("
                                      "KHTML, like Gecko) "
                                      "Chrome/125.0.0.0 "
                                      "Safari/537.36 "
                                      "Edg/125.0.0.0",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'sort': '0', 'pageNo': '38', 'pageSize': '15'}

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
                # page_data = self.extract_data(session)
                # detailed_data = self.process_page(page_data)
                detailed_data = self.extract_page_data(session)
                views_list.extend(detailed_data)
                self.logger.info(f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']

                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page, item['files_type'])
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page):

        data_list = []
        frames_list = session_page.eles('x://*[@id="listContent"]/div/div[1]')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div/a').attr('href')
                frame_files = [file.text for file in frame.eles('x://div[4]/div[3]/ul')]

                data_list.append({
                    'url': frame_url,
                    'files_type': frame_files
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page):

        frames = session_page.eles('x://*[@id="listContent"]/div')
        model_list = []

        for frame in frames:
            title = frame.ele('x://div[1]/div[1]/a/div').text
            subject = frame.ele('x://div[1]/div[4]').text.split('：')[-1].strip()
            description = frame.ele('x://div[1]/div[7]').text.split('：')[-1].strip()
            source_department = frame.ele('x://div[1]/div[5]').text.split('：')[-1].strip()

            release_time = frame.ele('x://div[1]/div[6]').text.split('：')[-1].strip()
            update_time = frame.ele('x://div[1]/div[6]').text.split('：')[-1].strip()

            open_conditions = ''
            data_volume = None
            file_type = [frame.ele('x://div[1]/div[2]/div').text]
            is_api = 'False'

            access_count = None
            download_count = frame.ele('x://div[2]/div/div[3]/div[2]').text
            api_call_count = None
            link = ''

            update_cycle = ''

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, location=self.name)

            model_list.append(model.to_dict())

        return model_list

    def extract_api_page_data(self, json_data):
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
    page = ChizhouCrawler(is_headless=True)
    page.run()
