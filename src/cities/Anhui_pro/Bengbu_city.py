import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class BengbuCrawler(PageBase):
    """
    xpath 直接获取信息
    目的数据在目录页中
    """
    def __init__(self, is_headless=True):
        city_info = {'name': '蚌埠市',
                     'province': 'Anhui',
                     'total_items_num': 230,
                     'each_page_count': 10,
                     'base_url': 'https://www.bengbu.gov.cn/site/label/8888?siteId=6795621&pageSize=10&pageIndex={page_num}&dataAreaId=&isDate=&dateFormat=&length=&providerOrgan=&sortField=publishDate&sortOrder=desc&result=&resourceType=&labelName=dataOpenResourcePageList'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
  "Accept": "text/html, */*; q=0.01",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  "Cookie": "__jsluid_s=6eb95ce8144130e3c25f9904bdc3307e; Hm_lvt_acd9cc04b6e1fdb4fb70c7816a9056f8=1716373347,1717504785; bengbu_gova_SHIROJSESSIONID=3edb6eee-f56b-4bd7-8376-293ccb295d9c; JSESSIONID=74D2AA7C28DF1C6DAB3EDA9155F71D8F; bengbu_govf_SHIROJSESSIONID=f7b56915-bbfe-4c40-bcd2-cbd757708ce6; SHIROJSESSIONID=d6cd3ec5-2924-494c-9ebf-919d5080ce8f; Hm_lpvt_acd9cc04b6e1fdb4fb70c7816a9056f8=1717505138; wzaConfigTime=1717505138797",
  "Host": "www.bengbu.gov.cn",
  "Ls-Language": "zh",
  "Referer": "https://www.bengbu.gov.cn/site/tpl/5631",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
  "X-Requested-With": "XMLHttpRequest",
  "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Windows\""
}

        self.params = {}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):
        """处理所有页面的视图数据。"""

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            # self.params['pageNo'] = str(i)
            url = self.base_url.format(page_num=i)
            session.get(url=url, headers=self.headers, proxies=self.proxies)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                frame_list = session.eles('xpath://div[1]/div[2]/ul/li')
                page_data = self.process_page(frame_list)

                views_list.extend(page_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, url_list):

        item_list = []
        session_page = SessionPage()

        for url in url_list:
            url_text = url.ele('x://div[2]/a').attr('href') or ''
            file_type = url.ele('x://div[2]/div[2]/p[1]/a/em').text if url.ele('x://div[2]/div[2]/p[1]/a/em') else ''
            if not re.match(r'^https://www.bengbu.gov.cn/site', url_text):
                continue
            else:
                url = url_text

            session_page.get(url=url, headers=self.headers, proxies=self.proxies)

            # try:
            #     item_data = self.extract_page_data(session_page, file_type, url)
            # except Exception as e:
            #     self.logger.error(f'{url} - 解析页面数据失败 - {e}' )
            #     continue

            item_data = self.extract_page_data(session_page, file_type, url)

            item_list.append(item_data)

        session_page.close()
        return item_list

    def extract_data(self, json_data):

        data = json_data

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        models = []

        for item in results['result']:
            # 提取和构建所需信息
            title = item.get('zy', '')
            subject = item.get('filedName', '')
            description = item.get('zymc', '')
            source_department = item.get('tgdwmc', '')
            release_time = datetime.strptime(item.get('zxtbsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('gxsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            open_conditions = ''  # 假设默认为'无'
            data_volume = 0  # 假设默认为0
            is_api = 'False'  # 假设默认为'否'
            file_type = [item.get('fjhzm', '')]
            access_count = item.get('fws', 0)
            download_count = item.get('xzs', 0)  # 假设默认为0
            api_call_count = 0  # 假设默认为0
            link = ''  # 未提供
            update_cycle = self.format_update_cycle(item.get('gxpl', ''))

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle)
            models.append(model.to_dict())

        return models

    def extract_page_data(self, session_page, file_type, url):

        try:
            title = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[1]/h1').text
            subject = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[2]').text.split('：')[-1]
            description = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[1]').text.split('：')[-1]
            source_department = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[3]').text.split('：')[-1]

            if not session_page.ele('x://*[@id="ls_wenzhang"]/h2/span'):
                release_time = datetime.strptime(session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[5]').text.split('：')[-1],
                                                 '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')
                update_time = datetime.strptime(session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[6]').text.split('：')[-1],
                                                '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')

                open_conditions = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[11]').text.split('：')[-1]
                data_volume = None  # 假设默认为0
                is_api = 'False'
                file_type = [file_type]

                access_count = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[8]').text.split('：')[-1]
                download_count = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[9]').text.split('：')[-1]
                api_call_count = None
                link = url

                update_cycle = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[7]').text.split('：')[-1]
            else:
                release_time = datetime.strptime(session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[7]').text.split('：')[-1],
                                                 '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')
                update_time = datetime.strptime(session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[8]').text.split('：')[-1],
                                                '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')

                open_conditions = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[14]').text.split('：')[-1]
                data_volume = None # 假设默认为0
                is_api = 'True'
                file_type = [file_type]

                access_count = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[12]').text.split('：')[-1]
                download_count = None
                api_call_count = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[6]').text.split('：')[-1]
                link = url

                update_cycle = session_page.ele('x://*[@id="ls_wenzhang"]/div[1]/div[2]/ul/li[9]').text.split('：')[-1]

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, location=self.name)

            return model.to_dict()
        except Exception as e:
            self.logger.error(f'{url} - 解析页面数据失败 - {e}')
            return None




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
    page = BengbuCrawler(is_headless=True)
    page.run()
