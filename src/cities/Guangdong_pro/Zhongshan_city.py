import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZhongshanCrawler(PageBase):
    """
    xpath: post 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath: get 目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '中山市',
                     'province': 'Guangdong',
                     'total_items_num': 810 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://zsdata.zs.gov.cn/web/queryDomain'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Cookie": "shiro.sesssion=e71e9304-6a2c-49d4-8da6-f03ff3c681e2; zh_choose_undefined=s; _gscu_1860267850=17925296bjgnlg42; _gscbrs_1860267850=1; zh_choose_null=s; _gscs_1860267850=17925296x8gi2m42|pv:8",
                        "Host": "zsdata.zs.gov.cn", "Origin": "https://zsdata.zs.gov.cn",
                        "Referer": "https://zsdata.zs.gov.cn/web/dataList?orgId=0&domainId=&resourceType=DATA",
                        "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'page': '5', 'resourceType': 'DATA', 'sortWord': 'lastUpdateTime', 'direction': 'null',
                       'listPageNum': '5'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['page'] = str(i)
            self.params['listPageNum'] = str(i)
            session.post(url=url, proxies=self.proxies, headers=self.headers, data=self.params)

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

                item_data = self.extract_page_data(session_page, '')
            except Exception as e:
                self.logger.warning(f'解析第{item["id"]}条数据失败 - {e}')
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://dl/dd')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/h2/a').attr('href')

                url_id = self.extract_id(frame_url)

                # 提取并输出匹配的内容
                frame_url = f'https://zsdata.zs.gov.cn/web/dataServerView?id={url_id}'

                data_list.append({
                    'url': frame_url,
                })
            except Exception as e:
                self.logger.warning(f'解析第{page_num}页数据失败 - {e}')
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):

        frames = session_page.ele('x://body/div[4]/div/div/div/div/div/table')

        # Assuming session_page is already loaded with HTML content
        title = session_page.ele('x://h2/a').text
        subject = frames.ele('x://tr[4]/td[2]').text
        description = frames.ele('x://tr[2]/td').text
        source_department = frames.ele('x://tr[1]/td/span').text

        # Dates are not directly available for release_time in the given HTML
        release_time = frames.ele('x://tr[3]/td[2]').text
        update_time = frames.ele('x://tr[6]/td[2]').text

        # Not directly available, using defaults or related available data
        open_conditions = frames.ele('x://tr[5]/td[2]').text
        data_volume = frames.ele('x://tr[3]/td[1]').text
        # 计算文件类型
        file_type = []
        for file in session_page.eles('x://div[@class="data_main"]/div/span'):
            file_extension = file.text
            if file_extension not in file_type:
                file_type.append(file_extension)

        is_api = 'False' if 'Json' not in file_type else 'True'

        access_count = frames.ele('x://tr[6]/td[1]').text
        download_count = frames.ele('x://tr[5]/td[1]').text
        api_call_count = None  # Placeholder as no relevant data is available
        link = session_page.url

        update_cycle = frames.ele('x://tr[4]/td[1]').text

        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, int(data_volume), is_api, file_type,
                          int(access_count), int(download_count), api_call_count, link, update_cycle, self.name)

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

            access_count = item.get('viewCount', None)
            download_count = item.get('downCount', None)
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

    @staticmethod
    def extract_id(string):
        match = re.search(r"javascript:dataserver\.view\('(.+?)'\)", string)
        if match:
            return match.group(1)
        return None


if __name__ == '__main__':
    page = ZhongshanCrawler(is_headless=True)
    page.run()
