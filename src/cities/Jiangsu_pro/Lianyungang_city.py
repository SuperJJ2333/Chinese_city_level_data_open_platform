import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class LianyungangCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '连云港市',
                     'province': 'Jiangsu',
                     'total_items_num': 1204 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://www.lyg.gov.cn/data/showDataController/showdata.do?pageNum1={page_num}&xian=0&colum=&value=&dep1seach='
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
                                 "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,"
                                                                                      "en-GB;q=0.7,en-US;q=0.6",
                        "Cookie":"JSESSIONID=F0A4215A4BC49BD3206B3AFC5E3D0A3D; "
                                 "__jsluid_s=a78c4d5de27cecb500d1c4dd65669306","Host":"www.lyg.gov.cn",
                        "Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate","Sec-Fetch-Site":"none",
                        "Sec-Fetch-User":"?1","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT "
                                                                                           "10.0; Win64; x64) "
                                                                                           "AppleWebKit/537.36 ("
                                                                                           "KHTML, like Gecko) "
                                                                                           "Chrome/126.0.0.0 "
                                                                                           "Safari/537.36 "
                                                                                           "Edg/126.0.0.0",
                        "sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                        "sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

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
                url = f'https://www.lyg.gov.cn/data/showDataController/showdetail.do?dmid={item["url_id"]}'
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page, item)
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self,session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://*[@id="xianshimokuai1"]/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div/h1').attr('onclick')

                url_id = self.extract_uuid(frame_url)

                # file_type = [file.text.lower() for file in frame.eles('x://div[4]/div[3]/ul/li')]
                access_count = frame.ele('x://div/span[3]/a').text
                download_count = frame.ele('x://div/span[4]/a').text

                data_list.append({
                    'url_id': url_id,
                    # 'file_type': file_type,

                    'access_count': access_count,
                    'download_count': download_count,
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page, item_info):

        frame = session_page.ele('x://*[@id="jbxx"]/table')

        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://html/body/div[4]/div/div[1]/div[1]').text
        subject = frame.ele(
            'x://tr[5]/td[1]').text
        description = frame.ele('x://tr[2]/td[2]').text
        source_department = frame.ele(
            'x://tr[2]/td[1]').text

        release_time = frame.ele(
            'x://tr[5]/td[2]').text
        update_time = frame.ele(
            'x://tr[5]/td[2]').text

        open_conditions = frame.ele('x://tr[3]/td[1]').text
        data_volume = self.extract_numbers(session_page.ele('x://html/body/div[4]/div/div[1]/span[1]').text)
        file_type = [frame.ele('x://tr[1]/td[2]').text.lower()]
        is_api = 'True' if '接口' in file_type or 'json' in file_type else 'False'

        access_count = item_info['access_count']
        download_count = item_info['download_count']
        api_call_count = download_count if is_api == 'True' else None
        link = session_page.url

        update_cycle = ''

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

    @staticmethod
    def extract_uuid(string):
        pattern = r'view\((\'|\")([0-9a-fA-F-]+)(\'|\")\)'
        match = re.search(pattern, string)
        if match:
            return match.group(2)
        return None

    @staticmethod
    def extract_numbers(string):
        pattern = r'数据量：(\d+).*?文件数：(\d+)'
        match = re.search(pattern, string)
        if match:
            data_volume = int(match.group(1))
            file_count = int(match.group(2))
            return data_volume
        return None


if __name__ == '__main__':
    page = LianyungangCrawler(is_headless=True)
    page.run()
