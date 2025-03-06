import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ShangraoCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '上饶市',
                     'province': 'Jiangxi',
                     'total_items_num': 11 + 50,
                     'each_page_count': 5,
                     'base_url': 'http://data.zgsr.gov.cn:2003/orgInfoListByName.jspx'}

        api_city_info = {'name': '上饶市_api',
                         'province': 'Jiangxi',
                         'total_items_num': 22 + 50,
                         'each_page_count': 5,
                         'base_url': 'http://data.zgsr.gov.cn:2003/orgInfoListByName.jspx',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie": "JSESSIONID=DEA1EE8790B0C5A02FDA7CB296B824F9; _jspxcms=0a8a043a711b45f0b32eb1de3529d871",
                        "Host": "data.zgsr.gov.cn:2003", "Origin": "http://data.zgsr.gov.cn:2003",
                        "Referer": "http://data.zgsr.gov.cn:2003/node/2.jspx",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "X-Requested-With": "XMLHttpRequest"}

        self.params = {'nodeId': '2', 'page': '1'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            params = self.params.copy()
            params['page'] = str(i)
            url = self.base_url
            session.post(url=url, proxies=self.proxies, headers=self.headers, data=params)

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
                continue

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://body/div[1]/div')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/h3/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'http:{frame_url}'
                else:
                    frame_url = frame_url

                # file_type = [file.text.lower() for file in frame.eles('x://div[4]/div[3]/ul/li')]

                data_list.append({
                    'url': frame_url,
                    # 'file_type': file_type
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):

        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://div[@class="title"]/a[@id="dataCatalogTitle"]').text
        subject = session_page.ele(
            'x://ul/li[contains(text(),"所属主题")]').text.split("：")[-1]
        description = "详细信息请访问原始网页。"  # 详细描述可能需要用户直接查看网页
        source_department = session_page.ele(
            'x://ul/li[contains(text(),"数据来源")]').text.split("：")[1]

        release_time = session_page.ele(
            'x://ul/li[contains(text(),"发布日期")]').text.split("：")[1]
        update_time = release_time  # 页面中未提供更新时间信息
        open_conditions = ''
        data_volume = session_page.ele(
            'x://ul/li[contains(text(),"条")]').text.split("：")[1]

        is_api = 'False'  # 假设页面中没有API服务

        file_type = []

        access_count = session_page.ele(
            'x://div/span[contains(text(),"访问量")]').text.split("：")[1]
        download_count = session_page.ele(
            'x://div/span[contains(text(),"下载量")]').text.split("：")[1]
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = session_page.ele(
            'x://ul/li[contains(text(),"更新周期")]').text.split("：")[1].strip()

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
    page = ShangraoCrawler(is_headless=True)
    page.run()
