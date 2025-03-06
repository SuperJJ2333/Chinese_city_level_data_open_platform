import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZhangjiakouCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '张家口市',
                     'province': 'Hebei',
                     'total_items_num': 228 + 200,
                     'each_page_count': 15,
                     'base_url': 'https://kf.zjkzwfw.gov.cn/extranet/rest/dataOpen/getResourceList'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie": "sid=A1D8483CF35A45888D67AC4A7FC2503E; _font_size_ratio_=1.0; _CSRFCOOKIE=C64A6C23C29BAE68FE27572ABD595D09486DC333; EPTOKEN=C64A6C23C29BAE68FE27572ABD595D09486DC333",
                        "Host": "kf.zjkzwfw.gov.cn", "Origin": "https://kf.zjkzwfw.gov.cn",
                        "Referer": "https://kf.zjkzwfw.gov.cn/extranet/openportal/pages/resource/resourcelist.html",
                        "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'token': None, 'usertype': None, 'params': {'currentPage': '0', 'pageSize': '15', 'sortField': '', 'sortOrder': 'desc', 'resourcesName': '', 'resourceSubject': '', 'resourceIndustry': '', 'resourceScene': '', 'registDepartGuid': '', 'openType': '', 'resourcesType': '', 'resourceFormat': ''}}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            self.params['params']['currentPage'] = str(i)
            params = self.encode_params(self.params)

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                # 提取API数据
                urls_list = self.extract_data(json_data)
                # 处理API数据
                page_data = self.process_page(urls_list)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, urls_list):

        session_page = SessionPage()
        items_list = []

        for item in urls_list:
            url = 'https://kf.zjkzwfw.gov.cn/extranet/rest/dataOpen/getResourceDetail'
            params = {'token': None, 'usertype': None, 'params': {'rowGuid': item['rowGuid'], 'type': '1'}}
            params = self.encode_params(params)

            session_page.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

            item_data = self.extract_page_data(session_page.response.text, item)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('custom', [])
        id_lists = []

        for item in results['resultList']:
            # 提取和构建所需信息
            rowGuid = item.get('rowGuid', '')
            data_amount = item.get('openresdataCount', '')
            id_lists.append({
                'rowGuid': rowGuid,
                'data_amount': data_amount
            })

        return id_lists

    def extract_page_data(self, json_data, upper_item):
        data = json.loads(json_data)
        item = data['custom']

        title = item.get('resourcesName', '')
        subject = item.get('resourceSubject', '')
        description = item.get('resourceDetail', '')
        source_department = item.get('provideDepartName', '')

        release_time = item.get('registDate', '')
        update_time = item.get('updateDate', '')  # No specific format given, assuming already in correct format

        open_conditions = item.get('openCondition', '')
        data_volume = upper_item['data_amount']  # This detail isn't provided, assume default
        is_api = 'True' if item.get('jsonUrl', False) else 'False'
        file_types = []
        if item.get('jsonUrl', False):
            file_types.append('JSON')
        if item.get('xmlUrl', False):
            file_types.append('XML')
        if item.get('csvUrl', False):
            file_types.append('CSV')
        if item.get('xlsUrl', False):
            file_types.append('XLS')
        if item.get('xlsxUrl', False):
            file_types.append('XLSX')
        if item.get('rdfUrl', False):
            file_types.append('RDF')

        access_count = item.get('visitCnt', None)
        download_count = item.get('downloadCnt', None)
        api_call_count = None  # No API call count detail provided
        link = f'https://kf.zjkzwfw.gov.cn/extranet/openportal/pages/resource/resource_detail.html?rowguid={upper_item["rowGuid"]}'  # No link detail provided
        update_cycle = item.get('updateFrequency', '')

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self,json_data):
        json_data = json.loads(json_data.strip('"'))
        results = json_data['data']
        models = []

        for item in results['result']:
            title = item.get('zymc', '')
            subject = item.get('filedName', '')
            description = item.get('zy', '')
            source_department = item.get('tgdwmc', '')

            release_time = datetime.strptime(item.get('cjsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
            update_time = datetime.strptime(item.get('gxsj', ''), '%Y%m%d%H%M%S').strftime('%Y-%m-%d')

            open_conditions = ''
            data_volume = None
            is_api = 'True'
            file_type = [item.get('fjhzm', '')]

            access_count = item.get('fws', None)
            download_count = None
            api_call_count = item.get('xzs', None)  # Assuming 0 since not provided
            link = ''
            update_cycle = ''

            model = DataModel(title, subject, description, source_department, release_time, update_time,
                              open_conditions, data_volume, is_api, file_type, access_count, download_count,
                              api_call_count, link, update_cycle, self.name)
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
    def encode_params(params):
        """
        将请求参数编码为URL格式。

        参数:
        params (dict): 请求参数字典。

        返回:
        str: 编码后的URL参数。
        """
        # 将 JSON 字典转换为 URL 编码的表单数据

        encoded_data = json.dumps(params)

        # URL 编码
        encoded_data = urllib.parse.quote(encoded_data)

        return encoded_data




if __name__ == '__main__':
    page = ZhangjiakouCrawler(is_headless=True)
    page.run()
