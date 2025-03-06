import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class YichunCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：post请求获取api数据，在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '宜春市',
                     'province': 'Jiangxi',
                     'total_items_num': 734,
                     'each_page_count': 15,
                     'base_url': 'http://data.yichun.gov.cn/extranet/rest/apitextapistore/getResourceList'
                     }

        api_city_info = {'name': '宜春市_api',
                         'province': 'Jiangxi',
                         'total_items_num': 24,
                         'each_page_count': 10,
                         'base_url': 'http://data.yichun.gov.cn/extranet/rest/apitextapistore/getServiceList',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Content-Length": "323",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie": "_CSRFCOOKIE=C406F6AE8D9DC72C9AC792597E2A5EA14A9AB3E4; EPTOKEN=C406F6AE8D9DC72C9AC792597E2A5EA14A9AB3E4; JSESSIONID_JAVA=FB9B088C3D1F4A2C80D51B648FA95B69; JSESSIONID=150F8CB5C9425CA0317B0C25A6261883; Hm_lvt_72920cca32daf35e7e46dfa6ccc27328=1718965996; arialoadData=true; ariawapChangeViewPort=true; Hm_lpvt_72920cca32daf35e7e46dfa6ccc27328=1718965999; extranet_token=; login_type=; lastAccessTimestamp=1718966010935",
                        "Host": "data.yichun.gov.cn", "Origin": "http://data.yichun.gov.cn",
                        "Proxy-Connection": "keep-alive",
                        "Referer": "http://data.yichun.gov.cn/extranet/openportal/pages/resource/resourceDeplist.html",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "X-Requested-With": "XMLHttpRequest"}

        self.params = {"token": "Epoint_WebSerivce_**##0601",
                       "params": {"currentPage": "0", "pageSize": "10", "sortField": "", "sortOrder": "desc",
                                  "resourcesName": "", "resourcesType": "", "fileType": "", "resourceSubject": "",
                                  "registDepartGuid": "", "isCountyOu": 0}}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num - 1):
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
                if self.is_api:
                    page_data = self.extract_api_page_data(json_data)
                else:
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
            url = 'http://data.yichun.gov.cn/extranet/rest/apitextapistore/getResourceDetail'
            params = {"token": "Epoint_WebSerivce_**##0601",
                      "params": {"rowGuid": item['rowGuid'], "userGuid": "", "type": "1"}}
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

        for item in results['resourceList']:
            # 提取和构建所需信息
            rowGuid = item.get('rowGuid', '')
            data_amount = item.get('dataCount', '')
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
        update_time = item.get('updateTime', '')  # No specific format given, assuming already in correct format

        open_conditions = item.get('openScope', '')
        data_volume = item.get('dataCount', None)# This detail isn't provided, assume default
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
        api_call_count = item.get('apiCnt', None)  # No API call count detail provided
        link = f'http://data.yichun.gov.cn/extranet/openportal/pages/resource/resource_detail.html?rowguid={upper_item["rowGuid"]}'  # No link detail provided
        update_cycle = item.get('updateFrequency', '')

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)
        model.to_dict()

        return model.to_dict()

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data.strip('"'))
        services = json_data['custom']['serviceList']  # 获取服务列表
        models = []

        for service in services:
            title = service.get('serviceName', '')
            description = service.get('remark', '')
            source_department = service.get('provideDept', '')
            subject = service.get('serviceSubject', '')

            # 这些字段可能没有提供相应的时间字段，使用已有字段来代替
            release_time = service.get('createTime', '')  # 假设创建时间为发布时间
            update_time = service.get('updateTime', '')  # 更新时间

            open_conditions = '申请开放' if service.get('isApplyPass', False) else '不开放'
            data_volume = None  # JSON数据中没有提供数据量，保留为0

            # 基于是否有API状态来判断是否是API
            is_api = 'True' if service.get('apiStatus', '') == '正常' else 'False'
            file_type = ['接口']  # 文件类型未在服务列表中明确，保留为空数组

            access_count = service.get('visitCount', None)
            download_count = service.get('applyCount', None)  # 使用申请次数作为下载次数的近似值
            api_call_count = service.get('applyCount', None)  # API调用次数默认为0，因为数据中未提供

            link = f'http://data.yichun.gov.cn/extranet/openportal/pages/api/api_detail.html?rowguid={service["rowGuid"]}'  # 假设没有具体的链接信息
            update_cycle = ''  # 更新周期未提供

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
    page = YichunCrawler(is_headless=True)
    page.run()
