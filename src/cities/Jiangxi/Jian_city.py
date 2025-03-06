import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class JianCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '吉安市',
                     'province': 'Jiangxi',
                     'total_items_num': 1364 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://opendata.jian.gov.cn/opendata-portal/prod-api/openData/page'
                     }

        api_city_info = {'name': '吉安市_api',
                         'province': 'Jiangxi',
                         'total_items_num': 148 + 10,
                         'each_page_count': 10,
                         'base_url': 'https://opendata.jian.gov.cn/opendata-portal/prod-api/apiService/page',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "authorization": "Bearer",
            "content-type": "application/json;charset=UTF-8",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin"
        }

        self.params = '{"current":2,"pageSize":10,"dataName":"","resResourceType":"","resOrgCode":[],"dataFieldCode":[],"resShareType":"","responsibilityListFlag":"","orderUpdateTime":"desc"}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['current'] = str(i)
            params = self.params

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=params, verify=False)

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
            if self.is_api:
                url = f'https://opendata.jian.gov.cn/opendata-portal/prod-api/apiService/infoById?apiId={item.get("rowGuid")}'
            else:
                url = f'https://opendata.jian.gov.cn/opendata-portal/prod-api/openData/infoById?dataId={item.get("rowGuid")}'

            headers = {
                "accept": "application/json, text/plain, */*",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "authorization": "Bearer",
                "priority": "u=1, i",
                "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin"
            }
            session_page.get(url=url, headers=headers, proxies=self.proxies, verify=False)

            if self.is_api:
                item_data = self.extract_api_page_data(session_page.response.text)
            else:
                item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    def extract_data(self, json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results:
            if self.is_api:
                rowGuid = item.get('apiId', '')
            else:
                # 提取和构建所需信息
                rowGuid = item.get('dataId', '')
            id_lists.append({
                'rowGuid': rowGuid,
            })

        return id_lists

    def extract_page_data(self, json_data, url):
        item = json.loads(json_data)  # 更新数据路径到单个数据对象

        data = item['data']

        # Extract basic info
        title = data.get('dataName', '')
        description = data.get('resRemark', '') if data.get(
            'resRemark') else title  # Use title if description is not available
        subject = data.get('dataFieldName', '')
        source_department = data.get('resOrgName', '')
        release_time = data.get('createTime', '')
        update_time = data.get('dataUpdateTime', '')
        open_conditions = "无条件开放" if data.get('openCondition', '') == '无' else "有条件开放"
        data_volume = data.get('dataSize', None)

        # Determine if the resource is an API based on resource type
        is_api = 'True' if data.get('resResourceType', '') == '1' else 'False'  # Assuming '1' indicates an API type
        file_types = []

        access_count = data.get('accessNum', None)
        download_count = data.get('downNum', None)
        api_call_count = None  # Not provided in JSON
        link = url  # Assume no direct link provided

        update_cycle = self.format_update_cycle(data.get('resUpdateCycle', ''))

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data.strip('"'))
        # 解析数据
        data = json_data['data']

        # 提取基本信息
        title = data.get('apiName', '')
        description = data.get('serviceDescription', '') if data.get('serviceDescription') else '无描述信息'
        subject = data.get('dataFieldName', '')
        source_department = data.get('resOrgName', '')
        release_time = data.get('createTime', '')
        update_time = data.get('dataUpdateTime', '')
        open_conditions = "有条件开放" if data.get('resShareType', '') == '402882a75885fd150158860e3d170006' else "无条件开放"
        data_volume = data.get('dataSize', None)

        # 确定资源是否为API，假设'local'类型为API
        is_api = 'True' if data.get('serviceApiType', '') == 'local' else 'False'

        # 没有具体的文件类型信息，所以留空数组
        file_type = ['接口']

        # 访问和下载次数
        access_count = data.get('accessNum', None)
        download_count = data.get('downNum', None)

        # API调用次数，默认没有则设为0
        api_call_count = data.get('callNum', None) if data.get('callNum') else None

        # 服务的完整路径或链接
        link = data.get('servicePath', '')

        # 更新周期，以数字表示，这里可以转换为更具体的描述
        update_cycle = self.format_update_cycle(data.get('resUpdateCycle', ''))

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_type, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

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
            1: '实时',
            2: '每日',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
            8: '其他',
            9: '长期'
        }

        # 处理空字符串或长度为0的情况
        if it == '' or (isinstance(it, str) and len(it) == 0) or it is None:
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
    page = JianCrawler(is_headless=True)
    page.run()
