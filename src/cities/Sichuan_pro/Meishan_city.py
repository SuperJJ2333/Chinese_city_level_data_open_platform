import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class MeishanCrawler(PageBase):
    """
    post：请求获取api数据
    直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '眉山市',
                     'province': 'Sichuan',
                     'total_items_num': 9385 + 1000,
                     'each_page_count': 10,
                     'base_url': 'http://data.ms.gov.cn/portal/catalog/getPage'
                     }

        api_city_info = {'name': '眉山市_api',
                         'province': 'Sichuan',
                         'total_items_num': 4515 + 1000,
                         'each_page_count': 10,
                         'base_url': 'http://data.ms.gov.cn/portal/dataservice/getPage',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        if not self.is_api:
            self.headers = {"Accept":"*/*","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"77","Content-Type":"application/x-www-form-urlencoded; charset=UTF-8","Cookie":"_gscu_120503026=19299350e4c34m27; Hm_lvt_ce25b27836955eea968bfeefa0185fef=1720173505; _gscu_877711934=20173506y4pzoj15; JSESSIONID=2EAC31ADE99D156FCB0FBC76BAEFC90E; _gscbrs_120503026=1; _gscs_120503026=20516826px4tyu27|pv:6","Host":"data.ms.gov.cn","Origin":"http://data.ms.gov.cn","Referer":"http://data.ms.gov.cn/portal/catalog?orgId=","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest"}

            self.params = {'codeId': '29001fffc4854893b4b3035f930624ff', 'searchOrder': '0', 'page': '2', 'rows': '10', 'organId': ''}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['page'] = str(i)
            session.post(url=url, headers=self.headers, proxies=self.proxies, data=self.params)

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
                    page_data = self.extract_page_data(json_data)
                views_list.extend(page_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, json_data):

        data_dict = json.loads(json_data)
        session_page = SessionPage()

        for item in data_dict['smcDataSetList']:
            url = f'https://data.zhihuichuzhou.com:8007/#/wdfwDetail?id=116982227351{item["createUser"]["id"]}'

            session_page.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page)

    def extract_data(self, link):

        session = SessionPage()
        url = link

        session.get(url=url, proxies=self.proxies)

        update_cycle = session.ele('x://*[@id="tabLink2"]/div[2]/div/table/tbody/tr[2]/td[6]').text

        return update_cycle

    def extract_page_data(self, json_data):
        data = json.loads(json_data)
        results = data['rows']
        # 调整数据路径以匹配提供的JSON结构
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('industryType', '')
            description = item.get('des', '')
            source_department = item.get('orgName', '')

            release_time = item.get('publicTime', '')
            update_time = item.get('updateTime', '')

            open_conditions = item.get('shareCondition', '')
            data_volume = item.get('resourceMount', None)
            is_api = 'False'  # 由于数据未指定API访问，统一设为'False'
            file_type = item.get('resourceTypeText', '').split(',')

            access_count = item.get('pageViewCount', None)
            download_count = item.get('pageDownCount', None)
            api_call_count = item.get('serviceCount', None)
            link = f'http://data.ms.gov.cn/portal/catalog_detail?id={item["id"]}&resourceSource=0&categoryCodeId={item["codeId"]}&orgId=&serviceType=0,1,3,4'  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('shareWay', ''))

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data)

        # 假设所有相关信息都在 'smcDataSetList' 键中
        results = json_data['result']['rows']  # 更新数据路径以匹配新的JSON结构
        models = []

        for item in results:
            title = item.get('serviceName', '')
            subject = item.get('industryName', '')
            description = item.get('des', '')
            source_department = item.get('orgName', '')

            release_time = item.get('publishTime', '')
            update_time = item.get('updateTime', '') if item.get('updateTime') else release_time  # 使用发布时间作为默认更新时间

            open_conditions = "无条件开放" if item.get('openType', '') == "0" else "有条件开放"  # 使用openType字段表示开放条件
            data_volume = item.get('resourceMount', None)
            is_api = 'True' if item.get('serviceType', '') == '1' else 'False'  # 假设serviceType为'1'表示有API
            file_type = [item.get('serviceTypeText', '').split(',')]

            access_count = item.get('serviceViewCount', None)
            download_count = item.get('collectNum', None)
            api_call_count = item.get('serviceApplyCount', None)
            link = f'http://data.ms.gov.cn/portal/service_detail?id={item["id"]}&type=openapi&orgId='
            update_cycle = self.extract_data(link)

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle,
                              self.name)
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
            1: '每年',
            2: '每日',
            3: '每年',
            4: '每季度',
            5: '每半年',
            7: '每年',
            6: '自定义',
            8: '实时'
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
    page = MeishanCrawler(is_headless=True)
    page.run()
