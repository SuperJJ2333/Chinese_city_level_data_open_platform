import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class FuzhouCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '抚州市',
                     'province': 'Jiangxi',
                     'total_items_num': 1139 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://data.jxfz.gov.cn/openapi/catalog/portal/catalog/catalogs/getCatalogByPage'}

        api_city_info = {'name': '抚州市_api',
                         'province': 'Jiangxi',
                         'total_items_num': 30 + 10,
                         'each_page_count': 10,
                         'base_url': 'https://data.jxfz.gov.cn/openapi/catalog/portal/catalog/catalogs/getCatalogByPage',
                         'is_api': 'True'
                         }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        if self.is_api:
            self.headers = {"Host":"data.jxfz.gov.cn","Connection":"keep-alive","Content-Length":"200","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","X-XSRF-TOKEN":"01290b2e-459e-4053-972d-247c2bd81569","sec-ch-ua-mobile":"?0","Authorization":"algorithm=HMAC-SHA256,appId=C20221025110751,time=1720458613350,sign=Cyh1ABLFFyH0edkxc9j/SYT20U1Jt9rokf7mfu2E4tw=","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","Content-Type":"application/json","Accept":"application/json, text/plain, */*","sec-ch-ua-platform":"\"Windows\"","Origin":"https://data.jxfz.gov.cn","Sec-Fetch-Site":"same-origin","Sec-Fetch-Mode":"cors","Sec-Fetch-Dest":"empty","Referer":"https://data.jxfz.gov.cn/interfaceServices?categoryDetailId=531&activeId=531","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Cookie":"mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; XSRF-TOKEN=01290b2e-459e-4053-972d-247c2bd81569; vuex=%7B%22token%22%3A%22%22%2C%22routerIndex%22%3A2%2C%22navIndex%22%3A0%2C%22dataTotal%22%3A3885853453%2C%22indexCount%22%3A1570%2C%22searchValue%22%3A%22%22%2C%22categoryDetailId%22%3A%22531%22%2C%22activeId%22%3A%22531%22%2C%22unReadQues%22%3A0%7D"}
            self.params = '{"catalogTitle":"","categoryDetailId":"531","pageNum":4,"pageSize":3,"orderName":"","orderSort":"","dictRefList":[{"value":"1","dictRef":"openLimit"},{"value":"API","dictRef":"physicalResourceType"}]}'

        else:
            self.headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
                            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                            "Authorization": "algorithm=HMAC-SHA256,appId=C20221025110751,time=1720457804637,sign=OM5JZ59/VxQ7tvLGHw3Y5dwSs7iaxhdBrx7ghjZweGY=",
                            "Connection": "keep-alive", "Content-Length": "201", "Content-Type": "application/json",
                            "Cookie": "mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; XSRF-TOKEN=01290b2e-459e-4053-972d-247c2bd81569; vuex=%7B%22token%22%3A%22%22%2C%22routerIndex%22%3A1%2C%22navIndex%22%3A0%2C%22dataTotal%22%3A3885853453%2C%22indexCount%22%3A1570%2C%22searchValue%22%3A%22%22%2C%22categoryDetailId%22%3A%22531%22%2C%22activeId%22%3A%22531%22%2C%22unReadQues%22%3A0%7D",
                            "Host": "data.jxfz.gov.cn", "Origin": "https://data.jxfz.gov.cn",
                            "Referer": "https://data.jxfz.gov.cn/openDate?categoryDetailId=531&activeId=531",
                            "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                            "X-XSRF-TOKEN": "01290b2e-459e-4053-972d-247c2bd81569",
                            "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

            self.params = '{"catalogTitle":"","categoryDetailId":"531","pageNum":4,"pageSize":10,"orderName":"","orderSort":"","dictRefList":[{"value":"1","dictRef":"openLimit"},{"value":"FILE","dictRef":"physicalResourceType"}]}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNum'] = i

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.fiddler_proxies, json=self.params, verify=False)

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
                    # 处理API数据
                    page_data = self.extract_page_data(json_data)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, json_data):

        data_dict = json.loads(json_data)
        session_page = SessionPage()

        for item in data_dict['smcDataSetList']:
            url = f'https://data.zhihuichuzhou.com:8007/#/wdfwDetail?id=116982227351{item["createUser"]["id"]}'

            session_page.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page)

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

    def extract_page_data(self, json_data):
        data = json.loads(json_data)
        results = data['data']['result']  # 直接访问result数组
        models = []

        for item in results:
            title = item.get('catalogTitle', '')
            subject = item['dynamicCatalogFields'].get('relatedCategoryResources', '')
            description = item['dynamicCatalogFields'].get('catalogAbstract', '')
            source_department = item['dynamicCatalogFields'].get('provider', '')

            release_time = item['dynamicCatalogFields'].get('catalogCreateTime', '')
            update_time = item['dynamicCatalogFields'].get('updateTime', '')
            open_conditions = item['dynamicCatalogFields'].get('accessLimit', '')

            data_volume = item['statisticsModel'].get('view', None)
            is_api = 'True' if 'api' in [stype['value'] for stype in item['serviceTypes']] else 'False'
            file_type = [item['dynamicCatalogFields'].get('resourceParamType', '')]

            access_count = item['statisticsModel'].get('view', None)
            download_count = item['statisticsModel'].get('exchange', None)
            api_call_count = None  # 示例中没有API调用次数，因此设为0
            link = f'https://data.jxfz.gov.cn/openDate/directory/{item["id"]}'  # 未指定链接，保留为空
            update_cycle = item['dynamicCatalogFields'].get('updateCycle', '')

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        json_data = json.loads(json_data.strip('"'))
        results = json_data['data']['result']  # 根据新的数据路径调整
        models = []

        for item in results:
            title = item['dynamicCatalogFields'].get('catalogTitle', '')
            subject = item['dynamicCatalogFields'].get('relatedCategoryResources', '')
            description = item['dynamicCatalogFields'].get('catalogAbstract', '')
            source_department = item['dynamicCatalogFields'].get('provider', '')

            release_time = item['dynamicCatalogFields'].get('catalogCreateTime', '')
            update_time = item['dynamicCatalogFields'].get('updateTime', '')
            open_conditions = item['dynamicCatalogFields'].get('accessLimit', '')

            data_volume = item['statisticsModel'].get('view', None)
            is_api = 'True' if any(st['value'] == 'INTERFACE_SERVICE' for st in item['serviceTypes']) else 'False'
            file_type = [item['dynamicCatalogFields'].get('resourceParamType', '')]

            access_count = item['statisticsModel'].get('view', None)
            download_count = item['statisticsModel'].get('exchange', None)
            api_call_count = None  # 示例中没有API调用次数，因此设为0
            link = f'https://data.jxfz.gov.cn/openDate/directory/{item["id"]}'  # 未指定链接，保留为空
            update_cycle = item['dynamicCatalogFields'].get('updateCycle', '')

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
    page = FuzhouCrawler(is_headless=True)
    page.run()
