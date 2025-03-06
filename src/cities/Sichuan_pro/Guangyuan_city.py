import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuangyuanCrawler(PageBase):
    """
    get请求获取api数据
    二级跳转：get请求：获取XML数据，在详细页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '广元市',
                     'province': 'Sichuan',
                     'total_items_num': 8728 + 1000,
                     'each_page_count': 5,
                     'base_url': 'https://data.cngy.gov.cn/open/dataPool/dataPoolPager'}


        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json",
                        "Cookie": "JSESSIONID=DB454C84A0E507EAF868EA7056D96B60; Hm_lvt_42b2edcc86601c1425f1249e728bf7d8=1719068071; _gscu_683580420=1906807119iytj11; _gscbrs_683580420=1; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:0.9%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; Hm_lpvt_42b2edcc86601c1425f1249e728bf7d8=1719068078; _gscs_683580420=19068071tng7x411|pv:2",
                        "Host": "data.cngy.gov.cn", "Origin": "https://data.cngy.gov.cn",
                        "Referer": "https://data.cngy.gov.cn/open/index.html?id=datapool", "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = '{"id":"","pageSize":"5","currentPage":2,"type":"","btntype":"","fileid":"","share":"","userid":null,"sorter":"release_time","order":"desc"}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['currentPage'] = i

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=self.params)

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
                    page_data = self.process_page(json_data)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, json_data):

        data_dict = json.loads(json_data)
        session_page = SessionPage()

        items_list = []
        headers = {"Accept":"application/json, text/javascript, */*; q=0.01","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"23","Content-Type":"application/json","Cookie":"JSESSIONID=D3055E5020EB5D81B5923F38FA669DDE; Hm_lvt_42b2edcc86601c1425f1249e728bf7d8=1719068071; _gscu_683580420=1906807119iytj11; _gscbrs_683580420=1; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:0.9%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; Hm_lpvt_42b2edcc86601c1425f1249e728bf7d8=1719130251; _gscs_683580420=t1912942877r47l15|pv:4","Host":"data.cngy.gov.cn","Origin":"https://data.cngy.gov.cn","Referer":"https://data.cngy.gov.cn/open/index.html?id=user&messid=17145&type=%E6%96%87%E4%BB%B62","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        for item in data_dict['data']['rows']:
            url = f'https://data.cngy.gov.cn/open/details/queryDetails'
            params = '{"id":"74668","type":1}'
            params = json.loads(params)
            params['id'] = item['ID']

            session_page.post(url=url, headers=headers, proxies=self.proxies, json=params)

            item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

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

    def extract_page_data(self, json_data, url):
        data = json.loads(json_data)
        item = data['data']['details'][0]  # 根据新的数据路径调整

        # 提取数据，对于不存在的字段保留为空
        title = item.get('name', '')
        subject = '生活服务'
        description = item.get('remarks', '')
        source_department = item.get('department', '')

        # 时间处理，使用日期时间格式化
        release_time = item.get('release_time', '')
        update_time = item.get('update_time', '')

        # 其他属性处理
        open_conditions = '无条件开放'

        access_count = item.get('total', None)
        download_count = item.get('download_count', None)
        data_volume = item.get('data_volume', None)
        is_api = 'False'
        file_type = [item.get('resourcrtype', '')]
        api_call_count = None  # 示例中没有API调用次数，因此设为0
        link = url
        update_cycle = self.format_update_cycle(item.get('cycle', ''))

        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

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

            data_volume = item['statisticsModel'].get('view', 0)
            is_api = 'True' if any(st['value'] == 'INTERFACE_SERVICE' for st in item['serviceTypes']) else 'False'
            file_type = [item['dynamicCatalogFields'].get('resourceParamType', '')]

            access_count = item['statisticsModel'].get('view', 0)
            download_count = item['statisticsModel'].get('exchange', 0)
            api_call_count = 0  # 示例中没有API调用次数，因此设为0
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
            1: '其他',
            2: '每日',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
            8: '实时',
            9: '每两年'
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
    page = GuangyuanCrawler(is_headless=True)
    page.run()
