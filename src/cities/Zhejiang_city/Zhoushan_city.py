import json
import re
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from common.form_utils import convert_timestamp_to_date
from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZhoushanCrawler(PageBase):
    """
    post：请求获取api数据
    直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '舟山市',
                     'province': 'Zhejiang',
                     'total_items_num': 662 + 200,
                     'each_page_count': 10,
                     'base_url': 'http://data.zhoushan.gov.cn:9966/catalog/dataOpen/otherPage'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Content-Length":"124","Content-Type":"application/json; charset=UTF-8","Host":"data.zhoushan.gov.cn:9966","Origin":"http://data.zhoushan.gov.cn","Proxy-Connection":"keep-alive","Referer":"http://data.zhoushan.gov.cn/","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}

        self.params = '{"pageNo":2,"pageSize":10,"param":{"resName":"","datadomain":"","idPoc":null,"keywords":"","areaCode":null,"capacity":null}}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['pageNo'] = str(i)
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=self.params)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
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
        results = data.get('data', {}).get('records', [])  # 调整数据路径为新的结构
        models = []

        for item in results:
            title = item.get('resName', '')  # 根据新的数据结构调整字段获取方式
            subject = self.get_name_by_id(item.get('datadomain', ''))  # 新的数据中没有主题名称，保留为空字符串
            description = item.get('resSummary', '')
            source_department = item.get('idPocName', '')

            release_time = convert_timestamp_to_date(item.get('releaseDate', None))  # 新数据中没有发布时间字段，设为None
            update_time = convert_timestamp_to_date(item.get('updateDate', None))

            open_conditions = "无条件开放" if item.get('isopen') == '1' else "有条件开放"  # 新数据中没有开放条件字段，设为'无条件开放'
            data_volume = None

            is_api = 'True'  # 根据是否有apiId判断是否为API数据
            file_type = ['XLS', 'CSV', 'XML', 'JSON', 'RDF']  # 新的数据结构中没有文件类型字段

            access_count = item.get('visitNum', None)
            download_count = item.get('downloadNum', None)
            api_call_count = None

            link = f'http://data.zhoushan.gov.cn/#/OpenData/DataSet/Detail?id={item["id"]}&isStructuredData=1'  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('frequery', ''))

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    @staticmethod
    def extract_api_page_data(self, json_data):
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
            0: '可交换',
            1: '不可交换',
            2: '每周',
            3: '每月',
            4: '每季度',
            5: '每半年',
            6: '每年',
            7: '不定期',
            8: '不更新'
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
    def get_name_by_id(input_id):
        data_list = [
            {"name": "信用服务", "id": "1"},
            {"name": "医疗卫生", "id": "2"},
            {"name": "社保就业", "id": "3"},
            {"name": "公共安全", "id": "4"},
            {"name": "城建住房", "id": "5"},
            {"name": "交通运输", "id": "6"},
            {"name": "教育文化", "id": "7"},
            {"name": "科技创新", "id": "8"},
            {"name": "资源能源", "id": "9"},
            {"name": "生态环境", "id": "10"},
            {"name": "工业农业", "id": "11"},
            {"name": "商贸流通", "id": "12"},
            {"name": "财税金融", "id": "13"},
            {"name": "安全生产", "id": "14"},
            {"name": "市场监督", "id": "15"},
            {"name": "社会救助", "id": "16"},
            {"name": "法律服务", "id": "17"},
            {"name": "生活服务", "id": "18"},
            {"name": "气象服务", "id": "19"},
            {"name": "地理空间", "id": "20"},
            {"name": "机构团体", "id": "21"},
            {"name": "其他", "id": "99"},]

        for item in data_list:
            if item["id"] == str(input_id):
                return item["name"]
        return "ID不存在"


if __name__ == '__main__':
    page = ZhoushanCrawler(is_headless=True)
    page.run()