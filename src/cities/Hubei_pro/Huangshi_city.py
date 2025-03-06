import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class HuangshiCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '黄石市',
                     'province': 'Hubei',
                     'total_items_num': 662 + 174,
                     'each_page_count': 100,
                     'base_url': 'http://data.huangshi.gov.cn/myapi/Datainfo/index?departmentid=&lyid=&bshy=&dataifa=0&page={page_num}'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"GET /myapi/Datainfo/index?departmentid=&lyid=&bshy=&dataifa=0&d=2024-07-07T15":"41:45.164Z HTTP/1.1","Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Cookie":"PHPSESSID=0e9555b8da34abeaee1cc1272340739f","Host":"data.huangshi.gov.cn","Proxy-Connection":"keep-alive","Referer":"http://data.huangshi.gov.cn/html/","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}
        self.params = '{"pageNo":2,"pageSize":10,"zylx":["01","02","03"],"ssbmId":["00"],"mlmc":"","gxsjPx":2,"llslPx":"","sqslPx":"","ssztlmId":["00"],"ssjclmId":["00"],"ssqtlmId":["00"],"kflx":["00"]}'
        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):

            url = self.base_url.format(page_num=i)
            session.get(url=url, headers=self.headers, proxies=self.proxies, verify=False)

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

        headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
                   "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Connection": "keep-alive",
                   "Cookie": "PHPSESSID=0c6af615c9b9ce5a34875f502db6bc7c", "Host": "data.huangshi.gov.cn",
                   "Referer": "http://data.huangshi.gov.cn/html/",
                   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}

        for item in urls_list:
            url = f'http://data.huangshi.gov.cn/myapi/Datainfo/view?infoid={item.get("rowGuid")}'

            session_page.get(url=url, headers=headers, proxies=self.proxies, verify=False)

            item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results['list']:
            # 提取和构建所需信息
            rowGuid = item.get('infoid', '')
            id_lists.append({
                'rowGuid': rowGuid,
            })

        return id_lists

    def extract_page_data(self, json_data, url):
        item = json.loads(json_data).get('data')  # 更新数据路径到单个数据对象

        data = item

        title = data.get('title', '')
        description = data.get('summary', '')
        source_department = data.get('departmentname', '')
        subject = self.get_name_by_id(data.get('bshy', ''))

        release_time = datetime.utcfromtimestamp(data.get('c_date')).strftime('%Y-%m-%d') if data.get(
            'c_date') else None
        update_time = release_time

        open_conditions = "无条件开放" if data.get('dataopen', '') == 1 else "有条件开放"
        data_volume = data.get('datanum', None)  # 使用datanum作为数据量
        is_api = 'True' if data.get('dataifa') == 1 else 'False'  # 根据dataifa判断是否API

        access_count = data.get('dls', None)
        download_count = data.get('fws', None)
        api_call_count = None  # 无API调用次数信息
        file_types = [data.get('datafmt', '')]  # 使用fjhzm作为文件类型

        link = url  # 使用datauri作为链接，如果有的话
        update_cycle = data.get('cycle', '')  # 使用cycle作为更新周期

        model = DataModel(title, subject, description, source_department, release_time, update_time,
                          open_conditions, data_volume, is_api, file_types, access_count, download_count,
                          api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, json_data):
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
            data_volume = 0
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

    @staticmethod
    def get_name_by_id(id):
        """
        根据提供的id返回对应的name。

        参数:
        data (list): 包含字典的列表，每个字典包含'id'和'name'键。
        id (int): 要查找的id值。

        返回:
        str: 对应的name，如果id不存在则返回None。
        """
        data = [
            {"id": 1, "name": "信用服务", "value": 2},
            {"id": 2, "name": "医疗卫生", "value": 2},
            {"id": 3, "name": "社保就业", "value": 4},
            {"id": 4, "name": "公共安全", "value": 1},
            {"id": 5, "name": "城建住房", "value": 2},
            {"id": 6, "name": "交通运输", "value": 4},
            {"id": 7, "name": "教育文化", "value": 4},
            {"id": 8, "name": "科技创新", "value": 0},
            {"id": 9, "name": "资源能源", "value": 0},
            {"id": 10, "name": "生态环境", "value": 1},
            {"id": 11, "name": "工业农业", "value": 0},
            {"id": 12, "name": "商贸流通", "value": 2},
            {"id": 13, "name": "财税金融", "value": 0},
            {"id": 14, "name": "安全生产", "value": 1},
            {"id": 15, "name": "市场监督", "value": 1},
            {"id": 16, "name": "社会救助", "value": 3},
            {"id": 17, "name": "法律服务", "value": 4},
            {"id": 18, "name": "生活服务", "value": 6},
            {"id": 19, "name": "气象服务", "value": 1},
            {"id": 20, "name": "地理空间", "value": 0},
            {"id": 21, "name": "机构团体", "value": 0},
            {"id": 22, "name": "其他", "value": 0}
        ]
        for item in data:
            if item['id'] == id:
                return item['name']
        return None


if __name__ == '__main__':
    page = HuangshiCrawler(is_headless=True)
    page.run()
