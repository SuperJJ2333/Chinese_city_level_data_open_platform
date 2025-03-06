import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class SanyaCrawler(PageBase):
    """
    post 请求获取api数据
    一级跳转：post：在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '三亚市',
                     'province': 'Hainan',
                     'total_items_num': 257 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://api.sanya.gov.cn/wl/home/opendata/GetDatasetList'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"APP_ID":"bj9pl7nrles8b9l3lp9g","Accept":"*/*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"92","Content-Type":"application/json","Host":"api.sanya.gov.cn","Origin":"https://dataopen1.sanya.gov.cn","Referer":"https://dataopen1.sanya.gov.cn/","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-site","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","authorization":"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBJZCI6ImM2a3AyNDc1azV2ZWFxc2ViY2kwIiwidWlkIjoiODg4ODg4ODg4ODg4ODg4ODg4ODgiLCJvcGVuSWQiOiI4ODg4ODg4ODg4ODg4ODg4ODg4OCIsInJ1blRpbWUiOiIiLCJleHAiOjE3MjAzNzA0MDN9.OF_MEYEnipH0vDDPtAzgFXkKvDqNkyy6SpdYwIlliAo","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}
        self.params = '{"page":3,"page_size":10,"department_id":"","theme_id":"","tag":"","format":"","keyword":""}'

        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['page'] = i
            response.post(url=self.base_url, headers=self.headers, json=self.params, proxies=self.proxies)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                extracted_data = self.extract_page_data(json_data)
                views_list.extend(extracted_data)
                self.logger.info(
                    f'{self.name} - 第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, resId):
        '''
        1. 发送post请求，获取api数据
        2. 解析api数据，获取开放条件和数据量
        :param resId:
        :return:
        '''

        url = 'https://data.guizhou.gov.cn/api/search/data/getDataDetailByDataId'

        params = '{"id":"88a912d8-bd97-4900-aee2-87929ba23a58"}'

        params = json.loads(params)
        params['id'] = resId
        session = SessionPage()

        session.post(url=url, headers=self.headers, json=params, proxies=self.proxies)

        response = session.json

        json_data = response

        item = json_data['data']

        data_volume = item.get('dataCapacity', None)
        update_cycle = item.get('frequency', '')

        return {'data_volume': data_volume, 'update_cycle': update_cycle}

    def extract_page_data(self, json_data):
        data = json_data
        results = data['paginator']['records']  # 根据新的数据路径调整
        models = []

        for item in results:
            # 提取数据项
            title = item.get('name', '')
            description = item.get('description', '')
            subject = ''
            source_department = item.get('department_name', '')

            release_time = datetime.strptime(item.get('public_time', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') \
                if item.get('public_time') else ''
            update_time = datetime.strptime(item.get('updated_date', ''), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') \
                if item.get('updated_date') else ''

            open_conditions = "无条件开放" if item.get('status', '') == 2 else "有条件开放"
            data_volume = None

            link = f'https://dataopen1.sanya.gov.cn/#/elmDataList?id={item.get("id")}={title}'
            # 其他信息提取
            file_type = [item.get('formats', '')]  # 假设formats字段直接提供了文件格式
            is_api = 'False' if 'json' not in file_type else 'True'

            access_count = item.get('view_count', None)
            download_count = item.get('download_count', None)
            api_call_count = item.get('tracking_count', None) # 假设API调用次数未提供

            location = self.name
            update_cycle = ''

            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, location)
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
            9: "归档不更新",
            8: "不定期更新",
            7: "其他",
            6: "每半年",
            5: "实时",
            4: "每天",
            3: "每周",
            2: "每月",
            1: "每季度",
            0: "每年"
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
    page = SanyaCrawler(is_headless=True)
    page.run()
