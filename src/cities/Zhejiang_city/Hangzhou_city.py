import concurrent
import json
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class HangzhouCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '杭州市',
                     'province': 'Zhejiang',
                     'total_items_num': 3419 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://data.hangzhou.gov.cn/dop/tpl/dataOpen/dataList.html?source_type=DATA'
                     }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Cache-Control":"max-age=0","Connection":"keep-alive","Cookie":"JSESSIONID=5EA55BA74FA17A648F08083580137D64; Hm_lvt_b322ec67712b87bd0916b9d26a11877a=1719307614; JSESSIONID=D0C25E346712DF82BCCC9668CE415445; Hm_lpvt_b322ec67712b87bd0916b9d26a11877a=1719319697","Host":"data.hangzhou.gov.cn","Referer":"https://data.hangzhou.gov.cn/dop/tpl/dataOpen/dataCataLogList.html?source_type=openCatalog","Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate","Sec-Fetch-Site":"cross-site","Sec-Fetch-User":"?1","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.params = {
            "source_id": "",
            "fieldCode": "",
            "themeCode": "",
            "serviceCode": "",
            "industryCode": "",
            "score": "",
            "ability": "",
            "source_type": "DATA",
            "openLevel": "",
            "searchKey": "",
            "sort": "",
            "code": "",
            "pageSplit": {
                "pageNumber": 1,
                "pageSize": 10
            },
            "topicType": ""
        }

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.page
        session.listen.start('list.action')

        url = self.base_url
        session.get(url=url)

        for i in range(1, self.total_page_num):

            while True:
                try:
                    json_data = session.listen.wait()
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                # 提取API数据
                urls_list = self.extract_data(json_data.response.body)
                # 处理API数据
                page_data = self.process_multi_pages(urls_list)
                views_list.extend(page_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

                session.listen.start('list.action')
                try:
                    next_page_button = session.ele('x://*[@class="btn-next"]')
                    next_page_button.click()
                    time.sleep(1)
                except Exception as e:
                    self.logger.warning(f'第{i}页 - 点击下一页按钮失败 - {e}')
                    break

        return views_list

    def process_page(self, urls_list):

        session_page = self.page.new_tab()

        items_list = []
        session_page.listen.start('dataDetail.action')

        for item in urls_list:
            url = f'https://data.hangzhou.gov.cn/dop/tpl/dataOpen/dataDetail.html?source_id={item.get("rowGuid")}&source_type=DATA&source_type_str=A&version=1&source_code={item.get("resource_id")}'

            session_page.get(url=url)

            res = session_page.listen.wait()

            item_data = self.extract_page_data(res.response.body, url)

            items_list.append(item_data)

        session_page.close()

        return items_list

    def process_multi_pages(self, urls_list):
        items_list = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_item = {executor.submit(self.process_single_page, item): item for item in urls_list}

            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    item_data = future.result()
                    items_list.append(item_data)
                except Exception as e:
                    self.logger.error(f'处理单页数据失败 - {e}')

        return items_list

    def process_single_page(self, item):
        session_page = self.page.new_tab()

        session_page.listen.start('dataDetail.action')

        url = f'https://data.hangzhou.gov.cn/dop/tpl/dataOpen/dataDetail.html?source_id={item.get("rowGuid")}&source_type=DATA&source_type_str=A&version=1&source_code={item.get("resource_id")}'

        session_page.get(url=url)

        res = session_page.listen.wait()

        item_data = self.extract_page_data(res.response.body, url)

        session_page.close()

        return item_data


    @staticmethod
    def extract_data(json_data):

        data = json_data

        # 假设所有相关信息都在 'result' 键中
        results = data.get('rows', [])
        id_lists = []

        for item in results:
            # 提取和构建所需信息
            rowGuid = item.get('id', '')
            resource_id = item.get('source_code', '')
            data_amount = item.get('openresdataCount', '')
            id_lists.append({
                'rowGuid': rowGuid,
                'data_amount': data_amount,
                'resource_id': resource_id
            })

        return id_lists

    def extract_page_data(self, json_data, upper_item):
        data = json_data
        # 从 JSON 数据中提取资源信
        item = data['resInfo']

        # 从项目中提取具体数据
        title = item.get('source_name', '')
        source_department = item.get('private_dept', '')
        description = item.get('instruction', '')
        subject = item.get('type_name', '')

        # 处理时间数据
        release_time = datetime.strptime(item['create_date'], '%Y-%m-%d %H:%M:%S') \
            if item.get('modify_date') else None
        update_time = datetime.strptime(item['modify_date'], '%Y-%m-%d %H:%M:%S') \
            if item.get('modify_date') else None
        update_cycle = item.get('updateCycle', '')

        # 其他统计数据
        access_count = item.get('source_views', None)
        download_count = item.get('source_download_count', None)
        api_call_count = item.get('source_apply_count', None)

        # 数据类别处理
        open_conditions = item.get('openCondition', '')
        file_types = [item.get('data_type', '')]
        data_volume = item.get('data_count', None)
        link = upper_item
        is_api = 'False' if item.get('source_apply_count', '') is None else 'True'

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

            access_count = item.get('fws', 0)
            download_count = 0
            api_call_count = item.get('xzs', 0)  # Assuming 0 since not provided
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
        encoded_data = "postData=" + urllib.parse.quote(encoded_data)

        return encoded_data


if __name__ == '__main__':
    page = HangzhouCrawler(is_headless=True)
    page.run()
