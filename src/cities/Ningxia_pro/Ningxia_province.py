import json
import math
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class NingxiaCrawler(PageBase):
    """
    post请求获取api数据
    二级跳转：在详情页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '宁夏省',
                     'province': 'Ningxia',
                     'total_items_num': 326 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://opendata.nx.gov.cn/portal/catalog/index?filterParam=region_code&filterParamCode={city_code}&page={page_num}'}

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"OPENSESSION=75f34ac8-8bf6-4784-87dd-b02052dcc983; __utrace=f28d520bdde711de3fa4277dd5b70c36","Host":"opendata.nx.gov.cn","Referer":"https://opendata.nx.gov.cn/portal/catalog/index?filterParam=region_code&filterParamCode=6402&page=1","Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate","Sec-Fetch-Site":"same-origin","Sec-Fetch-User":"?1","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.city_type = {'宁夏省': ['0000', 2001], '石嘴山市': [6402, 78], '吴忠市': [6403, 88]}

        self.params = {'pageSize': '10', 'pageNum': '1', 'xzqh': '3405'}

    def run(self):
        """
        爬虫入口函数
        """
        # 遍历城市list
        for city_name, city_item in self.city_type.items():
            # 获取城市名称
            self.name = city_name
            # 获取总item数
            self.total_items_num = city_item[1]
            # 计算页数
            self.total_page_num = self.count_page_num()

            if city_item[0] == '0000':
                self.base_url = 'https://opendata.nx.gov.cn/portal/catalog/index?page={page_num}'
            else:
                self.base_url = self.base_url.format(city_code=city_item[0], page_num='{page_num}')

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url.format(page_num=i)
            session.get(url=url, proxies=self.proxies, headers=self.headers)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.response.content
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}/{self.total_page_num}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                page_data = self.extract_data(session, i)
                detailed_data = self.process_page(page_data)
                views_list.extend(detailed_data)
                self.logger.info(f'{self.name} - 第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page, item['file_type'])
            except Exception as e:
                self.logger.warning(f'{item["url"]} - 解析数据失败 - {e}')
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://*[@id="app"]/div[6]/div[2]/div[3]/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'https://opendata.nx.gov.cn{frame_url}'
                else:
                    frame_url = frame_url

                file_type = [file.text.lower() for file in frame.eles('x://div[4]/div[3]/ul/li')]

                data_list.append({
                    'url': frame_url,
                    'file_type': file_type
                })
            except Exception as e:
                self.logger.warning(f'第{page_num}页 - 解析数据失败 - {e}')
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):

        frame = session_page.ele('x://*[@id="app"]/div[6]/div[2]/div/ul/li[1]/table')

        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://*[@id="app"]/div[6]/div[1]/div/div/div/div[1]/ul[1]/li/h4').text
        subject = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[2]/span').text
        description = frame.ele('x://tr[7]/td[2]').text
        source_department = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[1]/span').text

        release_time = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[3]/span').text
        update_time = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[4]/span').text

        open_conditions = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[5]/span').text
        data_volume = frame.ele('x://tr[1]/td[4]').text
        file_type = files_type
        is_api = 'True' if '接口' in file_type else 'False'

        access_count = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[6]').text
        download_count = session_page.ele(
            'x://*[@id="app"]/div[6]/div[1]/div/div/div/div[2]/ul/li[7]').text
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = frame.ele('x://tr[4]/td[2]').text

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    @staticmethod
    def extract_api_page_data(json_data):
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
    page = NingxiaCrawler(is_headless=True)
    page.run()
