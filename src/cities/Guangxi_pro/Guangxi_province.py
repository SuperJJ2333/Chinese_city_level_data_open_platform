import json
import math
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuangxiCrawler(PageBase):
    """
    post请求获取api数据
    二级跳转：在详情页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': 'Guangxi_province',
                     'province': 'Guangxi',
                     'total_items_num': 326 + 500,
                     'each_page_count': 10,
                     'base_url': 'http://data.ahzwfw.gov.cn:8000/dataopen-web/data/findByPage'}

        super().__init__(city_info, is_headless)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
        }

        self.city_types = {
            "南宁市": "https://nn.data.gxzf.gov.cn/nn/api/",
            "柳州市": "https://lz.data.gxzf.gov.cn/lz/api/",
            "桂林市": "https://gl.data.gxzf.gov.cn/gl/api/",
            "梧州市": "https://wz.data.gxzf.gov.cn/wz/api/",
            "北海市": "https://bh.data.gxzf.gov.cn/bh/api/",
            "防城港市": "https://fcg.data.gxzf.gov.cn/fcg/api/",
            "钦州市": "https://qz.data.gxzf.gov.cn/qz/api/",
            "贵港市": "https://gg.data.gxzf.gov.cn/gg/api/",
            "玉林市": "https://yl.data.gxzf.gov.cn/yl/api/",
            "百色市": "https://bs.data.gxzf.gov.cn/bs/api/",
            "贺州市": "https://hz.data.gxzf.gov.cn/hz/api/",
            "河池市": "https://hc.data.gxzf.gov.cn/hc/api/",
            "来宾市": "https://lb.data.gxzf.gov.cn/lb/api/",
            "崇左市": "https://cz.data.gxzf.gov.cn/cz/api/"
        }

        self.params = {'pageSize': '10', 'pageNum': '1', 'xzqh': '3405'}

    def run(self):
        """
        爬虫入口函数
        """
        # 遍历城市list
        for city_name, city_url in self.city_types.items():
            # 获取城市名称
            self.name = city_name + '_api'
            # 获取总item数
            self.session.get(url=city_url, headers=self.headers, proxies=self.proxies)
            self.total_items_num = int(self.session.ele('x://*[@id="record_count"]/span').text)
            # 计算页数
            self.total_page_num = math.ceil(self.total_items_num / self.each_page_num) + 1

            self.base_url = city_url

            self.total_data = self.process_views()
            self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url + f'index?page={i}'
            response.get(url=url, headers=self.headers, proxies=self.proxies)

            while True:
                try:
                    json_data = response.json if response.json else response.html
                    break
                except Exception as e:
                    time.sleep(1)
                    # self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                # extracted_data = self.extract_data(json_data)
                detailed_data = self.extract_page_data(response)
                views_list.extend(detailed_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_detailed_page(self, views_list):

        session = SessionPage()
        detailed_url = 'http://data.ahzwfw.gov.cn:8000/dataopen-web/data/findByRid'
        params = {'rid': '00821a44dc5a451c99778ca55c74fa8b'}
        data_list = []

        for item in views_list:
            params['rid'] = item
            session.post(url=detailed_url, headers=self.headers, data=params, proxies=self.proxies)

            json_data = session.response.text
            if json_data:
                extracted_data = self.extract_page_data(json_data)
                data_list.append(extracted_data)

        return data_list

    def extract_data(self, json_data):

        data_dict = json_data
        info_list = []

        # 检查内容是否存在
        if data_dict['data']['rows']:
            # 遍历每个数据项，提取数据集名称和文件类型
            for item in data_dict['data']['rows']:
                # self.logger.info(f"爬取的id为：{item['rid']}")
                info_list.append(item['rid'])

        return info_list

    def extract_page_data(self, session_page):
        # 假设session_page是一个加载了HTML内容的lxml的HTML元素对象
        models_list = []

        entries_list = session_page.eles('x://*[@id="app"]/div[8]/div[2]/div[3]/ul/li')

        for item in entries_list:
            title = item.ele('x://div/a').text
            description = item.ele('@class=describe').text.split('：')[-1].strip()
            source_department = item.ele('x:/div[3]/div[1]/span[2]').text
            subject = item.ele('x://div[3]/div[2]/span[2]').text

            release_time = item.ele('x://div[3]/div[3]/span[2]').text
            update_time = item.ele('x://div[3]/div[4]/span[2]').text

            open_conditions = item.ele('x://div[3]/div[5]/span[2]').text
            file_type = [file.text for file in item.eles('x://div[4]/div[1]/ul/li')]
            is_api = 'True' if '接口' in file_type else 'False'
            data_volume = None  # 假设数据量未提供

            access_count = item.ele('x://div[4]/div[2]').text.strip().replace('次', '').replace(',', '')
            download_count = None  # 假设下载次数未提供
            api_call_count = None  # 假设API调用次数未提供
            link = item.ele('x://div/a').attr('href')

            update_cycle = ''  # 假设更新周期未提供

            # 创建DataModel实例
            model = DataModel(
                title, subject, description, source_department, release_time,
                update_time, open_conditions, data_volume, is_api, file_type,
                int(access_count), download_count, api_call_count, link, update_cycle,
                location=self.name
            )
            models_list.append(model.to_dict())

        return models_list

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
    page = GuangxiCrawler(is_headless=True)
    page.run()
