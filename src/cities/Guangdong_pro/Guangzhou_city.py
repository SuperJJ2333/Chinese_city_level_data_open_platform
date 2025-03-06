import json
import time
from datetime import datetime

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class GuangzhouCrawler(PageBase):
    """
    post 请求获取api数据
    一级跳转：直接在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '广州市',
                     'province': 'Guangdong',
                     'total_items_num': 1480,
                     'each_page_count': 10,
                     'base_url': 'https://data.gz.gov.cn/data-source/dsDataCatalog/findDsDataTableExts'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                        "Content-Type": "application/json;charset=UTF-8",
                        "Cookie": "Hm_lvt_7ebd82d459427bd5de9c02ef0dfa5707=1717913329; Hm_lpvt_7ebd82d459427bd5de9c02ef0dfa5707=1717913965",
                        "Host": "data.gz.gov.cn", "Origin": "https://data.gz.gov.cn",
                        "Referer": "https://data.gz.gov.cn/dataSet.html", "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                        "X-Requested-With": "XMLHttpRequest",
                        "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = '{"body":{"orgCode":"","name":"","subjectId":null,"useType":null,"dataFormat":null,' \
                      '"openStatus":null,"display":"1"},"page":1,"pageSize":10,"sortName":"lastUpdateTime",' \
                      '"sortOrder":"desc"}'

        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        response = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNum'] = i
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
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def extract_page_data(self, json_data):
        data = json_data
        results = data['body']  # 修改了数据路径以匹配新的JSON结构
        models = []

        for item in results:
            location = self.name
            title = item.get('name', '')  # 使用'name'作为标题
            subject = item.get('subjectName', '')  # 使用'subjectName'作为主题
            description = item.get('description', '')  # 描述直接取自'description'
            source_department = item.get('orgName', '')  # 使用'orgName'作为来源部门

            # 处理日期格式，确保在日期为空时能处理异常
            release_time = item.get('created')
            if release_time:
                release_time = datetime.strptime(release_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

            update_time = item.get('lastUpdated')
            if update_time:
                update_time = datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

            open_conditions = '无条件开放' if item.get(
                'openStatus') == '1' else '有条件开放'  # 假设'openStatus' '1'是无条件开放
            data_volume = item.get('dataCount', None)
            is_api = 'True' if "3" in item.get('dataFormat') else 'False'
            file_type = [f['fileFormat'] for f in item.get('dataFileList', []) if 'fileFormat' in f]  # 列出所有文件类型

            access_count = item.get('viewCount', None)
            download_count = item.get('downloadCount', None)
            api_call_count = item.get('apiCallCount', None)
            link = f"https://data.gz.gov.cn/search.html?title={title}"  # 示例链接，实际情况可能需要根据具体情况调整
            update_cycle = self.format_update_cycle(item.get('updateCycle', ''))

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
            1: '不定期',
            2: '每天',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
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
    page = GuangzhouCrawler(is_headless=True)
    page.run()
