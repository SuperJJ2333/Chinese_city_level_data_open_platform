import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ChengdeCrawler(PageBase):
    """
    xpath: post获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:get 目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': 'Chengde',
                     'province': 'Hebei',
                     'total_items_num': 111 + 200,
                     'each_page_count': 10,
                     'base_url': 'https://www.chengde.gov.cn/shuju/web/queryDomain'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Cookie": "shiro.sesssion=179f5f62-b970-437a-a84d-fa16ebaeb092; zh_choose_undefined=s",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
        }

        self.params = {'page': '3', 'orgId': 'null', 'orgDirId': 'null', 'domainId': '0', 'resourceType': 'DATAQUERY', 'sortWord': 'modifyDate', 'direction': 'down', 'listPageNum': '3'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url
            self.params['page'] = str(i)
            session.post(url=url, proxies=self.proxies, headers=self.headers, data=self.params)

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
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page)
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://body/dl/dd')

        # 正则表达式模式匹配单引号内的内容
        pattern = r"dataserver\.view\('([^']+)'\)"

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div/h2/a').attr('href')

                # 使用re.search进行匹配
                match = re.search(pattern, frame_url)
                # 提取并输出匹配的内容
                if match:
                    frame_url = f'https://www.chengde.gov.cn/shuju/web/dataServerView?id={match.group(1)}&pageNum={page_num}'
                else:
                    continue

                data_list.append({
                    'url': frame_url,
                })
            except Exception as e:
                continue

        return data_list

    @staticmethod
    def extract_page_data(session_page):
        # 以下XPath假设是基于你上传HTML文件中的结构
        frame = session_page.ele('x://body/div[2]/div/div/div/div/div/table')

        title = session_page.ele('x://div[@class="header"]/h2/a').text
        subject = frame.ele('x://tr[4]/td[2]').text  # 数据主题在第四行第四列
        description = frame.ele('x://tr[2]/td[1]').text  # 摘要信息在第二行第二列
        source_department = frame.ele('x://tr[1]/td[1]').text  # 部门名称在第一行第二列

        release_time = frame.ele('x://tr[3]/td[2]').text  # 数据截止时间在第三行第四列
        update_time = frame.ele('x://tr[3]/td[2]').text  # 页面未提供更新时间信息

        open_conditions = frame.ele('x://tr[5]/td[2]').text  # 开放等级在第五行第四列
        data_volume = frame.ele('x://tr[3]/td[1]').text  # 数据条数在第三行第二列
        file_type = [file.text for file in session_page.eles('x://body/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/div[1]/span')]  # 文件类型在第三行第四列
        is_api = 'True' if 'Json' in file_type else 'False'

        access_count = frame.ele('x://tr[6]/td[1]').text  # 访问量在第六行第二列
        download_count = frame.ele('x://tr[5]/td[1]').text  # 下载量在第五行第二列
        api_call_count = 0  # 页面未提供API调用次数
        link = session_page.url  # 页面的URL

        update_cycle = frame.ele('x://tr[4]/td[1]').text  # 更新频率在第四行第二列

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle)

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


if __name__ == '__main__':
    page = ChengdeCrawler(is_headless=True)
    page.run()
