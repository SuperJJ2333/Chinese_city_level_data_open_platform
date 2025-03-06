import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class XiaoganCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '孝感市',
                     'province': 'Hubei',
                     'total_items_num': 692 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://www.xiaogan.gov.cn/themeList.jspx?pageNo={page_num}'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Cookie": "Hm_lvt_d6b6c7f82e14a35f60f4c25b44d895b8=1718372692; Hm_lvt_e44f7210835046e58582a29167d49099=1718372692; _front_site_id_cookie=1; clientlanguage=zh_CN; shrio_sessionid=263e523dfdec4be088ad84a1371a0196; animateKey=1; _site_id_cookie=1; viewTimes=1; Hm_lpvt_d6b6c7f82e14a35f60f4c25b44d895b8=1718372721; Hm_lpvt_e44f7210835046e58582a29167d49099=1718372721",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"}

        self.params = {'page': '3', 'orgId': 'null', 'orgDirId': 'null', 'domainId': '0', 'resourceType': 'DATAQUERY',
                       'sortWord': 'modifyDate', 'direction': 'down', 'listPageNum': '3'}

    def run(self):
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
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page, '')
            except Exception as e:
                self.logger.warning(f'解析数据失败 - {e}')
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://body/div[5]/div[1]/div[2]/div[2]/div/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://h2/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'https://www.xiaogan.gov.cn{frame_url}'
                else:
                    frame_url = frame_url

                file_types = frame.eles('x://div[4]/div[3]/ul')
                file_types_text = [file.text.strip() for file in file_types]

                data_list.append({
                    'url': frame_url,
                    'file_types': file_types_text
                })
            except Exception as e:
                self.logger.warning(f'第{page_num}页 - 解析数据失败 - {e}')
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "数据名称")]/following-sibling::div').text
        subject = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "数据主题")]/following-sibling::div').text
        description = "环境质量月报"  # Description seems to be a static piece of information about the dataset.
        source_department = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "提供单位")]/following-sibling::div').text

        release_time = self.convert_date_format(session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "发布时间")]/following-sibling::div').text)
        update_time = release_time # No specific update time information in the HTML.

        open_conditions = ''
        is_api_conditions = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "API接口")]/following-sibling::div/a').text
        data_volume = None  # Assuming data volume isn't provided.

        file_type = session_page.ele('x://div[@class="data-content"]//div[contains(text(), '
                                     '"数据格式")]/following-sibling::div').text  # This would ideally be set based on
        # the passed argument files_type.
        is_api = 'True' if is_api_conditions else 'False'

        access_count = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "浏览次数")]/following-sibling::div').text
        download_count = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "下载量/调用量")]/following-sibling::div').text.split('/')[0]
        api_call_count = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "下载量/调用量")]/following-sibling::div').text.split('/')[1]

        link = session_page.url  # Assuming this is the correct way to get the URL.
        update_cycle = session_page.ele(
            'x://div[@class="data-content"]//div[contains(text(), "更新频度")]/following-sibling::div').text

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
    def convert_date_format(date_str):
        """
        将日期从 DD-MM-YY 格式转换为 YYYY-MM-DD 格式。

        :param date_str: 日期字符串，格式为 DD-MM-YY
        :return: 转换后的日期字符串，格式为 YYYY-MM-DD
        """
        # 将输入字符串解析为 datetime 对象
        date_obj = datetime.strptime(date_str, "%y-%m-%d")

        # 将 datetime 对象格式化为新的字符串格式
        new_date_str = date_obj.strftime("%Y-%m-%d")

        return new_date_str



if __name__ == '__main__':
    page = XiaoganCrawler(is_headless=True)
    page.run()
