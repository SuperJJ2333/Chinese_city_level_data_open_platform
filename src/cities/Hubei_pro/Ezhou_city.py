import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class EzhouCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '鄂州市',
                     'province': 'Hubei',
                     'total_items_num': 176,
                     'each_page_count': 7,
                     'base_url': 'https://www.ezhou.gov.cn/sjkfn/sjj/bmfl/index{page_num}.html'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cookie": "Hm_lvt_54775391a0b0a31ca0feac1d0e57fbb0=1718265575; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; Hm_lpvt_54775391a0b0a31ca0feac1d0e57fbb0=1718266524",
            "Host": "www.ezhou.gov.cn", "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1", "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

        self.params = {'page': '3', 'orgId': 'null', 'orgDirId': 'null', 'domainId': '0', 'resourceType': 'DATAQUERY',
                       'sortWord': 'modifyDate', 'direction': 'down', 'listPageNum': '3'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num-1):
            if i == 0:
                url = self.base_url.format(page_num='')
            else:
                url = self.base_url.format(page_num=f'_{i}')
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

                item_data = self.extract_page_data(session_page, item['file_types'])
            except Exception as e:
                self.logger.warning(f'解析数据失败 - {e}')
                continue

            items_list.append(item_data)

        return items_list

    def fetch_access_amount_download_amount(self, docid):
        url = f'https://www.ezhou.gov.cn/subscribe/interfacecount/detail.do?docid={docid}'

        session = SessionPage()
        session.get(url=url, headers=self.headers, proxies=self.proxies)
        json_data = session.response.text

        if json_data:
            access_amount = json.loads(json_data).get('data', {}).get('visitCount', 0)
            download_amount = json.loads(json_data).get('data', {}).get('downloadCount', 0)

            return access_amount, download_amount

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://body/div[1]/div/div[2]/div[2]/div[2]/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div/h3/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('https'):
                    frame_url = f'https://www.ezhou.gov.cn/sjkfn/sjj/bmfl/{frame_url}'
                else:
                    frame_url = frame_url

                file_types = frame.eles('x://div/div[2]/div[1]/span')
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
        title = session_page.ele('x://body/div[1]/div/div[2]/div[1]/h3').text
        subject = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[4]/div[2]').text
        description = session_page.ele('x://div[@class="content-td w100"]').text
        source_department = session_page.ele(
            'x://body/div[1]/div/div[2]/div[2]/ul/li[3]/div[4]').text

        release_time = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[7]/div[4]').text
        update_time = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[7]/div[2]').text

        open_conditions = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[6]/div[2]').text
        data_volume = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[5]/div[4]').text

        file_type = files_type
        is_api = 'False'  # 示例中没有直接的信息来判断是否是API

        # access_count = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[4]/div[4]').text.split('/')[0]
        # download_count = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[4]/div[4]').text.split('/')[1]
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        match = re.search(r'_(\d+)\.html$', link)
        docid = match.group(1)
        access_count, download_count = self.fetch_access_amount_download_amount(docid)

        update_cycle = session_page.ele('x://body/div[1]/div/div[2]/div[2]/ul/li[2]/div[4]').text

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


if __name__ == '__main__':
    page = EzhouCrawler(is_headless=True)
    page.run()
