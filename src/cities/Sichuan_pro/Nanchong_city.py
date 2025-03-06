import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class NanchongCrawler(PageBase):
    """
    一级跳转：get请求：获取json数据，在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '南充市',
                     'province': 'Sichuan',
                     'total_items_num': 1613 + 500,
                     'each_page_count': 20,
                     'base_url': 'https://www.nanchong.gov.cn/data/resCatalog/find?code=&type=&inputtext=&RELEASETIME=desc&accesscount=&downnum=&callnum=&ykfcclnum=&update_time=&page={page_num}&rows=20&resourcenum=&tag_id1=&tag_id2=&tag_id3='
                     }
        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"*/*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"JSESSIONID=2963A4D4F0D11B5FA5A3C4539FFA7A0B; _gscu_651081769=19297691tckkrc17; _trs_uv=ly8iw1cn_5533_h64z; _gscbrs_651081769=1; _gscs_651081769=205055532cmk5c17|pv:2","Host":"www.nanchong.gov.cn","Referer":"https://www.nanchong.gov.cn/data/catalog/index.html","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.params = {'page': '2', 'limit': '10', 'field': 'data_Update_Time'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            url = self.base_url.format(page_num=i)
            session.get(url=url, proxies=self.fiddler_proxies, headers=self.headers, verify=False)

            while True:
                try:
                    json_data = session.response.text if session.response.text else session.html
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}页 - 解析JSON数据失败，正在重试 - {e}')

            if json_data:
                if self.is_api:
                    processed_data = self.extract_api_page_data(json_data)
                else:
                    processed_data = self.extract_page_data(json_data, '')
                views_list.extend(processed_data)
                self.logger.info(
                    f'第{i}/{self.total_page_num}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

        return views_list

    def process_data(self, extracted_data):

        session = SessionPage()
        item_data = []

        if not extracted_data:
            return []

        for item in extracted_data:
            url = f'https://data.jingmen.gov.cn/prod-api/openResource/rest/findView?id={item["id"]}'
            session.get(url=url, headers=self.headers, proxies=self.proxies)

            item_data.append(self.extract_page_data(session.response.text, url))

        return item_data

    def extract_data(self, uuid):

        session = SessionPage()
        url = f'https://www.nanchong.gov.cn/data/resCatalog/getinfoitembycode?resourcecode={uuid}'

        session.get(url=url, proxies=self.proxies)

        json_data = session.response.text
        json_data = json.loads(json_data).get('data', '')[0]

        open_conditions = json_data.get('UPDATECYCLENAME', '')
        update_cycle = json_data.get('sharingtype', '')

        return open_conditions, update_cycle

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)
        if data is None:
            return []
        results = data['data']  # 根据新的数据路径调整
        models = []

        for item in results:
            # 直接从每个条目中提取所需数据，使用get方法以防数据字段缺失
            title = item.get('NAME', '')
            subject = item.get('TAG_NAME1', '')
            description = item.get('ABSRACTINFO', '')
            source_department = item.get('PROVIDEDEPT', '')

            release_time = item.get('RELEASETIME', '')
            update_time = item.get('UPDATE_TIME', '')

            data_volume = item.get('RESOURCESNUM', None)
            is_api = 'True' if item.get('CALLNUM', 0) > 0 else 'False'  # 根据描述，没有API访问标记，设为False

            file_type = item.get('FORMAT_NAME', '').split(',')  # 假设格式名称可以直接使用

            access_count = item.get('ACCESSCOUNT', None)
            download_count = item.get('DOWNNUM', None)
            api_call_count = item.get('CALLNUM', None)
            link = f'https://www.nanchong.gov.cn/data/catalog/details.html?id={item.get("ID", "")}'  # 假设没有具体的链接信息

            open_conditions, update_cycle = self.extract_data(item.get('RESOURCECODE', ''))

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        # 提取数据列表
        results = xmltodict.parse(json_data).get('BusinessMessage', {}).get('data', {}).get('content', {}).get(
            'content', [])
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('topicClassify', '')
            description = item.get('serviceName', '')  # 使用 serviceName 作为描述如果 resourceName 不足够描述性
            source_department = item.get('organName', '')
            release_time = item.get('createTime', '')
            update_time = item.get('updateTime', '')
            open_conditions = '有条件开放' if item.get('isOpen', '') == '1' else '无条件开放'
            data_volume = item.get('resourceMount', '0')  # 数据量信息，如果没有提供默认为 '0'

            access_count = item.get('browseCount', 0)
            download_count = item.get('applyCount', 0)  # 假设 applyCount 可能意味着下载
            api_call_count = item.get('serviceCount', 0)  # 使用 serviceCount 作为 API 调用次数
            link = item.get('preUrl', '')  # 使用 preUrl 作为链接地址，如果有的话

            file_type = ['接口']  # 文件类型，如果没有默认为空
            is_api = 'True'  # 假设 serviceType 为 '4' 表示 API 类型
            update_cycle = '每日'

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)

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
    page = NanchongCrawler(is_headless=True)
    page.run()