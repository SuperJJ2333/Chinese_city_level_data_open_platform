import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class LuzhouCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '泸州市',
                     'province': 'Sichuan',
                     'total_items_num': 6956 + 1000,
                     'each_page_count': 10,
                     'base_url': 'https://data.luzhou.cn/portal/dataservice/getPage'
                     }

        api_city_info = {'name': '泸州市_api',
                         'province': 'Sichuan',
                         'total_items_num': 6986 + 1000,
                         'each_page_count': 10,
                         'base_url': 'https://data.luzhou.cn/portal/dataservice/getPage',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        if self.is_api:
            self.headers = {"Accept":"*/*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"95","Content-Type":"application/x-www-form-urlencoded; charset=UTF-8","Cookie":"JSESSIONID=F353C990F1BDFA5EB4449E086F0E0ABE; Hm_lvt_dce2578ca0b0d4c50e3f97a9871ac709=1719001694,1719050026,1720515882; HMACCOUNT=A202F23FD4E0D795; Hm_lpvt_dce2578ca0b0d4c50e3f97a9871ac709=1720516071","Host":"data.luzhou.cn","Origin":"https://data.luzhou.cn","Referer":"https://data.luzhou.cn/portal/openapi?orgId=","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

            self.params = {'codeId': '031435e690e74555937e3285bfceba88', 'searchOrder': '0', 'page': '2', 'rows': '10', 'serviceType': '0,4'}
        else:
            self.headers = {"Accept":"*/*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Content-Length":"95","Content-Type":"application/x-www-form-urlencoded; charset=UTF-8","Cookie":"JSESSIONID=F353C990F1BDFA5EB4449E086F0E0ABE; Hm_lvt_dce2578ca0b0d4c50e3f97a9871ac709=1719001694,1719050026,1720515882; HMACCOUNT=A202F23FD4E0D795; Hm_lpvt_dce2578ca0b0d4c50e3f97a9871ac709=1720515908","Host":"data.luzhou.cn","Origin":"https://data.luzhou.cn","Referer":"https://data.luzhou.cn/portal/opendata?orgId=","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}
            self.params = {'codeId': '031435e690e74555937e3285bfceba88', 'searchOrder': '0', 'page': '3', 'rows': '10', 'serviceType': '0,3'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['page'] = str(i)
            params = self.params

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, data=params)

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

        for item in urls_list:
            url = f'http://www.wlcbdata.gov.cn/portal/service_detail?id={item.get("rowGuid")}&type=opendata&orgId='

            headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
                                 "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                       "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,"
                                                                              "en-US;q=0.6",
                       "Cache-Control": "max-age=0",
                       "Connection": "keep-alive", "Cookie": "JSESSIONID=E0F5CF23AA4E749EC56F9CA2305A19DF",
                       "Host": "www.wlcbdata.gov.cn", "Referer": "http://www.wlcbdata.gov.cn/portal/opendata?orgId=",
                       "Upgrade-Insecure-Requests": "1", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                                                                       "Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}

            session_page.get(url=url, headers=headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page, item)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('result', [])
        id_lists = []

        for item in results['rows']:
            # 提取和构建所需信息
            rowGuid = item.get('id', '')

            access_amount = item.get('serviceViewCount', 0)
            download_amount = item.get('serviceApplyCount', 0)

            id_lists.append({
                'rowGuid': rowGuid,
                'access_amount': access_amount,
                'download_amount': download_amount
            })

        return id_lists

    def extract_page_data(self, session_page, item):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://*[@id="tabLink1"]/div[2]/div/ul/li[1]/span[2]').text
        subject = session_page.ele(
            'x://li[span="数据主题："]/span[2]').text
        description = session_page.ele(
            'x://li[span="服务描述："]/span[2]').text
        source_department = session_page.ele(
            'x://li[span="提供方："]/span[2]').text

        release_time = session_page.ele(
            'x://li[span="发布时间："]/span[2]').text
        update_time = release_time

        open_conditions = session_page.ele('x://tr[td="开放条件"]/td[4]').text
        data_volume = item.get('collectNum', None)  # 页面中未提供数据量信息
        file_types = [session_page.ele('x://tr[td="资源格式"]/td[2]').text]
        is_api = 'False' if '数据库' not in file_types else 'True'

        access_count = item.get('serviceViewCount', None)
        download_count = item.get('serviceApplyCount', None)
        api_call_count = None
        link = session_page.url

        update_cycle = session_page.ele(
            'x://tr[td="更新周期"]/td[6]').text

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
        encoded_data = urllib.parse.quote(encoded_data)

        return encoded_data


if __name__ == '__main__':
    page = LuzhouCrawler(is_headless=True)
    page.run()
