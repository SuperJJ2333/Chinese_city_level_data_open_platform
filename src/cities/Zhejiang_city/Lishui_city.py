import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class LishuiCrawler(PageBase):
    """
    一级页面：post请求获取api数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '丽水市',
                     'province': 'Zhejiang',
                     'total_items_num': 1025 + 200,
                     'each_page_count': 10,
                     'base_url': 'http://data.lishui.gov.cn/jdop_front/channal/datapubliclist.do'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/javascript, */*; q=0.01","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Content-Length":"133","Content-Type":"application/x-www-form-urlencoded","Cookie":"visited=1; JSESSIONID=C3DD664C1848722BA11F37ED445DEFC0; ZJZWFWSESSIONID=f24db85a-2b7f-451f-b31e-c6360a058587","Host":"data.lishui.gov.cn","Origin":"http://data.lishui.gov.cn","Proxy-Connection":"keep-alive","Referer":"http://data.lishui.gov.cn/jdop_front/channal/data_public.do","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest"}

        self.params = 'pageNumber={page_num}&pageSize=10&type=1&domainId=0&deptId=&regionId=&keyword=&content=&searchString=&orderDefault=&isDownload=1&dimensionId=0'

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            params = self.params
            params = params.format(page_num=i)

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

        headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Cache-Control":"max-age=0","Cookie":"visited=1; JSESSIONID=C3DD664C1848722BA11F37ED445DEFC0; ZJZWFWSESSIONID=f24db85a-2b7f-451f-b31e-c6360a058587","Host":"data.lishui.gov.cn","Proxy-Connection":"keep-alive","Referer":"http://data.lishui.gov.cn/jdop_front/channal/data_public.do","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}\

        for item in urls_list:
            url = item

            session_page.get(url=url, headers=headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page, item)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []
        urls = re.findall(r'detail/data\.do\?iid=\d+', results)

        for url in urls:
            if 'https' not in url:
                url = 'http://data.lishui.gov.cn/jdop_front/' + url

            id_lists.append(url)

        return id_lists

    def extract_page_data(self, session_page, item):
        title = session_page.ele('x://*[@class="sjxqTit1"]').text
        subject = session_page.ele('x://tr[td="数据领域："]/td[2]').text
        description = session_page.ele('x://tr[td[contains(text(),"要:")]]/td[2]').text
        source_department = session_page.ele('x://tr[td="数源单位："]/td[4]').text

        release_time = session_page.ele('x://tr[td="发布日期："]/td[4]').text
        update_time = session_page.ele('x://tr[td="更新日期："]/td[2]').text

        open_conditions = session_page.ele('x://tr[td="数据分级："]/td[2]').text
        data_volume = int(session_page.ele('x://tr[td="数据量："]/td[2]').text if session_page.ele('x://tr[td="数据量："]/td[2]') and session_page.ele('x://tr[td="数据量："]/td[2]').text != "" else 0)
        file_types = [file.text for file in session_page.eles('x://tr[td="数据下载："]/td[2]/a')]
        is_api = 'True' if session_page.ele('x://tr[td="数据接口："]/td[4]').text else 'False'

        access_count = int(session_page.ele('x://tr[td="访问/下载次数："]/td[4]').text.split('/')[0])
        download_count = int(session_page.ele('x://tr[td="访问/下载次数："]/td[4]').text.split('/')[1])
        api_call_count = None  # Assuming no direct count available
        link = session_page.url

        update_cycle = session_page.ele('x://tr[td="更新周期："]/td[4]').text

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
    page = LishuiCrawler(is_headless=True)
    page.run()
