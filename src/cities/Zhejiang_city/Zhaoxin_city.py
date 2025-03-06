import json
import re
import time
import urllib.parse
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZhaoxinCrawler(PageBase):
    """
    post请求获取api数据
    二级页面：get请求在详细页获取目的数据

    """

    def __init__(self, is_headless=True):
        city_info = {'name': '绍兴市',
                     'province': 'Zhejiang',
                     'total_items_num': 795 + 500,
                     'each_page_count': 10,
                     'base_url': 'https://data.sx.gov.cn/athena/catalog/page'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Cache-Control":"max-age=0","Connection":"keep-alive","Content-Length":"204","Content-Type":"application/json;charset=UTF-8","Cookie":"JSESSIONID=b9d0f89a-cc65-449c-84fc-6ab295a3d947; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}","Host":"data.sx.gov.cn","If-Modified-Since":"0","Origin":"https://data.sx.gov.cn","Referer":"https://data.sx.gov.cn/kf/open/table","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","X-Requested-With":"XMLHttpRequest","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        self.params = '{"pageNum":2,"pageSize":10,"deptId":"","domainId":"","orderField":"2","isDownload":"","uncode":"","scene":"","fieldType":"","searchString":"","content":"","orderDefault":"","isCatalog":"","searchType":""}'

        self.params = json.loads(self.params)

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(1, self.total_page_num):
            self.params['pageNo'] = i
            params = self.params

            url = self.base_url
            session.post(url=url, headers=self.headers, proxies=self.proxies, json=params)

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

        headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"JSESSIONID=6910df15-a876-4cdf-b8df-cc9c35b5618c; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}","Host":"data.sx.gov.cn","Referer":"https://data.sx.gov.cn/kf/open/table/detail/201431","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}

        for item in urls_list:
            url = f'https://data.sx.gov.cn/athena/catalog/getDetails?dataId={item.get("rowGuid")}'

            session_page.get(url=url, headers=headers, proxies=self.proxies)

            item_data = self.extract_page_data(session_page.response.text, url)

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(json_data):

        data = json.loads(json_data)

        # 假设所有相关信息都在 'result' 键中
        results = data.get('data', [])
        id_lists = []

        for item in results['rows']:
            # 提取和构建所需信息
            rowGuid = item.get('iid', '')
            id_lists.append({
                'rowGuid': rowGuid,
            })

        return id_lists

    def extract_page_data(self, json_data, url):
        item = json.loads(json_data).get('data')  # 更新数据路径到单个数据对象

        title = item.get('title', '')
        subject = item.get('domainStr', '')  # 假设domainStr表示主题
        description = item.get('content', '')
        source_department = item.get('deptName', '')

        release_time = item.get('createDate', '')
        update_time = item.get('dataUpdateDate', '')  # 使用modifyDate作为更新时间

        condition = item.get('openStatus', 0)
        if condition == 0:
            open_conditions = "申请开放"
        elif condition == 1:
            open_conditions = "登录公开"
        else:
            open_conditions = "完全公开"

        data_volume = item.get('dataCount', None)  # 使用dataCount作为数据量
        is_api = 'True' # 根据interfaceId判断是否API

        access_count = item.get('visitCount', None)
        download_count =item.get('useCount', None)
        api_call_count = item.get('invokes', None)

        file_types = ["XLS", "CSV", "XML", "JSON", "RDF"]  # 使用fileType作为文件类型

        link = url  # 假设没有直接的链接信息
        update_cycle = self.format_update_cycle(item.get('updateFreq', ''))  # 计算更新周期   # 使用updateFrequency作为更新周期

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
            1: "每日",
            2: "每周",
            3: "每月",
            4: "每季度",
            5: "每半年",
            6: "每年",
            7: "不定期",
            8: "不更新",
            11: "其他"
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
    page = ZhaoxinCrawler(is_headless=True)
    page.run()