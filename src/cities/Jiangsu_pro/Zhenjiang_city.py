import json
import time
from datetime import datetime
import xmltodict

from DrissionPage import ChromiumOptions, ChromiumPage, SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class ZhengjiangCrawler(PageBase):
    """
    一级跳转：get请求：获取json数据，在目录页获取目的数据
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '镇江市',
                     'province': 'Jiangsu',
                     'total_items_num': 4248 + 1500,
                     'each_page_count': 10,
                     'base_url': 'https://data.zhenjiang.gov.cn/prod-api/openResource/rest/page?resourceName=&topicClassify=&dataRelam=&organName=&industryType=&isOpen=&updateTimeSort=&applyCountSort=&browseCountSort=&scoreCountSort=&downCountSort=&page={page_num}&size=10'
                     }

        api_city_info = {'name': '镇江市_api',
                         'province': 'Jiangsu',
                         'total_items_num': 17 + 10,
                         'each_page_count': 10,
                         'base_url': 'https://data.zhenjiang.gov.cn/prod-api/openService/rest/page?serviceName=&topicClassify=&dataRealm=&organName=&industryType=&servicePublishTimeSort=&applyCountSort=&browseCountSort=&scoreCountSort=&downCountSort=&page={page_num}&size=10',
                         'is_api': 'True'
                         }

        # super().__init__(city_info, is_headless)
        super().__init__(api_city_info, is_headless)

        if self.is_api:
            self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"arialoadData=false","Host":"data.zhenjiang.gov.cn","Referer":"https://data.zhenjiang.gov.cn/?uuid=2d25323c-7b6c-11ed-b150-5254007afb88","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}
        else:
            self.headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6","Connection":"keep-alive","Cookie":"arialoadData=false","Host":"data.zhenjiang.gov.cn","Referer":"https://data.zhenjiang.gov.cn/?uuid=2d25323c-7b6c-11ed-b150-5254007afb88","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0","sec-ch-ua":"\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"","sec-ch-ua-mobile":"?0","sec-ch-ua-platform":"\"Windows\""}
        self.params = {'page': '2', 'limit': '10', 'field': 'data_Update_Time'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.session

        for i in range(0, self.total_page_num - 1):
            url = self.base_url.format(page_num=i)
            session.get(url=url, proxies=self.proxies, headers=self.headers, verify=False)

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

    def extract_data(self, json_data):
        data = xmltodict.parse(json_data)
        data_list = []

        result = data.get('BusinessMessage', []).get('data', [])

        if result['content'] is None:
            return []

        for item in result['content']['content']:
            if isinstance(item, dict):
                item_id = item.get('id', '')
            elif isinstance(item, str):
                item = result['content']['content']
                item_id = item.get('id', '')

                data_list.append({
                    'id': item_id,
                })
                continue
            else:
                item_id = item.get('id', '')

            data_list.append({
                'id': item_id,
            })

        return data_list

    def extract_page_data(self, json_data, item_url):
        data = json.loads(json_data)
        results = data['data']['content']  # 使用正确的路径获取数据列表
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('industryType', '')  # 使用行业类型作为主题
            description = item.get('resourceRes', '')
            source_department = item.get('organName', '')

            release_time = item.get('publishTime', '')
            update_time = item.get('updateTime', '')
            open_conditions = '无条件开放' if item.get('isOpen', 0) == 0 else '有条件开放'
            data_volume = item.get('storageCount', None)

            # 判断是否有API接口
            is_api = 'True' if item.get('serviceInterfaceCount', 0) > 0 else 'False'

            # 文件类型由资源类型决定
            file_type = [item.get('resMountType', '')] if item.get('resMountType', '') else []

            access_count = item.get('browseCount', None)
            download_count = item.get('downCount', None)
            api_call_count = item.get('serviceInterfaceCount', None)  # 使用服务接口计数作为API调用次数
            link = f'https://data.zhenjiang.gov.cn/?uuid={item.get("instanceId", "")}#/open/data-resource/info/{item.get("id", "")}'  # 假设没有具体的链接信息
            update_cycle = self.format_update_cycle(item.get('updateCycle', ''))

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

    def extract_api_page_data(self, json_data):
        # 提取数据列表
        data = json.loads(json_data)
        results = data['data']['content']  # 使用正确的路径获取数据列表
        models = []

        for item in results:
            title = item.get('resourceName', '')
            subject = item.get('industryType', '')  # 使用行业类型作为主题
            description = item.get('des', '')
            source_department = item.get('organName', '')

            release_time = item.get('servicePublishTime', '')
            update_time = item.get('updateTime', '')
            open_conditions = '无条件开放' if item.get('isOpen', 0) == 0 else '有条件开放'
            data_volume = item.get('dataRealm', None)

            # 判断是否有API接口
            is_api = 'True'

            # 文件类型由资源类型决定
            file_type = ['API']  # 假设所有资源都是API类型

            access_count = item.get('browseCount', None)
            download_count = item.get('downCount', None)
            api_call_count = item.get('callCount', None)  # 使用调用次数
            link = f''  # 假设preUrl字段提供了API的链接

            update_cycle = '每月'

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
            8: '实时',
            2: '每日',
            3: '每周',
            4: '每月',
            5: '每季度',
            6: '每半年',
            7: '每年',
            1: '其他'
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
    page = ZhengjiangCrawler(is_headless=True)
    page.run()
