import json
import re
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class TaizhouCrawler(PageBase):
    """
    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=True):
        city_info = {'name': '赣州市',
                     'province': 'Jangxi',
                     'total_items_num': 138,
                     'each_page_count': 6,
                     'base_url': 'http://zwkf.ganzhou.gov.cn/page_{page_num}/Open_directory.shtml',
                     }
        api_city_info = {'name': '赣州市_api',
                         'province': 'Jangxi',
                         'total_items_num': 27,
                         'each_page_count': 6,
                         'base_url': 'http://zwkf.ganzhou.gov.cn/page_{page_num}/api_service.shtml',
                         'is_api': 'True'
                         }

        super().__init__(city_info, is_headless)
        # super().__init__(api_city_info, is_headless)

        self.headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
                                  "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,"
                                                                               "en-US;q=0.6",
                        "Cookie": "JSESSIONID=F3B44038EA5BDE49ED902D089E8810F6", "Host": "zwkf.ganzhou.gov.cn",
                        "Proxy-Connection": "keep-alive", "Referer": "http://zwkf.ganzhou.gov.cn/Open_directory.shtml",
                        "Upgrade-Insecure-Requests": "1", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                                                                        "Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}

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
                if self.is_api:
                    item_data = self.extract_api_page_data(session_page, item)
                else:
                    item_data = self.extract_page_data(session_page, item)
            except Exception as e:
                self.logger.warning(f'解析{item["url"]}失败 - {e}')
                continue

            items_list.append(item_data)

        return items_list

    def extract_data(self, session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://html/body/div[5]/div[2]/div[2]/div/div')

        # 循环遍历每一个frame
        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[2]/a').attr('onclick')

                match = re.search(r"isLogin\('([^']+)'\)", frame_url)
                extracted_id = match.group(1)

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'http://zwkf.ganzhou.gov.cn/id_{extracted_id}/api_detail.shtml'
                else:
                    frame_url = frame_url

                # open_type = frame.ele('x://div[2]/span[2]/em').text

                data_list.append({
                    'url': frame_url,
                    # 'file_type': file_type
                    # 'open_type': open_type
                })
            except Exception as e:
                self.logger.warning(f'第{page_num}页 - 解析frame数据失败 - {e}')
                continue

        return data_list

    @staticmethod
    def extract_page_data(session_page, item):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://div[@class="gzTt_r"]/a[@class="shiy_rigA1"]').text
        subject = session_page.ele('x://div[@class="shiyr_div3"][1]/span[2]/em').text
        description = session_page.ele('x://div[@class="zlxg_t2"]').text
        source_department = session_page.ele('x://div[@class="shiyr_div3"][1]/span[1]/em').text

        release_time = session_page.ele('x://div[@class="shiy_left"]/p[2]').text + "-" +\
                       session_page.ele('x://div[@class="shiy_left"]/p[1]').text
        update_time = release_time

        open_conditions = item['open_type']  # 页面中未提供开放状态信息
        data_volume = session_page.ele('x://div[@class="shiyr_div3"][2]/span[1]/em').text
        file_type = [session_page.ele('x://body/div[6]/div[2]/div/div/div[2]/div[3]/span/em').text]  # 文件类型信息未提供
        is_api = 'False'  # 无信息表示是否为API

        access_count = session_page.ele('x://*[@id="apifw_one"]/div[3]/div[1]/table//tr[1]/td').text
        download_count = session_page.ele('x://*[@id="apifw_one"]/div[3]/div[1]/table//tr[2]/td').text
        api_call_count = None  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = ''  # 页面中更新周期是固定文本

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

    def extract_api_page_data(self, session_page, item):
        # 假定session_page是已经加载了HTML内容的对象
        title = session_page.ele('x://div[@class="gzTt_r"]/a[@class="shiy_rigA1"]').text
        subject = session_page.ele('x://div[@class="shiyr_div3"][1]/span[2]/em').text
        description = session_page.ele('x://div[@class="zlxg_t2"]').text
        source_department = session_page.ele('x://div[@class="shiyr_div3"][1]/span[1]/em').text

        release_time = session_page.ele('x://div[@class="shiy_left"]/p[2]').text + "-" + \
                       session_page.ele('x://div[@class="shiy_left"]/p[1]').text
        update_time = release_time

        open_conditions = ''  # 页面中未提供开放状态信息
        data_volume = session_page.ele('x://div[@class="shiyr_div3"][2]/span[1]/em').text
        file_type = [session_page.ele('x://body/div[6]/div[2]/div/div/div[2]/div[3]/span/em').text]  # 文件类型信息未提供
        is_api = 'False'  # 无信息表示是否为API

        access_count = session_page.ele('x://*[@id="apifw_one"]/div[3]/div[1]/table//tr[1]/td').text
        download_count = session_page.ele('x://*[@id="apifw_one"]/div[3]/div[1]/table//tr[2]/td').text
        api_call_count = 0  # 页面中未提供API调用次数信息
        link = session_page.url

        update_cycle = ''  # 页面中更新周期是固定文本

        # 创建DataModel实例
        model = DataModel(title, subject, description, source_department, release_time,
                          update_time, open_conditions, data_volume, is_api, file_type,
                          access_count, download_count, api_call_count, link, update_cycle, self.name)

        return model.to_dict()

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
    page = TaizhouCrawler(is_headless=True)
    page.run()

