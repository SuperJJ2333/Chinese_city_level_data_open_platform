import json
from Crypto.Cipher import AES
import base64
import time
from datetime import datetime

from DrissionPage import SessionPage

from mother_class.base_page import PageBase
from mother_class.data_model import DataModel


class NanjingCrawler(PageBase):
    """
    JS逆向

    xpath: get 获取目录页网页数据
    二级跳转：需要获取详情页的url
    xpath:目的数据在详情页中
    """

    def __init__(self, is_headless=False):
        city_info = {'name': '南京市',
                     'province': 'Jiangsu',
                     'total_items_num': 2716 + 500,
                     'each_page_count': 10,
                     'base_url': 'http://opendata.nanjing.gov.cn/#/data'
                     }

        super().__init__(city_info, is_headless)

        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                                                                        "Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"}

        self.params = {'page': '3', 'orgId': 'null', 'orgDirId': 'null', 'domainId': '0', 'resourceType': 'DATAQUERY',
                       'sortWord': 'modifyDate', 'direction': 'down', 'listPageNum': '3'}

    def run(self):
        self.total_data = self.process_views()
        self.save_files()

    def process_views(self):

        views_list = []
        session = self.page
        session.listen.start('kfmhDataCatelog/query')
        url = self.base_url

        for i in range(1, self.total_page_num):

            session.get(url=url)

            while True:
                try:
                    break
                except Exception as e:
                    time.sleep(1)
                    self.logger.warning(f'第{i}/{self.total_page_num}页 - 解析JSON数据失败，正在重试 - {e}')

            if True:
                page_data = self.extract_page_data(session, i)
                views_list.extend(page_data)
                self.logger.info(f'第{i}页 - 已获取数据{len(views_list)}/{self.total_items_num}条')

            next_page_but = session.ele('x://*[@class="btn-next"]')
            next_page_but.click(by_js=True)
            time.sleep(0.5)

        return views_list

    def process_page(self, page_data):
        items_list = []
        session_page = SessionPage()

        for item in page_data:
            try:
                url = item['url']
                session_page.get(url=url, headers=self.headers, proxies=self.proxies)

                item_data = self.extract_page_data(session_page, item['file_type'])
            except Exception as e:
                continue

            items_list.append(item_data)

        return items_list

    @staticmethod
    def extract_data(session_page, page_num):
        data_list = []
        frames_list = session_page.eles('x://*[@id="app"]/div[7]/div[2]/div[3]/ul/li')

        for frame in frames_list:
            try:
                frame_url = frame.ele('x://div[1]/a').attr('href')

                # 提取并输出匹配的内容
                if not frame_url.startswith('http'):
                    frame_url = f'http://dt.gov.cn{frame_url}'
                else:
                    frame_url = frame_url

                file_type = [file.text.lower() for file in frame.eles('x://div[4]/div[3]/ul/li')]

                data_list.append({
                    'url': frame_url,
                    'file_type': file_type
                })
            except Exception as e:
                continue

        return data_list

    def extract_page_data(self, session_page, files_type):

        json_data = session_page.listen.wait()

        results = self.decrypt_aes(json_data.response.body)

        results = json.loads(results).get('result', [])  # 根据新的数据路径调整

        models = []

        for item in results:
            title = item.get('XXZYMC', '')
            subject = item.get('XXZYLM', '')
            description = item.get('XXZYZY', '')
            source_department = item.get('XXZYTGF', '')

            release_time = item.get('FBRQ', '')
            update_time = item.get('CREATETIME', '')

            # 根据KFFS字段判断开放条件
            open_conditions = "无条件开放" if item.get('KFFS', 0) == 1 else "有条件开放"

            # 数据量使用XXZYGSFL字段表示
            data_volume = None

            # 判断是否为API提供服务，此处假设若含'JSON'则为API服务
            is_api = 'True' if 'JSON' in item.get('XXZYGSLX', '') else 'False'

            file_type = [o.strip() for o in item.get('XXZYGSLX', '').split(',')]

            access_count = item.get('PAGE_VIEW', None)
            download_count = item.get('DOWNLOADS', None)
            api_call_count = item.get('download_count', None) if is_api == 'True' else None

            link = f'http://opendata.nanjing.gov.cn/#/dataDetail?guid={item.get("XXZYID", "")}&xxzydm={item.get("XXZYDM", "")}'  # 假设没有具体的链接信息
            update_cycle = item.get('GXZQ', '')

            # 创建DataModel实例
            model = DataModel(title, subject, description, source_department, release_time,
                              update_time, open_conditions, data_volume, is_api, file_type,
                              access_count, download_count, api_call_count, link, update_cycle, self.name)
            models.append(model.to_dict())

        return models

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
    def decrypt_aes(encrypted_data, key='njfcchpteswythxt'):
        # Base64解码加密数据
        encrypted_data_bytes = base64.b64decode(encrypted_data)

        # 将密钥和IV解析为字节
        key_bytes = key.encode('latin1')
        iv_bytes = key.encode('latin1')  # IV和key相同

        # 创建AES解密器
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv_bytes)

        # 解密数据
        decrypted_data = cipher.decrypt(encrypted_data_bytes)

        # 去除填充（Zero Padding）
        decrypted_data = decrypted_data.rstrip(b'\x00')

        # 将解密后的数据转换为UTF-8字符串
        return decrypted_data.decode('utf-8')


if __name__ == '__main__':
    page = NanjingCrawler(is_headless=False)
    page.run()
