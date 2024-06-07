import copy
import json
from typing import List, Callable
import requests
from DrissionPage import SessionPage
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import html
from requests import RequestException


from src.common.content_utils import *
from src.common.form_utils import *
from src.mother_class.base_page import PageBase


class PageRequestsFetcher(PageBase):
    """以SessionPage的方式抓取网页内容。"""

    def fetch_web_by_requests(self, request_type: str, data_dict: dict = None, page_num_name: str = '', cleaned_method: Callable = None,
                              update_time_to: bool = False, off_set: int = 1, base_url: str = None, change_url=None, headers=None, data_type: str = 'data',
                              is_begin_from_zero=False, is_versify=True, is_by_requests=False, is_direct_fetch=False):
        """
        通用方法以多线程方式抓取网页内容，支持POST和GET请求。

        参数:
        request_type (str): 请求类型，'get' 或 'post'。
        data_dict (dict): 如果是 POST 请求，使用的数据字典。
        page_num_name (str): POST 请求中页码参数的名称。
        cleaned_method (Callable): 清洁数据的方法。
        update_time_to (bool): 是否更新时间戳。
        """
        page_range = range(0, self.total_page_num - 1) if is_begin_from_zero else range(1, self.total_page_num)

        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            futures = self.submit_request_tasks(executor, page_range, request_type, data_dict, page_num_name, update_time_to, off_set, change_url, headers, data_type, is_versify, is_by_requests, base_url)

            self.process_request_futures(futures, cleaned_method, base_url)

        self.save_files()

    def submit_request_tasks(self, executor, page_range, request_type, data_dict, page_num_name, update_time_to, off_set, change_url, headers, data_type, is_versify, is_by_requests, base_url):
        """提交抓取任务到线程池。"""
        futures = []
        for page_num in page_range:
            data_dict_copy = copy.deepcopy(data_dict) if data_dict else {}
            if page_num_name:
                set_nested_value(data_dict_copy, page_num_name, str(page_num * off_set))
            if update_time_to:
                data_dict_copy['time_to'] = int(time.time())

            if request_type == 'post':
                # requests访问方式
                futures.append(executor.submit(self.get_data_from_post, data_dict_copy, page_num, headers=headers, is_versify=is_versify))
            elif request_type == 'post_xpath':
                futures.append(executor.submit(self.get_data_from_post_xpath, data_dict_copy, page_num, headers=headers))
            elif request_type == 'post_session':
                # session访问方式
                post_url = f'{change_url}{page_num}/10' if change_url else None
                futures.append(executor.submit(self.get_data_from_post, data_dict_copy, page_num, is_by_session=True, post_url=post_url, data_type=data_type, headers=headers, is_versify=is_versify))
            elif request_type == 'get':
                futures.append(executor.submit(self.get_data_from_get, page_num * off_set, headers=headers, is_versify=is_versify))
            elif request_type == 'get_xpath':
                futures.append(executor.submit(self.get_data_from_get_xpath, page_num * off_set, headers=headers, is_versify=is_versify, is_by_requests=is_by_requests, base_url=base_url))
        return futures

    def process_request_futures(self, futures, cleaned_method, base_url):
        """处理线程池返回的结果。"""
        filtered_data = None

        for future in as_completed(futures):
            news_data = future.result()
            try:
                filtered_data = cleaned_method(news_data['json_dict'], self.targeted_year, news_data['page'], logger, base_url=base_url) if cleaned_method else news_data
            except Exception as e:
                logger.error(f"进行清理函数时，出现问题: {e}")

            if filtered_data:
                try:
                    # 不需要过滤重复数据
                    # news_data = remove_duplicates(filtered_data)
                    news_data = filtered_data
                    self.process_and_store_news(news_data, is_direct_fetch=True)
                except Exception as e:
                    logger.error(f"获取详细新闻内容时，出现问题: {e}")

    def get_data_from_post(self, data_dict, page_num, post_url=None, is_by_session=False, headers=None, data_type='data', is_versify=True):
        """通过POST请求获取数据。"""
        json_dict = {}
        news_url = post_url or self.base_url

        try:
            if is_by_session:
                tab = SessionPage()
                if data_type != 'json':
                    tab.post(news_url, data=data_dict, proxies=self.proxy, headers=headers, verify=is_versify)
                else:
                    tab.post(news_url, json=data_dict, proxies=self.proxy, headers=headers, verify=is_versify)
                json_dict = tab.json if tab.json else tab.html
            else:
                response = requests.post(news_url, data=data_dict, proxies=self.proxy, headers=headers, verify=is_versify)
                json_dict = response.json()

            if page_num % 10 == 0:
                logger.info(f"当前的页数：{page_num}/{self.total_page_num}")
        except RequestException as e:
            logger.error(f"第{page_num}页，请求发生错误 - {e}")
        except json.JSONDecodeError as e:
            logger.error(f"第{page_num}页，JSON解析错误 - {e}")

        return {'json_dict': json_dict, 'page': page_num}

    def get_data_from_get(self, page_num, headers=None, is_versify=True):
        """通过GET请求获取数据。"""
        json_dict = {}
        url = self.base_url.format(page_num=page_num)

        try:
            response = requests.get(url, proxies=self.proxy, headers=headers, verify=is_versify)
            decoded_content = response.content.decode('utf-8')
            json_dict = json.loads(decoded_content)
            if page_num % 10 == 0:
                logger.info(f"当前的页数：{page_num}/{self.total_page_num}")
        except Exception as e:
            logger.error(f"第{page_num}页，发生错误 - {e}")
        finally:
            return {'json_dict': json_dict, 'page': page_num}

    def get_data_from_post_xpath(self, data_dict, page_num, headers=None, is_by_session=True):
        """通过POST请求并解析页面中的数据。"""
        news_list = []

        if is_by_session:
            tab = SessionPage()
            try:
                tab.post(self.base_url, data=data_dict, proxies=self.proxy, headers=headers)
                frames = tab.eles(self.content_xpath['frames'])
                for frame in frames:
                    news_dict = self.extract_news_data(frame)
                    if news_dict:
                        news_list.append(news_dict)
                logger.info(f"第{page_num}页 - 符合年份为{self.targeted_year}的新闻有 {len(news_list)}/{len(frames)} 条")
            except RequestException as e:
                logger.error(f"第{page_num}页，请求发生错误 - {e}")
            except json.JSONDecodeError as e:
                logger.error(f"第{page_num}页，JSON解析错误 - {e}")
            finally:
                if page_num % 10 == 0:
                    logger.info(f"当前的页数：{page_num}页/{self.total_page_num}页")
        return news_list

    def extract_news_data(self, frame):
        """提取新闻数据。"""
        try:
            title = frame.ele(self.content_xpath['title']).attr('title') or frame.ele(self.content_xpath['title']).text
            date = self.get_news_date(frame, self.content_xpath['date'])
            title_url = frame.ele(self.content_xpath['url']).attr('href') if 'url' in self.content_xpath and self.content_xpath['url'] else frame.ele(self.content_xpath['title']).attr('data-url') or frame.ele(self.content_xpath['title']).attr('href')
            if title and date:
                news_dict = {'topic': title, 'date': date, 'url': title_url}
                cleaned_dict = clean_news_data(news_dict)
                if is_in_year(cleaned_dict['date'], self.targeted_year):
                    return cleaned_dict
        except Exception as e:
            return None

    def get_data_from_get_xpath(self, page_num, headers=None, is_versify=True, is_by_requests=False, base_url=None):
        """通过GET请求并解析页面中的数据。"""
        news_list = []

        try:
            news_url = self.base_url.format(page_num=page_num)
            frames = self.get_frames_from_url(news_url, headers, is_versify, is_by_requests)

            for frame in frames:
                news_dict = self.extract_news_data(frame)
                if news_dict:
                    news_list.append(news_dict)
        except RequestException as e:
            logger.error(f"第{page_num}页，请求发生错误 - {e}")
        except json.JSONDecodeError as e:
            logger.error(f"第{page_num}页，JSON解析错误 - {e}")
        finally:
            logger.info(f"第{page_num}页 - 符合年份为{self.targeted_year}的新闻有 {len(news_list)}/{len(frames)} 条")
            if page_num % 10 == 0:
                logger.info(f"当前的页数：{page_num}页/{self.total_page_num}页")
        return news_list

    def get_frames_from_url(self, url, headers, is_versify, is_by_requests):
        """从URL获取页面框架。"""
        if is_by_requests:
            response = requests.get(url, proxies=self.proxy, headers=headers, verify=is_versify)
            page_content = response.content.decode('utf-8')
            doc = html.fromstring(page_content)
            return doc.xpath(self.content_xpath['frames'])
        else:
            tab = SessionPage()
            tab.get(url, proxies=self.proxy, headers=headers, verify=is_versify)
            return tab.eles(self.content_xpath['frames'])

    @staticmethod
    def get_news_date(frame, date_xpath):
        """获取新闻日期。"""
        for xpath in date_xpath:
            date = frame.ele(xpath)
            if date:
                return date
        return None