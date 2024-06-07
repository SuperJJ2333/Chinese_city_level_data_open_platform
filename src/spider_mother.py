import copy
import json
import math
import sys
import threading
import time
from typing import List, Callable
import requests
from DrissionPage import ChromiumPage, ChromiumOptions, SessionPage
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from lxml import html
from requests import RequestException
import urllib3

from src.common.content_utils import *
from src.common.form_utils import *

# 禁用 InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss} - <level>{level}</level> - {message}</green>",
    level="INFO",
)


class PageMother:
    def __init__(self, city_info: dict, content_xpath: dict = None, is_headless=False, thread_num: int = 5, proxies: dict = None):
        """
        初始化爬虫母类。

        参数:
        city_info (dict): 城市信息字典，包含 'name', 'base_url', 'targeted_year', 'province', 'total_news_num', 'each_page_num' 等键。
        content_xpath (dict): 用于解析页面的XPath字典。
        is_headless (bool): 是否启用无头浏览器模式。
        thread_num (int): 线程数量。
        proxies (dict): 代理设置字典。
        """
        self.name = city_info['name']
        self.base_url = city_info['base_url']
        self.targeted_year = city_info['targeted_year'] - 1
        self.province = city_info['province']
        self.total_news_num = city_info['total_news_num']
        self.each_news_num = city_info['each_page_num']
        self.total_page_num = math.ceil(self.total_news_num / self.each_news_num) + 1

        self.content_xpath = content_xpath or {}
        self.is_headless = is_headless
        self.thread_num = thread_num
        self.proxy = proxies if proxies is not None else {"http": None, "https": None}

        self.news_data = []
        self.unique_news = []
        self.lock = threading.Lock()

        self.start_page()

    def start_page(self):
        """根据是否为无头模式初始化浏览器页面。"""
        options = ChromiumOptions()
        options.auto_port()
        if self.is_headless:
            options.headless()
        self.page = ChromiumPage(options)

        logger.info(f"{self.name} - 爬取年份为{self.targeted_year} - 新闻数量为{self.total_news_num}条 - "
                    f"页数有{self.total_page_num - 1}页 - 爬虫最大进程数为{self.thread_num}")

    def fetch_web_multiple(self, frames_xpath=None, title_xpath=None, date_xpath=None, listen_target=None,
                           cleaned_method: Callable = None, by_method: str = None, is_begin_from_zero=None):
        """多线程抓取web页面数据并处理重复数据。"""
        page_range = range(0, self.total_page_num - 1) if is_begin_from_zero else range(1, self.total_page_num)

        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            future_to_url = self.submit_fetch_tasks(executor, page_range, frames_xpath, title_xpath, date_xpath,
                                                    listen_target, cleaned_method, by_method)

            self.process_futures(future_to_url, cleaned_method)

        self.save_files()

    def submit_fetch_tasks(self, executor, page_range, frames_xpath, title_xpath, date_xpath, listen_target,
                           cleaned_method, by_method):
        """提交抓取任务到线程池。"""
        if by_method == "by_json":
            return {executor.submit(self.get_data_from_json, self.base_url.format(page_num=page_num), page_num): page_num for page_num in page_range}
        elif by_method == "by_listen":
            return {executor.submit(self.get_data_from_listen, self.base_url.format(page_num=page_num), page_num, listen_target, is_new_tab=True): page_num for page_num in page_range}
        elif by_method == "by_session":
            return {executor.submit(self.get_data_from_listen, self.base_url.format(page_num=page_num), page_num, listen_target, is_new_tab=True): page_num for page_num in page_range}
        else:
            return {executor.submit(self.get_data_from_page, self.base_url.format(page_num=page_num), page_num, frames_xpath, title_xpath, date_xpath): page_num for page_num in page_range}

    def process_futures(self, future_to_url, cleaned_method):
        """处理线程池返回的结果。"""
        for future in as_completed(future_to_url):
            try:
                news_data = future.result()
                if cleaned_method:
                    filtered_data = cleaned_method(news_data['json_dict'], self.targeted_year, news_data['page'], logger)
                else:
                    filtered_data = news_data['json_dict']

                if filtered_data:
                    unique_data = remove_duplicates(filtered_data)
                    self.process_and_store_news(unique_data)
            except Exception as e:
                logger.error(f"处理future结果时发生错误: {e}")

    def fetch_web_by_click(self, next_button_xpath: str, process_json_method: Callable, target_path=None, by_method: str = None):
        """通过页面点击抓取网页数据，无法多线程操作。"""
        self.page.get(self.base_url)
        if by_method == 'by_listen':
            self.page.listen.start(target_path)

        for page_num in range(1, self.total_page_num):
            news_data = self.fetch_page_data(page_num, by_method, target_path)
            if news_data:
                cleaned_json_data = process_json_method(news_data)
                self.news_data.extend(cleaned_json_data)
            self.click_next_page(next_button_xpath)
            time.sleep(2)

        self.news_data = remove_duplicates(self.news_data)
        self.get_content()

    def fetch_page_data(self, page_num, by_method, target_path):
        """根据方法抓取页面数据。"""
        if by_method == 'by_listen':
            return self.get_data_from_listen(self.page.url, page_num, target_path)
        elif by_method == 'by_json':
            return self.get_data_from_json(self.page.url, page_num)
        else:
            return self.get_data_from_page('', page_num, is_by_click=True)

    def click_next_page(self, next_button_xpath):
        """点击页面上的“下一页”按钮。"""
        try:
            self.page.ele(next_button_xpath).click()
        except Exception as e:
            self.page.ele(next_button_xpath).click(by_js=True)
            logger.warning(f"下一页按钮错误 - {e}")
        finally:
            logger.info("进入下一页")

    def fetch_web_by_json(self, cleaned_method: Callable = None):
        """当页面为json格式时，使用多线程直接获取API接口数据。"""
        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            futures = [executor.submit(self.get_data_from_json, self.base_url.format(page_num=page_num), page_num) for page_num in range(1, self.total_page_num)]

            for future in as_completed(futures):
                try:
                    news_data = future.result()
                    filtered_data = cleaned_method(news_data['json_dict'], self.targeted_year, news_data['page'], logger) if cleaned_method else news_data
                    if news_data:
                        news_data = remove_duplicates(filtered_data)
                        self.process_and_store_news(news_data, is_direct_fetch=True)
                except Exception as e:
                    logger.error(f"通过URL获取内容时，出现问题: {e}")

        self.save_files()

    def fetch_web_by_requests(self, request_type: str, data_dict: dict = None, page_num_name: str = '', cleaned_method: Callable = None,
                              update_time_to: bool = False, off_set: int = 1, base_url: str = None, change_url=None, headers=None, data_type: str = 'data',
                              is_begin_from_zero=False, is_versify=True, is_by_requests=False):
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
                futures.append(executor.submit(self.get_data_from_post_method, data_dict_copy, page_num, headers=headers, is_versify=is_versify))
            elif request_type == 'post_xpath':
                futures.append(executor.submit(self.get_data_from_post_xpath, data_dict_copy, page_num, headers=headers))
            elif request_type == 'post_session':
                post_url = f'{change_url}{page_num}/10' if change_url else None
                futures.append(executor.submit(self.get_data_from_post_method, data_dict_copy, page_num, is_by_session=True, post_url=post_url, data_type=data_type, headers=headers, is_versify=is_versify))
            elif request_type == 'get':
                futures.append(executor.submit(self.get_data_from_get_method, page_num * off_set, headers=headers, is_versify=is_versify))
            elif request_type == 'get_xpath':
                futures.append(executor.submit(self.get_data_from_get_xpath, page_num * off_set, headers=headers, is_versify=is_versify, is_by_requests=is_by_requests, base_url=base_url))
        return futures

    def process_request_futures(self, futures, cleaned_method, base_url):
        """处理线程池返回的结果。"""
        for future in as_completed(futures):
            news_data = future.result()
            try:
                filtered_data = cleaned_method(news_data['json_dict'], self.targeted_year, news_data['page'], logger, base_url=base_url) if cleaned_method else news_data
            except Exception as e:
                logger.error(f"进行清理函数时，出现问题: {e}")

            if filtered_data:
                try:
                    news_data = remove_duplicates(filtered_data)
                    self.process_and_store_news(news_data)
                except Exception as e:
                    logger.error(f"获取详细新闻内容时，出现问题: {e}")

    def process_and_store_news(self, news_data, is_direct_fetch=False):
        """处理并存储新闻数据，保证线程安全。"""
        with self.lock:
            if is_direct_fetch:
                self.unique_news.extend(news_data)
            else:
                self.unique_news = process_news_data(news_data, self.page, self.unique_news)

    def get_content(self):
        """获取新闻内容。"""
        self.unique_news = process_news_data(self.news_data, self.page, self.unique_news)
        self.save_files()

    def get_data_from_page(self, url, page_num, frames_xpath=None, title_xpath=None, date_xpath=None, is_by_click=False):
        """从给定的URL加载新闻列表。"""
        frames_xpath = frames_xpath or self.content_xpath['frames']
        title_xpath = title_xpath or self.content_xpath['title']
        date_xpath = date_xpath or self.content_xpath['date']

        tab = self.page.new_tab() if not is_by_click else self.page
        if not is_by_click:
            tab.get(url)
            if page_num % 10 == 0:
                logger.info(f"当前的页数：{page_num}/{self.total_page_num} - URL：{url}")
        else:
            if page_num % 10 == 0:
                logger.info(f"当前的页数：{page_num}/{self.total_page_num}")

        news_frames = tab.eles(frames_xpath)
        titles = [frame.ele(title_xpath) for frame in news_frames]
        dates = [self.get_news_date(frame, date_xpath) for frame in news_frames]

        news_data = fetch_news_data(titles, dates, url, self.targeted_year, page_num)
        if not is_by_click:
            tab.close()
        return {'json_dict': news_data}

    def get_news_date(self, frame, date_xpath):
        """获取新闻日期。"""
        for xpath in date_xpath:
            date = frame.ele(xpath)
            if date:
                return date
        return None

    def get_data_from_json(self, url, page_num):
        """通过get请求获取API返回的JSON数据。"""
        tab = self.page.new_tab()
        tab.get(url)
        if page_num % 10 == 0:
            logger.info(f"当前的页数：{page_num}/{self.total_page_num} - URL：{url}")

        soup = BeautifulSoup(tab.html, 'html.parser')
        json_str = soup.get_text()
        data = json.loads(json_str) if json_str else None
        tab.close()
        return data

    def get_data_from_listen(self, url, page_num, listen_target, is_new_tab=False):
        """通过监听post请求获取数据。"""
        tab = self.page.new_tab() if is_new_tab else self.page
        tab.listen.start(listen_target)
        tab.get(url)

        res = tab.listen.wait(timeout=10)
        data_dict = res.response.body if res else None
        if not data_dict:
            tab.refresh()
            res = tab.listen.wait(timeout=20)
            data_dict = res.response.body if res else None
            if not data_dict:
                logger.error(f"第{page_num}页无法抓取到目标数据包 - URL：{url}")
                return None

        if is_new_tab:
            tab.close()

        if page_num % 10 == 0:
            logger.info(f"当前的页数：{page_num} - URL：{url}")

        return {'json_dict': data_dict, 'page': page_num} if data_dict else None

    def get_data_from_post_method(self, data_dict, page_num, post_url=None, is_by_session=False, headers=None, data_type='data', is_versify=True):
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

    def get_data_from_get_method(self, page_num, headers=None, is_versify=True):
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

    def save_files(self):
        """保存数据到文件。"""
        logger.success(f"{self.name} - 共爬取数据{len(self.unique_news)}条")
        save_to_excel(self.unique_news, self.name, self.province)
        self.page.quit()
