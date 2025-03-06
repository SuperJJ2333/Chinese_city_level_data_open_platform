import json
from typing import List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup


from src.common.content_utils import *
from src.common.form_utils import *
from src.mother_class.base_page import PageBase


class PageMultipleFetcher(PageBase):
    """模拟浏览器抓取多页数据，并处理重复数据。"""

    def fetch_web_multiple(self, frames_xpath=None, title_xpath=None, date_xpath=None, listen_target=None,
                           cleaned_method: Callable = None, by_method: str = None, is_begin_from_zero=None):
        """多线程抓取web页面数据并处理重复数据。"""
        page_range = range(0, self.total_page_num - 1) if is_begin_from_zero else range(1, self.total_page_num)

        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            future_to_url = self.submit_fetch_tasks(executor, page_range, frames_xpath, title_xpath, date_xpath,
                                                    listen_target, by_method)

            self.process_futures(future_to_url, cleaned_method)

        self.save_files()

    def submit_fetch_tasks(self, executor, page_range, frames_xpath, title_xpath, date_xpath, listen_target,
                           by_method):
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
                self.data_list.extend(cleaned_json_data)
            self.click_next_page(next_button_xpath)
            time.sleep(2)

        self.news_data = remove_duplicates(self.data_list)
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

    def get_data_from_page(self, url, page_num, is_by_click=False):
        """通过浏览器加载页面，从给定的URL加载新闻列表。"""
        frames_xpath = self.content_xpath['frames']
        title_xpath = self.content_xpath['title']
        date_xpath = self.content_xpath['date']

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

    def get_data_from_json(self, url, page_num):
        """通过浏览器加载页面，获取API返回的JSON数据。"""
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
        """通过浏览器加载页面，监听post请求获取数据。"""
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

    @staticmethod
    def get_news_date(frame, date_xpath):
        """获取新闻日期。"""
        for xpath in date_xpath:
            date = frame.ele(xpath)
            if date:
                return date
        return None
