import time

from loguru import logger
import re
from concurrent.futures import ThreadPoolExecutor

from src.common.form_utils import *


def fetch_news_data(titles, times, link, targeted_date, page_num):
    """获取URL中的标题与时间"""
    news_data = []
    for title, time in zip(titles, times):
        try:
            if not title or not time:
                continue

            if title.text is None and title is None and title.text.strip() == '':  # 更明确地检查标题元素和文本的存在性
                continue

            try:
                # 尝试获取链接和标题属性
                url = title.get('href', '')
            except:
                url = title.attr('href')
            try:
                topic = title.get('title', '').strip()
            except:
                topic = title.text.strip()

            # 如果链接不完整，添加基础链接
            if not url.startswith('http'):
                url = link + url

            # 检查time元素的存在性，并且确保text属性也存在
            if isinstance(time, str):
                date = time.strip()
            elif time.text and title.text.strip() != '':  # 修正这里的逻辑
                date = time.text.strip()
            else:
                date = ''

            data = clean_news_data({'url': url, 'topic': topic, 'date': date})

            # 判断是否符合目标年份
            if is_in_year(data['date'], targeted_date):
                news_data.append(data)
        except Exception as e:
            logger.warning(f"获取标题时出现问题 - {e} - {link} - {title.text}")
            continue
    logger.info(f"第{page_num}页 - 符合年份为{targeted_date}的新闻有 {len(news_data)}/{len(titles)} 条")
    return news_data


def fetch_page_content(page, url, title):
    """
    访问指定的 URL，获取网页内容，并清除不需要的内容。

    参数:
    page -- 页面操作对象。
    url -- 需要访问的 URL 地址。
    title -- 页面的标题，用于过滤和内容比对。

    返回:
    texts -- 清洗后的文本列表。
    """
    # 新建标签页并尝试访问 URL
    tab = page.new_tab()
    tab.get(url, retry=2, timeout=10)
    time.sleep(0.5)

    # 初始化文本容器
    unique_texts = set()
    texts = []

    # 尝试获取页面的所有元素
    try:
        elements = tab.eles('xpath://*')
        if not elements:  # 如果未找到元素，尝试刷新页面
            tab.refresh()
            elements = tab.eles('xpath://*')
    except Exception as e:
        logger.error(f"没有必要元素 - {e} - {url}")
        tab.close()
        return texts

    # 定义非关键词和空白字符模式
    non_keywords = {'404 Not Found', '版权所有', '无法访问', '联系我们', '门户网站', '站点不存在', '公网安备', '网站地图', '背景颜色', 'ICP备案', '无障碍',
                    '微信扫一扫', '打印本页', '分享微信', '浏览器推荐', '首页', '登录', '智能引导', '智能问答', '站群导航', '政务机器人',
                    'index.html', '动态信息', '404页面',
                    '版权声明', '信息公开指南', '扫一下', '索引号', '邮政编码', '关怀版', '老年模式', '公安网备', '导航区', '扫描二维码',
                    '长辈版', '运维电话', '网站标识码', '无法处理此请求'}
    space_pattern = re.compile(r'\s+')

    for element in elements:
        try:
            text = element.text.strip()
            # 检查文本是否重复
            if text in unique_texts:
                continue
            unique_texts.add(text)

            # 检查文本是否包含非关键词，过滤过长的空白和无关信息
            if not any(nk in text for nk in non_keywords) and (text.count('\n') <= 10 and text.count('\t') <= 6):
                cleaned_text = space_pattern.sub(' ', text)  # 替换多个空白字符为单个空格
                # 进一步根据长度和标题相关性过滤文本
                """
                判断规则为：
                1，文本含有“新闻标题”且比“新闻标题”还多15个字 且 含有“学习考察”或“考察学习”
                2，文本比“新闻标题”还多20个字 且 含有“学习考察”或“考察学习”
                3，字数超过50
                """
                # 判断规则应用
                rule_1 = contains_keywords(cleaned_text) and len(cleaned_text) > 15 + len(
                    title) and title in cleaned_text
                rule_2 = contains_keywords(cleaned_text) and len(cleaned_text) > 20 + len(
                    title)
                rule_3 = len(cleaned_text) > 50

                if rule_1 or rule_2 or rule_3:
                    texts.append(cleaned_text)
        except Exception as e:
            logger.debug(f"遍历元素时出现问题 - {e}")
            continue

    if not texts:
        pass

    # 关闭标签页并返回结果
    tab.close()

    return texts


def is_content_valuable(news, page):
    """
    检查新闻内容是否包含关键词并记录。
    参数:
    news -- 新闻数据字典。
    page -- 页面操作对象。
    返回:
    news -- 更新后的新闻字典或 None。
    """
    texts = fetch_page_content(page, news['url'], news['topic'])

    if not texts:
        logger.error(f"无内容可抓取 - {news['topic']} - {news['url']}")
        return None

    # 判断标题是否含有关键词
    title_contains = contains_keywords(news['topic'])
    # 判断全文是否含有关键词
    content_contains = any(contains_keywords(text) for text in texts)

    if title_contains or content_contains:
        logger.success(f"{news['topic']}--"
                       f"新闻内容为: {texts[0][:200]}")
        news['content'] = texts
        return news
    elif not title_contains and not content_contains:
        # if not title_contains:
        #     logger.warning(f"{news['topic']}--标题中不包含关键词：{news['url']}")
        # if not content_contains:
        logger.warning(f"内容中不包含关键词 - {news['topic']} - {news['url']}")
    return None


def contains_keywords(text):
    """
    检查文本中是否含有设定的关键词。
    参数:
    text -- 要检查的文本字符串。
    返回:
    boolean -- 是否包含关键词。
    """
    keywords = ["学习考察", "考察学习"]
    return any(keyword in text for keyword in keywords)


def process_news_data(news_data, page, unique_news, extracted_method):
    """
    使用多线程处理所有新闻数据，并提取有价值的内容。
    参数:
    news_data -- 新闻数据列表。
    page -- 页面操作对象。
    visited -- 已访问的新闻标识集合。
    返回:
    unique_news -- 已处理的有价值新闻列表。
    """
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(is_content_valuable, news, page) for news in news_data]
        for future in futures:
            result = future.result()
            if result:
                unique_news.append(result)
    return unique_news
