from datetime import datetime
import os
import re
import pandas as pd
from loguru import logger


def save_to_excel(unique_news, name, province):
    """
    将新闻数据保存为Excel文件。
    参数:
    unique_news -- 包含新闻数据的列表，其中每个元素是一个字典。
    name -- 用于生成Excel文件名的标识字符串。

    函数会将Excel文件保存在项目根目录外的`output`文件夹中。
    """
    # 创建DataFrame
    df = pd.DataFrame(unique_news)

    # 获取当前文件的目录（假设此脚本位于src/utils文件夹中）
    current_dir = os.path.dirname(__file__)

    if not os.path.exists(os.path.join(current_dir, f'../../output/{province}')):
        os.makedirs(os.path.join(current_dir, f'../../output/{province}'))  # 如果输出目录不存在，创建它

    # 构造文件完整路径
    filename = os.path.join(current_dir, f'../../output/{province}', f'{name}_news.xlsx')

    # 保存到Excel，不包含索引
    df.to_excel(filename, index=False)

    # 日志记录文件保存位置
    logger.info(f"文件已保存至: {filename}")


def remove_duplicates(news_data):
    """
    去除新闻数据中的重复条目，基于标题和日期。
    参数:
    news_data -- 新闻数据列表，每个元素是一个字典，包含'url', 'topic', 'date'等键。
    返回:
    无重复的新闻数据列表。
    """
    seen = set()  # 用于存储已见过的(topic, date)元组
    unique_news = []

    for news in news_data:
        identifier = (news['topic'], news['date'])  # 创建一个用于识别重复的元组
        if identifier not in seen:
            seen.add(identifier)
            unique_news.append(news)
        else:
            logger.warning(f"跳过重复URL: {news['topic']} - 日期为：{news['date']}")

    return unique_news


def clean_news_data(news):
    """
    清洗新闻数据字典中的topic和date字段。
    参数:
    news -- 新闻数据字典，格式为：
        {'url': 'some_url',
        'topic': 'some_topic',
        'date': 'some_date'}

    返回:
    news -- 清洗后的新闻数据字典。
    """
    # 清洗topic，只保留中文字符
    if 'topic' in news:
        # 使用正则表达式匹配中文字符
        news['topic'] = ''.join(re.findall(r'[\u4e00-\u9fff]+', news['topic']))

    # 清洗date，确保其符合YYYY-MM-DD格式
    if 'date' in news:
        news['date'] = format_date(news['date'])

    if 'url' in news:      # 清洗url，只保留域名部分
        news['url'] = format_url(news['url'])
    return news


def format_date(date_str):
    """
    将不同格式的日期字符串转换为 'YYYY-MM-DD' 格式。

    参数:
    date_str (str): 输入的日期字符串。

    返回:
    str: 标准化的日期字符串或者None（如果无法识别格式）。
    """
    if isinstance(date_str, str):
        if not date_str or date_str.strip() == '' or date_str.lower() == 'nan':
            return ''
    elif isinstance(date_str, float):
        if pd.isnull(date_str):
            return ''
    elif isinstance(date_str, datetime):
        return date_str.strftime('%Y-%m-%d')
    elif pd.isnull(date_str):
        return ''

    # 匹配 YYYY-MM-DD 格式
    match_iso = re.search(r'\d{4}-\d{1,2}-\d{1,2}', date_str)
    if match_iso:
        return match_iso.group(0)

    # 匹配 YYYY年MM月DD日 格式
    match_extended = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if match_extended:
        year, month, day = match_extended.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # 匹配 YYYY/MM/DD 格式
    match_line = re.search(r'(\d{4})/(\d{1,2})/(\d{1,2})', date_str)
    if match_line:
        year, month, day = match_line.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # 匹配 YYYY.MM.DD 格式
    match_dot = re.search(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', date_str)
    if match_dot:
        year, month, day = match_dot.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # 匹配 DD-MM-YY 格式
    match_dmy = re.search(r'(\d{1,2})-(\d{1,2})-(\d{2})', date_str)
    if match_dmy:
        day, month, year = match_dmy.groups()
        year = '20' + year  # 假设年份在2000年之后
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    return None

def format_url(url_str):
    url_match = re.search(r"geturl\('(.*?)'", url_str)
    if url_match:
        url = url_match.group(1)
    else:
        url = url_str
    return url


def is_in_year(date_str, year):
    """
    判断给定的日期字符串是否属于指定的年份。

    参数:
    date_str (str): 日期字符串，格式为 "YYYY-MM-DD"。
    year (int): 指定的年份。

    返回:
    bool: 如果日期属于指定年份，返回 True，否则返回 False。
    """
    # 从日期字符串中提取年份部分
    extracted_year = int(date_str[:4])

    # 比较提取的年份和指定的年份是否相同
    return extracted_year == year


def set_nested_value(dic, path, value):
    """
    根据点分隔的路径在嵌套字典中设置值。

    :param dic: 要修改的字典。
    :param path: 表示目标位置路径的字符串，路径各部分由点分隔。
    :param value: 在目标位置设置的值。
    """
    keys = path.split('.')  # 通过点分割路径字符串获取所有键
    for key in keys[:-1]:  # 遍历所有键（除了最后一个）
        dic = dic.setdefault(key, {})  # 获取键对应的字典值，如果不存在，则初始化为字典并返回
    dic[keys[-1]] = int(value)  # 在最后一个键处设置值

def convert_timestamp_to_date(timestamp: int) -> str:

    if isinstance(timestamp, str):
        timestamp = int(timestamp)

    # 将时间戳转换为datetime对象
    dt_object = datetime.fromtimestamp(timestamp / 1000)
    # 格式化为YYYY-MM-DD字符串
    formatted_date = dt_object.strftime('%Y-%m-%d')
    return formatted_date
