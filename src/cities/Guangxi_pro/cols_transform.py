import re

import pandas as pd
import os
from pathlib import Path

from mother_class.data_model import DataModel


def process_xlsx_files(source_folder, target_folder):
    """
    处理指定文件夹内的所有Excel文件，转换列名，并保存到新的文件夹。

    参数:
    source_folder (str): 源文件夹路径，包含需要处理的Excel文件。
    target_folder (str): 目标文件夹路径，用于保存处理后的文件。

    """
    # 确保目标文件夹存在
    Path(target_folder).mkdir(parents=True, exist_ok=True)

    column_names = ['location', 'title', 'subject', 'description', 'source_department', 'release_time', 'update_time',
                    'open_conditions',
                    'data_volume', 'is_api', 'file_type', 'access_count', 'download_count', 'api_call_count', 'link',
                    'update_cycle']

    # 遍历源文件夹中的所有Excel文件
    for file_name in os.listdir(source_folder):
        if file_name.endswith('.xlsx'):
            print(f"正在处理文件：{file_name}")
            file_path = os.path.join(source_folder, file_name)
            # 读取Excel文件
            df = pd.read_excel(file_path)

            # 指定新的列名映射
            column_mapping = {
                '所属行政区域': 'location',
                '资源名称': 'title',
                '数据领域': 'subject',
                '部门名称': 'source_department',
                '资源描述': 'description',

                '发布日期': 'release_time',
                '数据更新日期': 'update_time',
                '开放类型': 'open_conditions',
                '数据容量': 'data_volume',
                '开放方式': 'is_api',
                '文件资源格式': 'file_type',
                '浏览量': 'access_count',
                '下载量': 'download_count',
                # 'API调用计数': 'api_call_count',
                # '链接': 'link',
                '更新频率': 'update_cycle'
            }

            # 重命名列
            df.rename(columns=column_mapping, inplace=True)

            df['location'] = extract_city_name(file_name)  # 处理行政区域列
            df['api_call_count'] = None  # 新增API调用计数列
            df['link'] = ''  # 新增链接列

            df = df[column_names]

            # 处理每行数据，创建DataModel对象
            data_models = [DataModel(**row.to_dict()) for index, row in df.iterrows()]
            filtered_models = [model.to_dict() for model in data_models]  # 示例筛选条件

            # 将筛选后的数据转换回DataFrame
            new_df = pd.DataFrame(filtered_models)

            # 保存新的DataFrame到Excel
            new_file_path = os.path.join(target_folder, file_name)
            new_df.to_excel(new_file_path, index=False)

    print("所有文件处理完成。")

def extract_city_name(text):
    match = re.search(r'([\u4e00-\u9fa5]+市)', text)
    if match:
        return match.group(1)
    return None


if __name__ == '__main__':
    # 使用函数
    source_folder = r'E:\pythonProject\outsource\China_province_opened_data\docs\Guangxi'
    target_folder = r'E:\pythonProject\outsource\China_province_opened_data\output\Guangxi'
    process_xlsx_files(source_folder, target_folder)
