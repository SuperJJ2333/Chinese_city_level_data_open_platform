import os
import re

import pandas as pd


# 定义一个函数，用来检查并更新'source_department'
def update_source_department(row):
    # 确保source_department是字符串类型
    source_department = str(row['source_department']) if pd.notna(row['source_department']) else ''
    location = str(row['location']) if pd.notna(row['location']) else ''

    if location not in source_department:
        return f"{location}{source_department}"
    return source_department

def extract_city_name(text):
    # 匹配中文城市名称的正则表达式
    match = re.search(r'([\u4e00-\u9fa5]+)市', text)
    if match:
        return match.group(1)
    return text

def merge_xlsx_files(directory, province_column=None, pinyin_map=None):
    all_data = []
    required_columns = ['province', 'location', 'title', 'subject', 'description', 'source_department', 'release_time',
                        'update_time', 'open_conditions', 'data_volume',
                        'is_api', 'file_type', 'access_count', 'download_count', 'api_call_count', 'link',
                        'update_cycle']
    columns = ['location', 'title', 'subject', 'description', 'source_department', 'release_time', 'update_time',
               'open_conditions', 'data_volume',
               'is_api', 'file_type', 'access_count', 'download_count', 'api_call_count', 'link', 'update_cycle']
    # 遍历目录下的所有文件
    for filename in os.listdir(directory):
        if filename.endswith("_news.xlsx") or filename.endswith("__.xlsx"):
            print(f"正在处理文件: {filename}")
            file_path = os.path.join(directory, filename)
            df = pd.read_excel(file_path)
            # 处理空白文件
            if df.shape[0] <= 0:
                continue
            # 处理城市列
            if pinyin_map:
                df['location'] = filename.split('_')[0]
                df['location'] = df['location'].apply(extract_city_name)
                df['location'] = df['location'].apply(lambda x: pinyin_map.get(x, x))

            # 应用这个函数到DataFrame的每一行
            # df['source_department'] = df.apply(update_source_department, axis=1)

            # 只选择所需的列
            df = df[columns]

            # 将数据添加到列表
            all_data.append(df)

    # 合并所有数据
    combined_df = pd.concat(all_data, ignore_index=True)

    if province_column:
        combined_df['province'] = province_column

    # 根据所需的列进行排序
    combined_df = combined_df[required_columns]

    return combined_df


if __name__ == '__main__':
    directory_path = r"E:\pythonProject\outsource\China_province_opened_data\output\Guangxi"
    pinyin_map_Anhui = {
        "Bengbu": "蚌埠市",
        "Chizhou": "池州市",
        "Chuzhou": "滁州市",
        "Huainan": "淮南市",
        "Hefei": "合肥市",
        "Huaibei": "淮北市",
        "Huangshan": "黄山市",
        "Liuan": "六安市",
        "Maanshan": "马鞍山市",
        "Suzhou": "宿州市",
        "Tonglin": "铜陵市",  # Note: Assumed typo corrected for 'Tongling'.
        "Wuhu": "芜湖市",
        "Xuancheng": "宣城市",
        "Haozhou": "亳州市",
        "Anhui": "安徽省"
    }
    pinyin_map_Shanxi = {
        "Changzhi": "长治市",
        "Datong": "大同市",
        "Jincheng": "晋城市",
        "Lvliang": "吕梁市",
        "Shuozhou": "朔州市",
        "Yanquan": "阳泉市",
        "Shanxi": "山西省"
    }
    pinyin_map_Guangxi = {
        'bh': '北海市',
        'cz': '崇左市',
        'fcg': '防城港市',
        'gg': '贵港市',
        'gl': '桂林市',
        'hc': '河池市',
        'hz': '贺州市',
        'lz': '柳州市',
        'nn': '南宁市',
        'qz': '钦州市',
        'wz': '梧州市',
        'yl': '玉林市'
    }

    province_column = '广西'
    result_df = merge_xlsx_files(directory_path, province_column=province_column, pinyin_map=pinyin_map_Guangxi)
    output_file = os.path.join(directory_path, f"{province_column}省开放数据整合.xlsx")
    result_df.to_excel(output_file, index=False)
    print(f"合并后的文件已保存到: {output_file}")
