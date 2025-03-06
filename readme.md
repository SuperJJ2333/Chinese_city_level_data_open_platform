# Chinese Province and City Data Open Platform

A comprehensive tool for collecting, processing, and analyzing open data from Chinese provinces and cities.

[中文版本](#中国省市数据开放平台)

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data Processing Tools](#data-processing-tools)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project provides a suite of tools to collect and process open data from Chinese provincial and city-level government platforms. It includes web crawlers for data collection, processing scripts for data transformation, and analysis tools for data interpretation.

## Features

- **Data Collection**: Automated crawlers for provincial and city-level open data platforms
- **Data Processing**: Tools for merging, comparing, and transforming data files
- **File Management**: Utilities for organizing and renaming files based on Chinese administrative divisions
- **Analysis Support**: Jupyter notebooks for data analysis and visualization

## Project Structure

```
.
├── docs/ # Documentation and processed data
│ ├── after_cities_output_files/ # Processed city data
│ ├── after_provinces_output_files/ # Processed provincial data
├── output/ # Raw collected data
│ ├── cities_output_files/ # Raw city data
│ ├── pre_cities_output_files/ # Pre-processed city data
│ ├── pre_provinces_output_files/ # Pre-processed provincial data
│ ├── provinces_output_files/ # Raw provincial data
│ └── reports/ # Generated reports
├── src/ # Source code
│ ├── analysis/ # Data analysis scripts
│ │ ├── analyze_cities_xlsxs.ipynb # City data analysis
│ │ ├── compare_xlsx_size.py # File size comparison tool
│ │ ├── merge_api_xlsx.py # API data merging tool
│ │ └── pinyin2Chinese_name.ipynb # Pinyin to Chinese converter
│ ├── cities/ # City-specific crawlers
│ ├── common/ # Shared utilities
│ ├── config.py # Configuration settings
│ └── main.py # Main execution script
├── requirements.txt # Project dependencies
├── LICENSE # MIT License
└── README.md # Project documentation
```
## Installation

1. Clone the repository:
```bash
git clone https://github.com/SuperJJ2333/Chinese_city_level_data_open_platform.git
cd Chinese_city_level_data_open_platform
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Data Collection

To collect data from all supported platforms:

```bash
python src/main.py
```

To collect data from specific cities or provinces:

```bash
python src/main.py --regions beijing,shanghai
```

### Data Processing

#### Convert Pinyin to Chinese Province Names

```bash
jupyter notebook src/analysis/pinyin2Chinese_name.ipynb
```

#### Merge API Data with Regular Data

```bash
python src/analysis/merge_api_xlsx.py
```

#### Compare File Sizes Before and After Processing

```bash
python src/analysis/compare_xlsx_size.py
```

## Data Processing Tools

### pinyin2Chinese_name.ipynb

Converts folder names from Pinyin to Chinese province names (e.g., "beijing" → "北京市").

### merge_api_xlsx.py

Merges API-sourced data files with regular data files, combining them into a single dataset while removing duplicates.

Features:
- Merges `xxx_api_news.xlsx` with `xxx_news.xlsx`
- Renames result to `xxx_数据开放.xlsx`
- Handles cases where only one file type exists

### compare_xlsx_size.py

Compares file sizes before and after processing, generating detailed reports.

Features:
- Compares file sizes across directories
- Copies missing files or directories
- Merges files when necessary
- Generates comprehensive reports

### analyze_cities_xlsxs.ipynb

Analyzes city-level open data and generates statistical reports:
- Calculates data volume statistics
- Analyzes update frequencies
- Evaluates data openness conditions
- Tracks API availability and usage

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

# 中国省市数据开放平台

一个用于收集、处理和分析中国省市开放数据的综合工具。

## 目录

- [项目概述](#项目概述)
- [功能特点](#功能特点)
- [项目结构](#项目结构)
- [安装方法](#安装方法)
- [使用说明](#使用说明)
- [数据处理工具](#数据处理工具)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 项目概述

本项目提供了一套工具，用于收集和处理中国省级和市级政府平台的开放数据。它包括用于数据收集的网络爬虫、用于数据转换的处理脚本以及用于数据解释的分析工具。

## 功能特点

- **数据收集**：针对省级和市级开放数据平台的自动化爬虫
- **数据处理**：用于合并、比较和转换数据文件的工具
- **文件管理**：基于中国行政区划组织和重命名文件的实用程序
- **分析支持**：用于数据分析和可视化的Jupyter笔记本

## 项目结构

```
.
├── docs/                      # 文档和处理后的数据
│   ├── after_cities_output_files/    # Processed city data
│   ├── after_provinces_output_files/ # Processed provincial data
├── output/                    # Raw collected data
│   ├── cities_output_files/   # Raw city data
│   ├── pre_cities_output_files/      # Pre-processed city data
│   ├── pre_provinces_output_files/   # Pre-processed provincial data
│   ├── provinces_output_files/       # Raw provincial data
│   └── reports/               # Generated reports
├── src/                       # 源代码
│   ├── analysis/              # 数据分析脚本
│   │   ├── analyze_cities_xlsxs.ipynb    # City data analysis
│   │   ├── compare_xlsx_size.py          # File size comparison tool
│   │   ├── merge_api_xlsx.py             # API数据合并工具
│   │   └── pinyin2Chinese_name.ipynb     # 拼音转中文转换器
│   ├── cities/                # 城市特定爬虫
│   ├── common/                # 共享工具
│   ├── config.py              # 配置设置
│   └── main.py                # 主执行脚本
├── requirements.txt           # 项目依赖
├── LICENSE                    # MIT许可证
└── README.md                  # 项目文档
```

## 安装方法

1. 克隆仓库：
```bash
git clone https://github.com/SuperJJ2333/Chinese_city_level_data_open_platform.git
cd Chinese_city_level_data_open_platform
```

2. 创建虚拟环境（推荐）：
```bash
python -m venv venv
source venv/bin/activate  # Windows系统: venv\Scripts\activate
```

3. 安装依赖：
```bash
pip install -r requirements.txt
```

## 使用说明

### 数据收集

收集所有支持平台的数据：

```bash
python src/main.py
```

收集特定城市或省份的数据：

```bash
python src/main.py --regions beijing,shanghai
```

### 数据处理

#### 将拼音转换为中文省份名称

```bash
jupyter notebook src/analysis/pinyin2Chinese_name.ipynb
```

#### 将API数据与常规数据合并

```bash
python src/analysis/merge_api_xlsx.py
```

#### 比较处理前后的文件大小

```bash
python src/analysis/compare_xlsx_size.py
```

## 数据处理工具

### pinyin2Chinese_name.ipynb

将文件夹名称从拼音转换为中文省份名称（例如，"beijing" → "北京市"）。

### merge_api_xlsx.py

将API来源的数据文件与常规数据文件合并，将它们组合成单个数据集，同时删除重复项。

功能：
- 合并 `xxx_api_news.xlsx` 和 `xxx_news.xlsx`
- 将结果重命名为 `xxx_数据开放.xlsx`
- 处理只存在一种文件类型的情况

### compare_xlsx_size.py

比较处理前后的文件大小，生成详细报告。

功能：
- 比较目录间的文件大小
- 复制缺失的文件或目录
- 必要时合并文件
- 生成全面的报告

### analyze_cities_xlsxs.ipynb

分析城市级开放数据并生成统计报告：
- 计算数据量统计
- 分析更新频率
- 评估数据开放条件
- 跟踪API可用性和使用情况

## 贡献指南

欢迎贡献！请随时提交Pull Request。

1. Fork该仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启一个Pull Request

## 许可证

本项目采用MIT许可证 - 详情请参阅[LICENSE](LICENSE)文件。
