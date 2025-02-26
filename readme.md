# Chinese City Level Data Open Platform

该项目包含用于爬取地级市数据开放平台的数据和代码。通过此项目，可以收集中国各地级市的公开数据，便于进一步分析和应用。

## 目录

- [背景](#背景)
- [安装](#安装)
- [使用方法](#使用方法)
- [项目结构](#项目结构)
- [贡献](#贡献)
- [许可证](#许可证)

## 背景

此项目的目的是提供一个工具，能够自动从各地级市的数据开放平台爬取和收集数据，为研究和数据分析提供支持。

## 安装

首先，克隆此仓库：

```bash
git clone https://github.com/SuperJJ2333/Chinese_city_level_data_open_platform.git
cd Chinese_city_level_data_open_platform

```
## 项目结构

```
.
├── .idea
├── output
├── src
│   ├── cities
│   ├── common
│   ├── config.py
│   ├── main.py
├── README.md

src: 包含所有的源代码
src/cities: 各个城市的爬虫实现
src/analysis: 数据分析和整合
src/common: 公共工具和辅助函数
config.py: 配置文件


output: 保存爬取的数据
output/cities_output_files: 保存各个城市的爬取数据
output/provinces_output_files: 保存各省的爬取数据

```

## 联系
如果有问题使用该项目，可以联系我 1014826460@qq.com
