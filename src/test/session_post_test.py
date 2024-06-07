import json

from DrissionPage import SessionPage


session = SessionPage()

url = "https://data.wuhu.cn/datagov-ops/data/getDataSetByWhere"

data = {'sort': '0', 'pageNo': '1', 'pageSize': '15'}

non_proxies = {'https': None, 'http': None}

headers = {
  "Accept": "application/json, text/javascript, */*; q=0.01",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
  "Cookie": "JSESSIONID=98A7E8F79E7F1FCA944CDE2118B8D7A2",
  "Host": "data.wuhu.cn",
  "Origin": "https://data.wuhu.cn",
  "Referer": "https://data.wuhu.cn/datagov-ops/data/dataIndex",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
  "X-Requested-With": "XMLHttpRequest",
  "sec-ch-ua": "\"Microsoft Edge\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Windows\""
}

session.post(url=url, data=data, proxies=non_proxies, headers=headers)


# print(session.response.json)
data = session.response.text.replace('\\', '').strip('"')
print(data)

data_dict = json.loads(data)
print(json.dumps(data_dict, ensure_ascii=False))

