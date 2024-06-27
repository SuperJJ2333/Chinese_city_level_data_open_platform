import requests

url = 'http://data.huangshi.gov.cn/myapi/Datainfo/index?departmentid=&lyid=&bshy=&dataifa=0&page=2'

non_proxies = {'http': None, 'https': None}

fiddler_proxies = {
    'http': 'http://127.0.0.1:8888',
    'https': 'http://127.0.0.1:8888'
}

headers = {
            "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cookie": "PHPSESSID=0c6af615c9b9ce5a34875f502db6bc7c", "Host": "data.huangshi.gov.cn",
            "Referer": "http://data.huangshi.gov.cn/html/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"}


response = requests.get(url, headers=headers, proxies=non_proxies, verify=False)
# response = requests.get(url, headers=headers)

print(response.content)
print("status_code: " + str(response.status_code))
