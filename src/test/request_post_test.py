import json

import requests

url = 'http://183.134.232.54x5f107a28c18865.ipv6best.cn/api/dataset/dataSetList'
non_proxies = {'http': None, 'https': None}

fiddler_proxies = {
    'http': 'http://127.0.0.1:8888',
    'https': 'http://127.0.0.1:8888'
}

headers = {"Accept": "application/json",
           "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
           "Connection": "keep-alive", "Content-Length": "165", "Content-Type": "application/json; charset=UTF-8",
           "Host": "183.134.232.54x5f107a28c18865.ipv6best.cn", "Origin": "http://data.ningbo.gov.cn",
           "Referrer-Policy": "no-referrer",
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
           "X-Content-Type-Options": "nosniff", "X-Download-Options": "noopen",
           "X-Permitted-Cross-Domain-Policies": "none"}

data = '{"pageNo":2,"pageSize":"10","dataUnit":"","domainClassification":"","formatClassification":"","order":"desc","orderName":"UPDATE_TIME","keywords":"","createUser":""}'

data = json.loads(data)

# response = requests.get(url, headers=headers, proxies=fiddler_proxies, verify=False)
# response = requests.get(url, headers=headers)
# response = requests.post(url, headers=headers, json=data, proxies=non_proxies, verify=False)
# response = requests.post(url, headers=headers, data=data, proxies=fiddler_proxies, verify=False)
response = requests.post(url, headers=headers, json=data)

print(response.content)
print("status_code: " + str(response.status_code))
