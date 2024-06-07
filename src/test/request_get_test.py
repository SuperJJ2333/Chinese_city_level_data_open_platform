import requests

url = 'https://www.hefei.gov.cn/open-data-web/kfzy/queryKfZyPage.do?zylxdm=1&currentPageNo=2&pageSize=10&rtcode=03&rcid=&flag=0&sortSign='

non_proxies = {'http': None, 'https': None}

headers = {
  "Cookie": "JSESSIONID=1F60BEEC781A349A69050BEE76CF5F9B; __jsluid_s=d4913291e8caeeb03cf8fe23aae051d8; hefei_gova_SHIROJSESSIONID=edf17941-814a-4df9-9c10-1efac5268c7c; arialoadData=false; __jsl_clearance_s=1717425274.84|0|x4efgFitP1ZY6xQglXDvDaLloco%3D",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
}

response = requests.get(url, headers=headers, proxies=non_proxies)
# response = requests.get(url, headers=headers)

print(response.content)
print("status_code: " + str(response.status_code))
