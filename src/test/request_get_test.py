import requests

url = 'https://data.nantong.gov.cn/api/anony/portalResource/findResourceByPage?page=0&size=10&sortType=&sortStyle=&themeType=&industryType=&applicationType=&isOpenToSociety=&resType=6&companyId=&assessmentUser=&comType=1'

non_proxies = {'http': None, 'https': None}

fiddler_proxies = {
    'http': 'http://127.0.0.1:8888',
    'https': 'http://127.0.0.1:8888'
}

headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6", "Cache-Control": "max-age=0",
            "Cookie": "SESSION=44e8c1c1-a5d7-4fbd-82d4-bcf9b45ea46b; imgurl=\"http://null/dssg/\"; __jsluid_s=88f61873fad4527e262d7c508b03e5d1; mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}",
            "Host": "data.nantong.gov.cn", "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1", "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
            "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Microsoft Edge\";v=\"126\"",
            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\""}

response = requests.get(url, headers=headers, proxies=fiddler_proxies, verify=False)
# response = requests.get(url, headers=headers)

print(response.content)
print("status_code: " + str(response.status_code))
