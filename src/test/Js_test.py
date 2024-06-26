from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Signature import pkcs1_15
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Hash import SHA1
import base64
import urllib.parse
import json

public_key_str = """-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCyVdkiuxwWtI7u1PXpKiWQn46Arm+6E3W1Rc1QrQblZeQie3kCymczT5I4pF+tnjHFValCy4hPJnZ5fJ7mYOuoBeYc7cOJgGMV86BneNlfAoXOS1Cpk+/pREohNSg2G95i1uJ9JJhTuLyZOj5h3sLKXFNSN0ygL3W8H0vvTHtZzQIDAQAB
-----END PUBLIC KEY-----
"""

# 私钥字符串
private_key_str = """-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBALJV2SK7HBa0ju7U9ekqJZCfjoCub7oTdbVFzVCtBuVl5CJ7eQLKZzNPkjikX62eMcVVqULLiE8mdnl8nuZg66gF5hztw4mAYxXzoGd42V8Chc5LUKmT7+lESiE1KDYb3mLW4n0kmFO4vJk6PmHewspcU1I3TKAvdbwfS+9Me1nNAgMBAAECgYEAnQSXXUwPrDIoE0Mwja5NSuwhhU2RE+3kWsntv5XJF5vB5VZ/sqRRf00VOogHsg8yVURZ9nTMPJxp/gdqxP8bHOpbXdC000kGCDA5Hi0wKW/pQsDBCuYVgvVDEXKEhvJ+OBQ0YaIXykumQCa5iNS89JQbWLvUNSntWQQIa5h3Q8UCQQDZeBYc3kRtAPFGYp3v/ZBmjAdo0jsAh5HLdlOoJhjWu/Bq0jj45tnScmRlVuNt2eoY8Lzm61OwkNC7jjZEKuT/AkEA0e6+CyhOOFRIXOYzKqSz7GAfz2MApbqCl2h55LNQvgvnCvWnAnVdOXVuo7awmDfV7QY4YbEMsG4Wea6hpMlFMwJBALn/qMIWJOYqOTKfJEBgWkIrICc6MDa6vSsNUG3v76yx3+YtWYchQ0poho/aafjJuhyMwrSqr9DDe5P/BVD2cxcCQQCG+rRixCWW/koQwUqAzqmJAD0zwPo3lPZGl7xYGht+NnT1jQE3CXNJcIIU7XAaTzxTTD6QQaBhCEeXMXtpUqgVAkBGDJ8WOmSzKOARdf3ZjUgFbqbCV4wxD2YFPsXIhMmB4bJiK5r5znMfWZk3qli8UE2vuceT1x9eIvMvziTrFUgt
-----END PRIVATE KEY-----"""


# 模拟 JavaScript 中的 btoa 和 encodeURIComponent
def encode_to_base64(input_string):
    # 使用 quote 编码字符串，类似于 JavaScript 的 encodeURIComponent
    encoded_string = urllib.parse.quote(input_string)
    # 将编码后的字符串转换为字节并进行 Base64 编码
    base64_encoded = base64.b64encode(encoded_string.encode('utf-8'))
    # 将 Base64 编码结果转换为字符串
    return base64_encoded.decode('utf-8')


# 实现字符编码计算函数
def get_byte_length(s):
    if s >= 65536 and s <= 1114111:
        return 4
    elif s >= 2048 and s <= 65535:
        return 3
    elif s >= 128 and s <= 2047:
        return 2
    else:
        return 1


# 分块加密函数
class RSACipher:
    def __init__(self, public_key_pem):
        self.public_key = RSA.import_key(public_key_pem)
        self.cipher = PKCS1_v1_5.new(self.public_key)

    def encrypt_long(self, message):
        max_chunk_size = 117  # 最大分块大小
        encrypted_chunks = []
        message_length = len(message)

        start = 0
        while start < message_length:
            end = min(start + max_chunk_size, message_length)
            chunk = message[start:end]
            encrypted_chunk = self.cipher.encrypt(chunk.encode('utf-8'))
            encrypted_chunks.append(base64.b64encode(encrypted_chunk).decode('utf-8'))
            start = end

        return ''.join(encrypted_chunks)


# 实现加密函数 u(t)
def u(t):
    cipher = RSACipher(public_key_str)
    encoded_data = encode_to_base64(t)
    encrypted_data = cipher.encrypt_long(encoded_data)
    if encrypted_data:
        return encrypted_data.upper()
    else:
        return None
# 实现签名函数 h(t)
def h(t):
    key = RSA.import_key(private_key_str)
    h = SHA1.new(t.encode('utf-8'))
    signature = pkcs1_15.new(key).sign(h)
    return base64.b64encode(signature).decode('utf-8')


# 定义参数并调用加密和签名函数
def main():
    params = {
        "page": 340,
        "pagesize": 8,
        "xxzytgf": "",
        "zygs": "",
        "kffs": "",
        "xxzymc": "",
        "sort": "",
        "xxzylm": "",
        "yhxxbh": "",
        "wjbm": "",
        "sortField": "",
        "sortRule": "desc",
        "bmjb": "市级"
    }
    t = {
        'url': 'http://opendata.nanjing.gov.cn/backend/NJIC_KFMH_JM/sjbm/queryItemInfo',
        'params': params
    }
    e = u(json.dumps(t['params']))
    n = h(e)
    i = f'{t["url"]}?body={e}&sign={n}'
    t['params'] = {}
    t['url'] = i
    print(f'body: {e}')
    print(f'sign: {n}')
    print(f'Updated URL: {t["url"]}')


if __name__ == '__main__':
    main()
