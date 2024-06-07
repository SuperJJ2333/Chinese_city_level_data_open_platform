import json


json_str = """
{
    "status": true,
    "data": [
        {
            "gljks": 0,
            "glsjs": 2,
            "glwds": 26,
            "glzys": 28,
            "isParent": "false",
            "name": "能源环境",
            "parentId": "",
            "rcid": "9A938A883F5D4F409853A29EC455903A"
        },
        {
            "gljks": 0,
            "glsjs": 29,
            "glwds": 12,
            "glzys": 41,
            "isParent": "false",
            "name": "信息产业",
            "parentId": "",
            "rcid": "876EB75E135D4BECB29679FA5056508E"
        },
        {
            "gljks": 0,
            "glsjs": 15,
            "glwds": 46,
            "glzys": 61,
            "isParent": "false",
            "name": "经济发展",
            "parentId": "",
            "rcid": "7BD6C484BD864D81969C671970755009"
        },
        {
            "gljks": 0,
            "glsjs": 0,
            "glwds": 92,
            "glzys": 92,
            "isParent": "false",
            "name": "教育文化",
            "parentId": "",
            "rcid": "A8828D6E0AE84F94B33211CE6240F4BF"
        },
        {
            "gljks": 0,
            "glsjs": 14,
            "glwds": 40,
            "glzys": 54,
            "isParent": "false",
            "name": "公共安全",
            "parentId": "",
            "rcid": "CC447F591F964C8FAF7CCAB0F148C121"
        },
        {
            "gljks": 0,
            "glsjs": 3,
            "glwds": 30,
            "glzys": 33,
            "isParent": "false",
            "name": "农村农业",
            "parentId": "",
            "rcid": "1FD56E9EC5EB416FB726988993473DDD"
        },
        {
            "gljks": 0,
            "glsjs": 0,
            "glwds": 25,
            "glzys": 25,
            "isParent": "false",
            "name": "金融服务",
            "parentId": "",
            "rcid": "52BD892C74234251804FF84A123D5F82"
        },
        {
            "gljks": 0,
            "glsjs": 0,
            "glwds": 30,
            "glzys": 30,
            "isParent": "false",
            "name": "文化娱乐",
            "parentId": "",
            "rcid": "54486354269D408E902541E5C8F04034"
        },
        {
            "gljks": 0,
            "glsjs": 3,
            "glwds": 2,
            "glzys": 5,
            "isParent": "false",
            "name": "就业服务",
            "parentId": "",
            "rcid": "CB0B447EAD8B44FEA3FEF3FD85FD4B74"
        },
        {
            "gljks": 0,
            "glsjs": 0,
            "glwds": 15,
            "glzys": 15,
            "isParent": "false",
            "name": "医疗卫生",
            "parentId": "",
            "rcid": "F71F22E8863941E7800C3D2A4D88406E"
        },
        {
            "gljks": 0,
            "glsjs": 0,
            "glwds": 22,
            "glzys": 22,
            "isParent": "false",
            "name": "法律服务",
            "parentId": "",
            "rcid": "140C42C773184A3FB4D3468ED30D1E13"
        },
        {
            "gljks": 3,
            "glsjs": 28,
            "glwds": 159,
            "glzys": 190,
            "isParent": "false",
            "name": "公共服务",
            "parentId": "",
            "rcid": "DF3B2BE096494DD1913475357B57D1F4"
        },
        {
            "gljks": 2,
            "glsjs": 3,
            "glwds": 24,
            "glzys": 29,
            "isParent": "false",
            "name": "交通服务",
            "parentId": "",
            "rcid": "72D241F44CB645E0B311C2F184D75ECF"
        },
        {
            "gljks": 0,
            "glsjs": 239,
            "glwds": 206,
            "glzys": 445,
            "isParent": "false",
            "name": "政府机构",
            "parentId": "",
            "rcid": "F502C35D2F364FDFABE2C959721A2C31"
        }
    ],
    "msg": ""
}
"""

json_data = json.loads(json_str)

targeted_id = {item['rcid']: item['glzys'] for item in json_data['data']}

print(targeted_id)
