import asyncio
import json
import re
import zlib
from binascii import a2b_hex

import aiohttp
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler

'''
    bilibili直播弹幕爬虫
'''

header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
}
# ws url
bili_live_ws_url = "https://ks-live-dmcmt-sh2-pm-01.chat.bilibili.com/sub"
# 获取建立ws连接时所需key的url
key_url = "https://api.live.bilibili.com/room/v1/Danmu/getConf?room_id={}&platform=pc&player=web"


'''
    ws二进制帧 = 长度为16的b站协议头 + 返回数据
    00 00 01 0F 00 10 00 02 00 00 00 05 00 00 00 00
    第三四位 01 0F表示 协议头+返回数据的长度，第五六位00 10是表示b站协议头的长度，
    第十二位的05表示返回数据是弹幕消息或礼物消息
    数据为弹幕消息或礼物即第十二位为05时，第八和十六位分别为02和00，
    其他情况下，第八和十六位分别为01和01，
'''

# ws数据包类型
ask_connect_num = 7  # 0x07 客户端发出请求连接
ack_connect_num = 8  # 0x08 服务端允许连接回应
heartbeat_num = 2  # 0x02 客户端请求房间人气值、发出心跳包
ask_danmu_num = 3  # 0x03 对客户端心跳包作出回应，返回房间人气值
return_danmu_num = 5  # 0x05 服务端返回弹幕消息、礼物等等

# 心跳包二进制数据
heartbeat_bin = "0000001F0010000100000002000000015B6F626A656374204F626A6563745D"

# 心跳包发送间隔时间
heartbeat_time = 0.483 * 30


async def bili_live_danmu(connect_data):
    session = aiohttp.ClientSession()
    async with session.ws_connect(bili_live_ws_url, proxy="http://127.0.0.1:8888") as ws:
    # async with session.ws_connect(bili_live_ws_url) as ws:
        # 请求连接的数据
        connect_data_bin = json.dumps(connect_data).encode()
        # 请求连接数据的长度 + 16位协议头长度
        msg_len_hex = hex(len(connect_data_bin) + 16).replace("0x", "")
        # 协议头16进制格式
        protocol_num_hex = f"000000{msg_len_hex}001000010000000{ask_connect_num}00000001"
        # 协议头16进制转为二进制格式
        protocol_bin = a2b_hex(protocol_num_hex.encode())
        # ws数据包二进制格式
        msg_bin = protocol_bin + connect_data_bin
        # print(msg_bin)
        # 发送直播间ws连接请求
        print(msg_bin)
        await ws.send_bytes(msg_bin)
        await asyncio.sleep(0.05)
        # 建立ws连接后必须发送一次心跳包
        await heartbeat(ws)
        # 定时心跳包
        scheduler.add_job(heartbeat, 'interval', seconds=heartbeat_time, args=[ws])
        async for msg in ws:
            # 第十二位 0x05 表示服务端返回弹幕消息、礼物、系统消息、网站公告等等
            # print(msg.data)
            if msg.data[11] == 5:
                try:
                    # b站将二进制数据包里除了16位协议头外的数据使用zlib进行压缩，读取时需要解压
                    msg_data = zlib.decompress(msg.data[16:])[16:].decode(errors='ignore')
                    if '"cmd":"DANMU_MSG"' in msg_data:
                        # todo 返回的数据有可能多条弹幕有可能一条弹幕，格式会不一致，需要调整判断
                        # result = re.findall('{"cmd":"DANMU_MSG".*?}]}', msg_data, re.S)
                        result = json.loads(msg_data)
                        # print(result)
                        # for i in result:
                        #     danmu_dict = json.loads(i)
                        danmu = {
                            "USER_ID": result["info"][2][0],
                            "USER_NAME": result["info"][2][1],
                            "MSG": result["info"][1],
                        }
                        print(danmu)
                except:
                    # 返回当前房间粉丝数的数据包没有压缩
                    pass

            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == 'close cmd':
                    await ws.close()
                    print('connection close')
                    break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print('websocket error to closed')
                break


async def heartbeat(ws):
    await ws.send_bytes(a2b_hex(heartbeat_bin.encode()))


def init(url):
    response1 = requests.get(url, headers=header)
    room_id = int(re.findall('"data":{"room_id":(.*?),"', response1.text, re.S)[0])
    response2 = requests.get(key_url.format(room_id), verify=False)
    connect_key = response2.json()["data"]["token"]
    return {"uid": 0, "roomid": room_id, "protover": 2, "platform": "web", "clientver": "1.7.5", "type": 2,
            "key": connect_key}


if __name__ == '__main__':
    room_url = "https://live.bilibili.com/21329290?spm_id_from=333.334.b_62696c695f6c697665.12"
    connect_data_dict = init(room_url)
    loop = asyncio.get_event_loop()
    loop.create_task(bili_live_danmu(connect_data_dict))
    scheduler = AsyncIOScheduler()
    scheduler.start()
    loop.run_forever()
