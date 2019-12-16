# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2019-12-12
Info: 非关系型数据库 之 Key-Value数据库 Redis的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/91875849
"""
from redis import StrictRedis, ConnectionPool

def connectRedis(h,pt,pw):
    try:
        # import redis
        # r = redis.Redis(host=h, port=pt, password=pw) # host后的IP是需要连接的ip，本地是127.0.0.1或者localhost

        url = "redis://:"+pw+"@"+h+":"+str(pt)+"/0"
        print(url)
        pool = ConnectionPool.from_url(url)
        r = StrictRedis(connection_pool=pool)

        print("Redis connection successful...")
        return r
    except:
        print("Redis connection refused...")
        return "error"

if __name__ and "__main__":
    redis = connectRedis('47.105.196.123', 6379, "QNzs@.root_1347908642")
    if redis != "error":
        # 获取当前数据库中key的数目
        dbSize = redis.dbsize()
        print("dbSize:", dbSize)

        # 获取所有符合规则的key
        list = redis.keys('XY_KEY_20181229083554_*')
        print("Rule-compliant size:", len(list))
        print("Rule-compliant key:", list)

        # 判断一个key是否存在
        key = "XY_KEY_20181229083554_L400000093"
        isExists = redis.exists(key)
        if isExists == 1:
            print("Is exists:", key)
        else:
            print("Non-existent:", key)

        # 给数据库中key为name的string赋予值Sha Shi Di
        redis.set('XY_KEY_Name', 'Sha Shi Di')

        # 返回数据库中key为name的string的value
        name = redis.get('XY_KEY_Name')
        print("Name:", name)

        # 判断key类型
        type = redis.type('XY_KEY_Name')
        print("Type:", type)

        # 将XY_KEY_Name重命名为XY_KEY_NickName
        rename_result = redis.rename('XY_KEY_Name', 'XY_KEY_NickName')
        print("Rename_result:", rename_result)

        # 删除XY_KEY_NickName这个key
        delete_result = redis.delete('XY_KEY_NickName')
        if delete_result == 1:
            print("Delete_result successful")
        else:
            print("Delete_result refused")

        # 删除当前选择数据库中的所有key
        # delete_current_db_key_result = redis.flushdb()
        # print("Delete_current_db_key_result:", delete_current_db_key_result)

        # 删除所有数据库中所有的key
        # delete_all_db_key_result = redis.flushall()
        # print("Delete_all_db_key_result:", delete_all_db_key_result)
