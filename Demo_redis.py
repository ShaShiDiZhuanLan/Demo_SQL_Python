# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2020-4-8
Info: 非关系型数据库 之 Key-Value数据库 Redis的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/91875849
"""
from redis import StrictRedis, ConnectionPool


def connect_redis(h, pt, pw):
    """
    连接Redis
    :param h:  host
    :param pt:  port
    :param pw:  password
    :return:
    """
    try:
        # import redis
        # r = redis.Redis(host=h, port=pt, password=pw) # host后的IP是需要连接的ip，本地是127.0.0.1或者localhost
        url = "redis://:"+pw+"@"+h+":"+str(pt)+"/0"
        print(url)
        pool = ConnectionPool.from_url(url)
        r = StrictRedis(connection_pool=pool)

        print("Redis connection successful...")
        return r
    except Exception as e:
        print("Redis connection refused...", str(e))
        return "error"


def key_count(rd):
    """获取当前数据库中key的数目"""
    count_tmp = rd.dbsize()
    print("key_count:", count_tmp)
    return count_tmp


def get_keys_rule(rd, keys_comp):
    """获取所有符合规则的key"""
    keys_comp_tmp = rd.keys(keys_comp)
    print("Rule-compliant size:", len(keys_comp_tmp))
    print("Rule-compliant key:", keys_comp_tmp)
    return keys_comp_tmp


def is_exists(rd, key_str):
    """判断一个key是否存在"""
    is_tmp = rd.exists(key_str)
    print("Is exists:" if is_tmp else "Non-existent:", key_str)
    return is_tmp


def set_key(rd, key, value):
    """设置键值"""
    try:
        rd.set(key, value)
        print("set_key success!")
        return "ok"
    except Exception as e:
        print("set_key error: ", str(e))
        return "error"


def get_key(rd, key):
    """获取键值"""
    try:
        tmp = rd.get(key)
        print("get_key success:", tmp)
        return tmp
    except Exception as e:
        print("get_key error: ", str(e))
        return "error"


def get_key_type(rd, key):
    """判断key类型"""
    try:
        tmp = rd.type(key)
        print("get_key_type success:", tmp)
        return tmp
    except Exception as e:
        print("get_key_type error: ", str(e))
        return "error"


def key_rename(rd, key, key_nick):
    """将key重命名为key_nick"""
    try:
        tmp = rd.rename(key, key_nick)
        print("key_rename success:", tmp)
        return tmp
    except Exception as e:
        print("key_rename error: ", str(e))
        return "error"


def del_key(rd, key):
    """删除指定的key"""
    delete_result = rd.delete(key)
    if delete_result:
        print("del_key successful")
    else:
        print("del_key refused")


def del_all_keys(rd, db_type="current"):
    """删除当前/所有选择数据库中的所有key"""
    try:
        if db_type == "current":    # 当前数据库中的所有key
            tmp = rd.flushdb()
            print("删除当前选择数据库中的所有key", tmp)
        else:    # 所有数据库的所有key
            tmp = redis.flushall()
            print("删除所有数据库中所有的key", tmp)
        return tmp
    except Exception as e:
        print("del_all_keys error: ", str(e))
        return "error"


if __name__ and "__main__":
    redis = connect_redis('127.0.0.1', 6581, "QNzs@.root_1347908642")
    if redis != "error":
        key_count(redis)
        get_keys_rule(redis, "XY_KEY_20181229083554_*")
        is_exists(redis, "XY_KEY_20181229083554_L400000093")
        set_key(redis, "XY_KEY_Name", "Sha Shi DI")
        get_key(redis, "XY_KEY_Name")
        get_key_type(redis, "XY_KEY_Name")
        key_rename(redis, "XY_KEY_Name", "XY_KEY_NickName")
        del_key(redis, "XY_KEY_NickName")