# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2019-12-12
Info: 非关系型数据库 之 文档型数据库 MongoDB 的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/92841495
"""
from pymongo import MongoClient


def is_exists_db(client, name):
    """数据库列表"""
    db_list = client.list_database_names()
    print("数据库列表:", db_list)
    if name in db_list:
        print("%s 数据库已存在！" % name)
    else:
        print("%s 数据库不存在！" % name)


def test_db(client):
    """测试数据库是否连接成功 返回测试结果"""
    db = client.test
    print("test_db: ", db)
    return db


def create_db(client, name):
    """创建一个数据库 返回数据库对象"""
    db = client[name]
    is_exists_db(client, name)
    return db


def is_exists_col(db, name):
    """表单列表"""
    col_list = db.list_collection_names()
    if name in col_list:
        print("%s 集合已存在！" % name)
    else:
        print("%s 集合不存在！" % name)


def create_col(db, name):
    """创建一个表单 返回表单对象"""
    sheet_tab_one = db[name]
    is_exists_col(db, name)
    return sheet_tab_one


def add_one_data(col, json):
    """增加一条数据 返回_id"""
    result = col.insert_one(json)
    print("增加一条数据: ", result.inserted_id)  # 返回 _id 字段
    return result.inserted_id


def add_many_data(col, jsons):
    """增加多条数据 返回所有文档对应的 _id 值"""
    result = col.insert_many(jsons)
    print("增加多条数据: ", result.inserted_ids)
    return result.inserted_ids


def del_one_data(col, query):
    """删除一条数据"""
    result = col.delete_one(query)
    print("数据已删除: ", result.deleted_count)


def del_many_data(col, query):
    """删除多条数据"""
    result = col.delete_many(query)
    print("数据已删除: ", result.deleted_count)


def del_col(col):
    """删除一个表单"""
    col.drop()
    print("删除表单成功")


def update_one_data(col, query, new_val):
    """修改匹配到的第一条数据"""
    result = col.update_one(query, new_val)
    print("文档已修改: ", result.modified_count)


def update_many_data(col, query, new_val):
    """修改匹配到的多条数据"""
    result = col.update_many(query, new_val)
    print("文档已修改: ", result.modified_count)


def sort_data(col, key, sort_b=True):
    """按照某字段排序 默认True是正序，False是倒序"""
    if sort_b:
        my_doc = col.find().sort(key)
    else:
        my_doc = col.find().sort(key, -1)
    for item in my_doc:
        print(item)


def search_one_data(col):
    """查询一条数据"""
    result = col.find_one()
    print("查询一条数据: ", result)


def search_many_data(col, limit=0):
    """查询集合中所有数据"""
    for item in col.find().limit(limit):
        print(item)


def search_miss_data(col, query):
    """根据指定条件查询 或者正则表达式查询（比如 my_query = { "name": { "$regex": "^S" } }）"""
    print("根据指定条件查询: ", query)
    my_doc = col.find(query)
    for item in my_doc:
        print(item)


if __name__ == "__main__":
    client_ip = "127.0.0.1"
    client_port = 27017
    db_name = 'mongodb_test'
    col_name = 'col_test'
    my_json = {"name": "ShaShiDi", "url": "https://shazhenyu.blog.csdn.net/"}
    # 可以指定_id 我们也可以自己指定 id，插入
    my_jsons = [{"_id": 1, "name": "sha", "url": "https://www.shazhenyu.com"},
                {"_id": 2, "name": "sha2", "url": "https://shazhenyu.com"}]
    my_query = {"name": "ShaShiDi"}
    my_query_regex = {"name": {"$regex": "^S"}} # 以下实例用于读取 name 字段中第一个字母为 "S" 的数据
    my_key = "_id"
    new_values = {"$set": {"name": "ShaShiDi_new"}}

    my_client = MongoClient(client_ip, client_port)
    test_db(my_client)
    my_db = create_db(my_client,db_name)
    my_col = create_col(my_db,col_name)

    # # 增加一条数据
    # add_one_data(my_col, my_json)
    # # 增加多条数据
    # add_many_data(my_col, my_jsons)

    # # 删除一个符合条件的集合
    # del_one_data(my_col, my_query)
    # # 删除所有符合条件的集合
    # del_many_data(my_col, my_query)
    # # 删除该集合中的所有文档
    # del_many_data(my_col, {})
    # # 删除表单
    # del_col(my_col)

    # # 修改第一个匹配到的文档
    # update_one_data(my_col, my_query, new_values)
    # # 修改所有符合条件的文档
    # update_many_data(my_col, my_query, new_values)

    # # 按照某字段排序 默认True是正序，False是倒序
    # sort_data(my_col, my_key)

    # # 根据指定条件查询
    # search_miss_data(my_col, my_query)
    # # 根据正则表达式查询
    # search_miss_data(my_col, my_query_regex)

    # # 查询集合中所有数据 如果写第二个参数，就是指定条数记录查询
    # # search_many_data(my_col)
    # search_many_data(my_col,2)
