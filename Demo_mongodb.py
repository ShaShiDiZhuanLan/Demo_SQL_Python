# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2019-12-12
Info: 非关系型数据库 之 文档型数据库 MongoDB 的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/92841495
"""
from pymongo import MongoClient

# 数据库列表
def ifnotDB(my_client, db_name):
    db_list = my_client.list_database_names()
    print("数据库列表:",db_list)
    if db_name in db_list:
        print("%s 数据库已存在！"%db_name)
    else:
        print("%s 数据库不存在！"%db_name)

# 测试数据库是否连接成功 返回测试结果
def testDB(my_client):
    db = my_client.test
    print("db:", db)
    return db

# 创建一个数据库 返回数据库对象
def creatDB(my_client, name):
    my_db = my_client[name]
    ifnotDB(my_client, name)
    return my_db

# 表单列表
def ifnotCol(my_db, col_name):
    col_list = my_db.list_collection_names()
    if col_name in col_list:
        print("%s 集合已存在！"%col_name)
    else:
        print("%s 集合不存在！"%col_name)

# 创建一个表单 返回表单对象
def creatCol(my_db, name):
    sheet_tab_one = my_db[name]
    ifnotCol(my_db, name)
    return sheet_tab_one

# 增加一条数据 返回_id
def addData_one(my_col, my_json):
    result = my_col.insert_one(my_json)
    print(result.inserted_id,"增加一条数据")# 返回 _id 字段
    return result.inserted_id

# 增加多条数据 返回所有文档对应的 _id 值
def addData_many(my_col, my_jsons):
    result = my_col.insert_many(my_jsons)
    print(result.inserted_ids)
    return result.inserted_ids

# 删除一条数据
def delData_one(my_col, myquery):
    result = my_col.delete_one(myquery)
    print(result.deleted_count,"数据已删除")

# 删除多条数据
def delData_many(my_col, myquery):
    result = my_col.delete_many(myquery)
    print(result.deleted_count,"数据已删除")

# 删除一个表单
def delCol(my_col):
    my_col.drop()
    print("删除表单成功")

# 修改匹配到的第一条数据
def updateData_one(my_col, myquery, new_val):
    result = my_col.update_one(myquery, new_val)
    print(result.modified_count, "文档已修改")

# 修改匹配到的多条数据
def updateData_many(my_col, myquery, new_val):
    result = my_col.update_many(myquery, new_val)
    print(result.modified_count, "文档已修改")

# 按照某字段排序 默认True是正序，False是倒序
def sortData(my_col, my_key, sortB = True):
    if sortB:
        my_doc = my_col.find().sort(my_key)
    else:
        my_doc = my_col.find().sort(my_key, -1)
    for item in my_doc:
        print(item)

# 查询一条数据
def searchData_one(my_col):
    result = my_col.find_one()
    print(result)

# 查询集合中所有数据
def searchData_many(my_col, limit = 0):
    for item in my_col.find().limit(limit):
        print(item)

# 根据指定条件查询 或者正则表达式查询（比如 my_query = { "name": { "$regex": "^S" } }）
def searchData_miss(my_col, my_query):
    print("根据指定条件查询: ", my_query)
    my_doc = my_col.find(my_query)
    for item in my_doc:
        print(item)

if __name__ == "__main__":
    client_ip = "193.112.61.11"
    client_port = 27017
    db_name = 'mongodb_test'
    col_name = 'col_test'
    my_json = {"name": "ShaShiDi", "url": "https://shazhenyu.blog.csdn.net/"}
    # 可以指定_id 我们也可以自己指定 id，插入
    my_jsons = [{"_id": 1,"name": "sha", "url": "https://www.shazhenyu.com"},{"_id": 2,"name": "sha2", "url": "https://shazhenyu.com"}]
    my_query = {"name": "ShaShiDi"}
    my_query_regex = {"name": {"$regex": "^S"}} # 以下实例用于读取 name 字段中第一个字母为 "S" 的数据
    my_key = "_id"
    new_values = {"$set": {"name": "ShaShiDi_new"}}

    my_client = MongoClient(client_ip, client_port)
    testDB(my_client)
    my_db = creatDB(my_client,db_name)
    my_col = creatCol(my_db,col_name)

    # # 增加一条数据
    # addData_one(my_col, my_json)
    # # 增加多条数据
    # addData_many(my_col, my_jsons)

    # # 删除一个符合条件的集合
    # delData_one(my_col, my_query)
    # # 删除所有符合条件的集合
    # delData_many(my_col, my_query)
    # # 删除该集合中的所有文档
    # delData_many(my_col, {})
    # # 删除表单
    # delCol(my_col)

    # # 修改第一个匹配到的文档
    # updateData_one(my_col, my_query, new_values)
    # # 修改所有符合条件的文档
    # updateData_many(my_col, my_query, new_values)

    # # 按照某字段排序 默认True是正序，False是倒序
    # sortData(my_col, my_key)

    # # 根据指定条件查询
    # searchData_miss(my_col, my_query)
    # # 根据正则表达式查询
    # searchData_miss(my_col, my_query_regex)

    # # 查询集合中所有数据 如果写第二个参数，就是指定条数记录查询
    # # searchData_many(my_col)
    # searchData_many(my_col,2)