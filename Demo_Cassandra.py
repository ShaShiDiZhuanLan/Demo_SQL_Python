# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2019-12-12
Info: 非关系型数据库 之 列数据库 Cassandra 的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/93198926
"""
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


# 创建会话
def create_key_space(key_space_name, ster):
    session_t = ster.connect(keyspace=key_space_name)  # ster.connect(keyspace='DemoShaShiDi') 获取指定keyspace的会话连接
    return session_t


# 连接会话
def connect_key_space(key_space_name, ster):
    session_t = ster.connect(keyspace=key_space_name)
    return session_t


# 打印会话连接key_spaces
def print_key_spaces(ster):
    print("-------打印会话连接key_spaces------")
    print(ster.metadata.keyspaces)
    print("-----------------------------------")


# 打印表单tables
def print_tables(ster, key_space_name):
    print("------------打印表单tables---------")
    print(ster.metadata.keyspaces[key_space_name].tables)
    print("-----------------------------------")


if __name__ == '__main__':
    # 获取集群
    ster = Cluster(contact_points=['127.0.0.1'], port=9042, load_balancing_policy=RoundRobinPolicy())
    # 会话连接名称
    keyspacename = "demoshashidi"

    session = connect_key_space(keyspacename, ster)

    print_key_spaces(ster)
    print_tables(ster, keyspacename)

    session.execute('drop table stu;')  # 删除table
    print("删除table")
    session.execute("create table stu(name text, id int primary key);")     # 创建table
    print("创建table")

    # 增加,和update类似
    sql = 'insert into stu(id,name) values(%s, %s)'
    session.execute(sql, (1, 'ShaShiDi'))
    session.execute(sql, (2, 'ShaShiDi'))
    print("增加id=1和2，name=ShaShiDi")

    # 更新,和insert类似
    sql = 'update stu set name=%s where id=%s'
    session.execute(sql, ('SHA SHI DI', 2))
    print("根据ID更新某项的字段，这里更新id=2的name为SHA SHI DI")

    # 查询所有
    sql = 'select * from stu'
    rs = session.execute(sql)
    print("查询所有:",rs.current_rows)

    # 主键id查询/条件查询
    sql = 'select * from stu where id=%s'
    rs = session.execute(sql, [2])  # 另一种写法,其它类似
    print("查询（写法1）:",rs.current_rows)
    rs = session.execute(sql, (2,))
    print("查询（写法2）:",rs.current_rows)

    # 删除
    sql = 'delete from stu where id=%s '
    session.execute(sql, (2,))
    print("按照ID删除某项")

    # 查询所有
    sql = 'select * from stu'
    rs = session.execute(sql)
    print("查询所有:",rs.current_rows)

    # 关键连接
    session.shutdown()
