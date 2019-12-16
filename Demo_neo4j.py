# encoding: utf-8
"""
Author: 沙振宇
CreateTime: 2019-7-9
UpdateTime: 2019-12-12
Info: 非关系型数据库 之 图数据库Neo4j的使用（Python3）
Url: https://shazhenyu.blog.csdn.net/article/details/93116754
"""
from py2neo import Graph, Node, Relationship

graph = Graph(host='193.112.61.11', http_port=7474, user='neo4j', password='123456')

# 清空库
graph.delete_all()

# 创建结点
test_node_0 = Node('西游记', name='唐僧')  # 修改的部分
test_node_1 = Node('西游记', name='孙悟空')  # 修改的部分
test_node_2 = Node('西游记', name='猪八戒')  # 修改的部分
test_node_3 = Node('西游记', name='沙师弟')  # 修改的部分
test_node_4 = Node('西游记', name='白龙马')  # 修改的部分

test_node_3.setdefault("博客地址",'https://shazhenyu.blog.csdn.net/')

graph.create(test_node_0)
graph.create(test_node_1)
graph.create(test_node_2)
graph.create(test_node_3)
graph.create(test_node_4)

# 创建关系
# 分别建立了test_node_1指向test_node_2和test_node_2指向test_node_1两条关系，关系的类型为"丈夫、妻子"，两条关系都有属性count，且值为1。
node_0_node_1 = Relationship(test_node_0, '师傅', test_node_1)
node_0_node_2 = Relationship(test_node_0, '师傅', test_node_2)
node_0_node_3 = Relationship(test_node_0, '师傅', test_node_3)
node_1_node_0 = Relationship(test_node_1, '徒弟', test_node_0)
node_2_node_0 = Relationship(test_node_2, '徒弟', test_node_0)
node_3_node_0 = Relationship(test_node_3, '徒弟', test_node_0)
node_4_node_0 = Relationship(test_node_4, '坐骑', test_node_0)
node_0_node_1['count'] = 1
node_4_node_0['count'] = 1

graph.create(node_0_node_1)
graph.create(node_0_node_2)
graph.create(node_0_node_3)
graph.create(node_1_node_0)
graph.create(node_2_node_0)
graph.create(node_3_node_0)
graph.create(node_4_node_0)

print(graph)
print(test_node_0)
print(test_node_1)
print(test_node_2)
print(test_node_3)
print(test_node_4)
print(node_0_node_1)
print(node_0_node_2)
print(node_0_node_3)
print(node_1_node_0)
print(node_2_node_0)
print(node_3_node_0)
print(node_4_node_0)