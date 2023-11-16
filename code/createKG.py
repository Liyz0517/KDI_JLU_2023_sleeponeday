# -*- coding: utf-8 -*-#

from py2neo import Graph, Node, Relationship
import os
import pandas as pd
flower_all = pd.read_csv("./flower_all.csv", sep='\t')
classification = ['花卉类别', '花卉功能', '应用环境', '盛花期_习性', '养护难度']  # 总类别
page_size = [0, 12, 20, 34, 42, 46]  # 页面范围

def flower_entity(graph):
    print("构造花的实体")
    for index, row in flower_all.iterrows():
        flower_properties = row.to_dict()
        flower_node = Node("Flower", **flower_properties)
        graph.create(flower_node)
def root_entity(graph):
    # step 0 总节点
    cql = 'CREATE (:花卉大全{id:\'0\', name:\'花卉大全\'})'
    graph.run(cql)
    for i, c in enumerate(classification):
        cql = '''
                MERGE (a:花卉大全{id:\'%d\', name:\'%s\'})
                MERGE (b {id:'0', name: '花卉大全'}) 
                MERGE (b)-[:划分]-> (a)
                ''' % (i + 1, c)
        graph.run(cql)
    print('step 0 done')
def classification_entity(graph):
    # step 1 类细分
    # ------------------------------------------------------
    content_file = open('data_down/花卉大全.txt', 'r', encoding='utf8')
    for i in range(len(classification)):
        for j in range(page_size[i], page_size[i + 1]):
            name = content_file.readline().split()[-1]
            cql = '''
                    MERGE (a:%s{id:\'%d\', name:\'%s\'})
                    MERGE (b {name: '%s'}) 
                    MERGE (b)-[:划分]-> (a)
                ''' % (classification[i], j, name, classification[i])
            graph.run(cql)
    print('step 1 done')
def variety_entity(graph):
    # step 2 构建品种
    # ------------------------------------------------------
    cql = 'CREATE (:花卉品种{id:\'0\', name:\'花卉品种\'})'
    graph.run(cql)
    file = open('data_down/种类.txt', 'r', encoding='utf8')
    i = 1
    for name in file.readlines():
        if len(name) == 0 or name == '未知':
            continue
        cql = '''
                    MERGE (a:花卉品种{id:\'%d\', name:\'%s\'})
                    MERGE (b {id:'0', name: '花卉品种'})
                    MERGE (b)-[:划分]-> (a)
                ''' % (i, name)
        i += 1
        graph.run(cql)
    print('step 2 done')
def Branch_of_biologyEntity(graph):
    # ------------------------------------------------------
    # step 3 分界
    # ------------------------------------------------------
    belong = ['界', '门', '纲', '目', '科', '属']
    for i, name in enumerate(belong):
        cql = 'CREATE (:生物学分支{id:\'%d\', name:\'%s\'})' % (i, name)
        graph.run(cql)
        if i > 0:
            cql = '''
                MERGE (a {name: '%s'})
                MERGE (b {name: '%s'})
                MERGE (b)-[:划分]-> (a)
            ''' % (belong[i], belong[i-1])
            graph.run(cql)
    file_path = './data_down/科属/'

    for p in belong:
        i = 0
        path = file_path + p + '.txt'
        for line in open(path, 'r', encoding='utf8').readlines():
            line = line.strip()
            if len(line) > 0 and p[0] == line[-1]:
                cql = '''
                    MERGE (a:%s{id:\'%d\', name:\'%s\'})
                    MERGE (b {name: '%s'})
                    MERGE (a)-[:属于]-> (b)
                ''' % (p, i, line, p)
                graph.run(cql)
            i = i + 1
    print('step 3 done')

def createFlower(graph):
    path2_file = open('data_down/花卉大全.txt', 'r', encoding='utf8')
    delt = ['\'', ')', '(', '{', '}']
    er = ['属于', '具有作用', 'accommodate', 'have_habit', 'suit']
    for i, path1 in enumerate(classification):
        for j in range(page_size[i], page_size[i+1]):
            class_name = path2_file.readline().split()[-1]
            flower_path = 'data_down/' + path1 + '/' + class_name + '.csv'
            flower = pd.read_csv(flower_path, sep='\t')
            print(len(flower))
            for k in range(0,len(flower)):
                flower_id = list(flower.iloc[k])[-1]
                target_row = flower_all.loc[flower_all["flower_id"] == flower_id].iloc[0]
                id, name, alias, img_url, category, families, flowerphase, introduce = list(target_row)
                for d in delt:
                    introduce = introduce.replace(d, ' ')
                    name = name.replace(d, ' ')
                    alias = alias.replace(d, ' ')
                cql = '''
                    MERGE (a:花卉{id:\'%s\', name:\'%s\', 别名:\'%s\', 图片:\'%s\', 开花季节:\'%s\', 简介:\'%s\'})
                    MERGE (b:花卉品种{name: '%s'})
                    MERGE (a)-[:归属]-> (b)
                    MERGE (d:%s{name: '%s'})
                    MERGE (a)-[:%s]-> (d)
                ''' % (id, name, alias, img_url, flowerphase, introduce, category, path1, class_name,er[i])
                graph.run(cql)


if __name__ == '__main__':
    graph = Graph("http://127.0.0.1:7474/browser/",
                       user = "neo4j", password = "neo4j", name = "neo4j")
    graph.run('match(n) detach delete n')
    flower_entity(graph)
    root_entity(graph)
    classification_entity(graph)
    variety_entity(graph)
    Branch_of_biologyEntity(graph)
    createFlower(graph)


'''
MATCH (n:花卉 {name: '蜜蜂兰'}) RETURN n; 查找花卉中的蜜蜂兰
MATCH (f:`花卉`)-[:具有作用]->(function:`花卉功能` {name: '吸甲醛花卉'})<-[:accommodate]-(environment:`应用环境` {name: '办公室花卉'})  RETURN f;
MATCH (f:`花卉`)-[:具有作用]->(function:`花卉功能` {name: '吸甲醛花卉'})  RETURN f LIMIT 5  ;
MATCH (f:`花卉`)-[:accommodate]->(environment:`应用环境` {name: '办公室花卉'})  RETURN f limit 5;

查找适合在办公室养的具有吸收甲醛作用的五种花卉
MATCH (flower:`花卉`)-[:具有作用]->(function:`花卉功能` {name: '吸甲醛花卉'})  
WITH flower  
MATCH (flower)-[:accommodate]->(environment:`应用环境` {name: '办公室花卉'})
RETURN flower limit 5; 
MATCH (flower:`花卉`)-[:具有作用]->(function:`花卉功能` {name: '防辐射花卉'})  
WITH flower  
MATCH (flower)-[:accommodate]->(environment:`应用环境` {name: '办公室花卉'})
WITH flower  
MATCH (flower)-[:具有作用]->(environment:`花卉功能` {name: '观赏花卉'})
RETURN flower limit 5;
'''
