from util import util
from util import mysql_pdbc
import matplotlib.pyplot as plt
import community
import networkx as nx


# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object.execute(sql)
    if len(repo) == 0:
        repo_name = ''
    else:
        repo_name = repo[0]['url'][29:]
    return repo_name


# 通过边文件获取节点
def get_nodes_by_link(dbObject_GHTorrent, node_filename, link_filename):
    util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
    link_data = []
    node_data = []
    nodes_id = set()
    util.get_data_from_csv(link_data, link_filename)
    link_data = link_data[1:]
    for link in link_data:
        nodes_id.add(link[0])
        nodes_id.add(link[1])
    for node_id in nodes_id:
        node_name = get_name_by_id(dbObject_GHTorrent, node_id)
        node_data.append([node_id, node_name])
    util.print_list_row_to_csv(node_filename, node_data, 'a')


if __name__ == '__main__':
    # 数据库对象
    # dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    for year in range(2011, 2019):
        for month in range(1, 13):
            print(str(year) + "_" + str(month))
            link_filename = "month\\" + "links_" + str(year) + "_" + str(month) + ".csv"
            node_filename = "month\\" + "nodes_" + str(year) + "_" + str(month) + ".csv"
            # 通过边文件获取节点（已完成）
            # get_nodes_by_link(dbObject_GHTorrent, node_filename, link_filename)

            # ['Source', 'Target', 'Weight', 'Type']
            link_data = []
            util.get_data_from_csv(link_data, link_filename)
            link_data = link_data[1:]
            if len(link_data) == 0:
                continue
            # ['id', 'label']
            node_data = []
            util.get_data_from_csv(node_data, node_filename)
            node_data = node_data[1:]

            # Graph构建
            G = nx.Graph()
            for link in link_data:
                G.add_edge(int(link[0]), int(link[1]), weight=int(link[2]))
            partition = community.best_partition(G, partition=None, weight='weight', resolution=1.0)
            # 计算模块度
            mod = community.modularity(partition, G)
            print(mod)

            # drawing
            # values = [partition.get(node) for node in G.nodes()]
            # nx.draw_spring(G, cmap=plt.get_cmap('jet'), node_color=values, node_size=30, with_labels=False)
            # plt.show()







