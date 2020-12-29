from util import util
from util import mysql_pdbc
import matplotlib.pyplot as plt
import community
import networkx as nx


if __name__ == '__main__':
    # 数据库对象
    # dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    for year in range(2011, 2019):
        for month in range(1, 13):
            link_filename = "data_2.0\\month\\" + "links_" + str(year) + "_" + str(month) + ".csv"
            node_filename = "data_2.0\\month\\" + "nodes_" + str(year) + "_" + str(month) + ".csv"

            # 获取边 ['Source', 'Target', 'Weight', 'Type']
            link_data = []
            util.get_data_from_csv(link_data, link_filename)
            link_data = link_data[1:]
            if len(link_data) == 0:
                continue
            # 获取节点 ['id', 'label']
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

            # 模块度，平均集聚系数，直径，最短路径，度中心性
            # print(mod, '\t', nx.average_clustering(G), '\t', nx.diameter(G), '\t', nx.average_shortest_path_length(G), '\t', nx.degree_centrality(G))
            # 社区数，模块度，平均集聚系数
            # print(max(partition.values()), '\t', mod, '\t', nx.average_clustering(G))
            # 社区数
            print(str(year) + "_" + str(month), '\t', len(link_data), '\t', len(node_data), '\t', max(partition.values()), '\t', mod, '\t', nx.average_clustering(G))

            # drawing
            # values = [partition.get(node) for node in G.nodes()]
            # nx.draw_spring(G, cmap=plt.get_cmap('jet'), node_color=values, node_size=30, with_labels=False)
            # plt.show()







