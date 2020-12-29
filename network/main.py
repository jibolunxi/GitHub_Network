import threading

from data import deal_data
from util import mysql_pdbc
from util import util
import csv
import datetime


# 通过id获取代码库名称
def get_name_by_id(db_object_GHTorrent, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object_GHTorrent.execute(sql)
    if len(repo) == 0:
        repo_name = ''
    else:
        repo_name = repo[0]['url'][29:]
    return repo_name


def network_build(link_filename, node_filename, db_object_GHTorrent, sql_table_name, start_time, end_time):

    ################################################################################################## 获取项目和开发者数据
    # 获取项目id和开发者列表
    select_sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders FROM " + sql_table_name + " WHERE created_at >= '" + start_time + "' AND created_at < '" + end_time + "' group by repo_id"
    select_res = db_object_GHTorrent.execute(select_sql)
    # 存储项目id
    repos_id = []
    # 存储项目开发人员id
    pr_coders = []
    for pr in select_res:
        if pr['coders'] != '':
            coders = pr['coders'].split(',')
            pr_coders.append(coders)
            repos_id.append(pr['repo_id'])

    ######################################################################################################## 计算边并存储
    # 项目数
    num = len(repos_id)
    # 边、节点存储
    res_links = []
    nodes = set()
    # 计算边权重
    for index_i in range(num):
        for index_j in range(index_i + 1, num):
            weight_count = len([x for x in pr_coders[index_i] if x in pr_coders[index_j]])
            if weight_count < 1:
                continue
            res_links.append([repos_id[index_i], repos_id[index_j], weight_count, 'undirected'])
            nodes.add(repos_id[index_i])
            nodes.add(repos_id[index_j])

            # 间歇存储边数据
            if len(res_links) > 100000000:
                util.print_list_row_to_csv(link_filename, res_links, 'a')
                res_links = []

    # 存储剩余边数据
    util.print_list_row_to_csv(link_filename, res_links, 'a')

    ####################################################################################################### 计算节点并存储
    res_nodes = []
    # 获取项目名称并存储
    for repo_id in nodes:
        repo_name = get_name_by_id(db_object_GHTorrent, repo_id)
        res_nodes.append([repo_id, repo_name])
    # 存储节点数据
    util.print_list_row_to_csv(node_filename, res_nodes, 'a')


if __name__ == '__main__':
    # 数据库对象
    db_object_GHTorrent = mysql_pdbc.SingletonModel()
    # 数据获取与预处理
    # deal_data.pull_requests_coders_insert(db_object_GHTorrent)

    # 网络构建-按月构建网络
    # for year in range(2011, 2019):
    #     for month in range(1, 13):
    #
    #         # 边、节点文件
    #         link_filename = "data_1.0\\month\\" + "links_" + str(year) + "_" + str(month) + ".csv"
    #         node_filename = "data_1.0\\month\\" + "nodes_" + str(year) + "_" + str(month) + ".csv"
    #         # 边、节点文件初始化
    #         util.print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
    #         util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
    #
    #         # 网络构建
    #         sql_table_name = "pull_requests_merged_coders"
    #         start_time, end_time = util.calculate_start_end_time(year, month)
    #
    #         # 开始执行时间
    #         start = datetime.datetime.now()
    #         # 不同项目之间相同的开发者数
    #         network_build(link_filename, node_filename, db_object_GHTorrent, sql_table_name, start_time, end_time)
    #         # 执行结束时间
    #         end = datetime.datetime.now()
    #         print(str(year) + "年" + str(month) + "月", end - start)





