from util import mysql_pdbc
import numpy as np
from util.ProcessBar import ProcessBar
from data import data_clean
from util import util


FORK_WEIGHT = 30
SAME_OWNER_WEIGHT = 10
SAME_STAR_WEIGHT = 0.5
SAME_CODER_WEIGHT = 10
WEIGHT_THRESHOLD = 20


def build_initial_network(nodes, links, year, dbObject_GHTorrent):
    top_pr_star_filename = "top_100_pr_star_projects_" + str(year) + ".csv"
    read_nodes = []
    util.get_data_from_csv(read_nodes, top_pr_star_filename)
    for node in read_nodes[1:]:
        nodes.append(node[0])

    pr_coder_sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from pr_coders_year group by repo_id"
    pr_coder_sql = pr_coder_sql.replace("year", str(year))
    pr_coders = dbObject_GHTorrent.execute(pr_coder_sql)

    # initial_node_pr_coders = []
    # for pr_code in pr_coders:
    #     if str(pr_code['repo_id']) in nodes:


if __name__ == '__main__':
    dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    # 获取每年新建的项目数
    # data_clean.get_repo_cerate_num_by_year(dbObject_GHTorrent)
    # 按年分割pr提交
    # data_clean.save_repo_pr_coders_by_year(dbObject_GHTorrent)
    # 获取pr和star数top100的项目并合并
    # data_clean.get_top_pr_star_projects_by_year(dbObject_GHTorrent, 100)

    for year in range(2010, 2020):
        # 利用top节点构建初始网络
        # nodes = []
        # links = []
        # build_initial_network(nodes, links, year, dbObject_GHTorrent)

        sql = "select count(*) from pull_request_history where action = 'opened' and created_at > 'year-01-01 00:00:00' and created_at < 'year-07-01 00:00:00' group by actor_id"
        sql = sql.replace("year", str(year))
        res = dbObject_GHTorrent.execute(sql)
        print(len(res))

        sql = "select count(*) from pull_request_history where action = 'opened' and created_at > 'year-07-01 00:00:00' and created_at < 'next-year-01-01 00:00:00' group by actor_id"
        sql = sql.replace("next-year", str(year + 1))
        sql = sql.replace("year", str(year))
        res = dbObject_GHTorrent.execute(sql)
        print(len(res))


    #     for month in range(2):
    #         # 结果输出文件初始化
    #         link_filename = "links_" + str(year) + "_" + str(month * 6 + 1) + "_" + str(month * 6 + 6) + ".csv"
    #         util.print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
    #         node_filename = "nodes_" + str(year) + "_" + str(month * 6 + 1) + "_" + str(month * 6 + 6) + ".csv"
    #         util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
    #
    #         repos_id = []
    #         pr_coders = []
    #         table_name = "pr_coders_" + str(year) + "_" + str(month * 6 + 1) + "_" + str(month * 6 + 6)
    #         print(table_name)
    #         get_repos_and_pr_coders(dbObject, repos_id, pr_coders, table_name)
    #         size = len(repos_id)
    #         print(size)
    #         filter_repos_id = []
    #         filter_pr_coders = []
    #         for index in range(size):
    #             if len(set(pr_coders[index])) > 10:
    #                 filter_repos_id.append(repos_id[index])
    #                 filter_pr_coders.append(set(pr_coders[index]))
    #
    #         print(len(filter_pr_coders))
    #         # 网络构建
    #         network_build(dbObject, filter_repos_id, filter_pr_coders, node_filename, link_filename)
