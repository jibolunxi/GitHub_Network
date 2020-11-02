from util import mysql_pdbc
import numpy as np
from util.ProcessBar import ProcessBar
from data import data_clean
from util import util


# 初始化结果文件
def init_res_file(filename):
    # 结果输出文件初始化
    link_filename = "links_" + filename + ".csv"
    util.print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
    node_filename = "nodes_" + filename + ".csv"
    util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
    return node_filename, link_filename


# 数据处理
def deal_data(db_object):
    # 获取每年新建的项目数
    data_clean.get_repo_cerate_num_by_year(db_object)
    # 按年分割pr提交
    data_clean.save_repo_pr_coders_by_year(db_object)
    # 获取pr和star数top100的项目并合并
    data_clean.get_top_pr_star_projects_by_year(db_object, 100)


# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object.execute(sql)
    if len(repo) == 0:
        repo_name = ''
    else:
        repo_name = repo[0]['url'][29:]
    return repo_name


# 获取项目id和对应的pr人员
def get_repos_and_pr_coders(db_object, repos_id, pr_coders, sql_table_name, year, month):
    if month != 10:
        sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + sql_table_name + " where created_at < 'year-next_month-01 00:00:00' and created_at > 'year-month-01 00:00:00' group by repo_id"
        next_month = month + 3
        if next_month < 10:
            sql = sql.replace('next_month', '0' + str(next_month))
        else:
            sql = sql.replace('next_month', str(next_month))
        sql = sql.replace('month', '0' + str(month))
    else:
        sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + sql_table_name + " where created_at > 'year-10-01 00:00:00' group by repo_id"
    sql = sql.replace('year', str(year))

    print(sql)

    all_pr_coders = db_object.execute(sql)
    for coders in all_pr_coders:
        if coders['coders'] != '':
            coder = coders['coders'].split(',')
            if len(coder) > 3:
                pr_coders.append(coder)
                repos_id.append(coders['repo_id'])


# 网络构建
def network_build(db_object, repos_id, pr_coders, node_filename, link_filename):
    # 网络节点和边
    res_nodes = []
    res_links = []
    # 项目数
    num = len(repos_id)
    # 进度条
    pb = ProcessBar(num)

    # 计算边权重
    for index_i in range(num):
        # 获取项目id和项目名
        repo_id = repos_id[index_i]
        repo_name = get_name_by_id(db_object, repos_id[index_i])
        if repo_name == '':
            continue

        # 存储节点数据
        node_data = [repo_id, repo_name]
        res_nodes.append(node_data)
        if len(res_nodes) > 10000000:
            util.print_list_row_to_csv(node_filename, res_nodes, 'a')
            res_nodes = []

        # 计算边权重
        for index_j in range(index_i + 1, num):
            weight_count = len(set(pr_coders[index_i]) & set(pr_coders[index_j]))
            if weight_count == 0:
                continue
            link_data = [repos_id[index_i], repos_id[index_j], weight_count, 'undirected']
            res_links.append(link_data)
            # 存储边权重
            if len(res_links) > 100000000:
                util.print_list_row_to_csv(link_filename, res_links, 'a')
                res_links = []

        # 进度条+1
        pb.print_next()

    # 存储剩余节点和边数据
    util.print_list_row_to_csv(link_filename, res_links, 'a')
    util.print_list_row_to_csv(node_filename, res_nodes, 'a')


if __name__ == '__main__':
    # 数据库对象
    dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    # 数据预处理
    # deal_data(dbObject_GHTorrent)

    # 按年构建网络
    for year in range(2010, 2019):
        print(year)
        for month in [1, 4, 7, 10]:
            pr_time = str(year) + '_' + str(month) + '_' + str(month + 2)
            node_filename, link_filename = init_res_file(pr_time)

            # 获取项目id和对应的pr人员
            repos_id = []
            pr_coders = []
            table_name = "pr_coders_" + str(year)
            get_repos_and_pr_coders(dbObject_GHTorrent, repos_id, pr_coders, table_name, year, month)

            # 网络构建
            network_build(dbObject_GHTorrent, repos_id, pr_coders, node_filename, link_filename)
