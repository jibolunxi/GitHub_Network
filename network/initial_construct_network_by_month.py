import csv
from util import mysql_pdbc
from util import util

# # 按行输出到文件
# def print_list_row_to_csv(filename, data, type):
#     filename = filename
#     with open(filename, type, newline='')as f:
#         f_csv = csv.writer(f)
#         for d in data:
#             f_csv.writerow(d)


# 初始化结果文件
def init_res_file(filename):
    # 结果输出文件初始化
    link_filename = "links_" + filename + ".csv"
    util.print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
    return link_filename


# 获取项目id和对应的pr人员
def get_repos_and_pr_coders(db_object, repos_id, pr_coders, sql_table_name, year, month):
    if month != 12:
        sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + sql_table_name + " where created_at < 'year-next_month-01 00:00:00' and created_at > 'year-month-01 00:00:00' group by repo_id"
        next_month = month + 1
        if next_month < 10:
            sql = sql.replace('next_month', '0' + str(next_month))
        else:
            sql = sql.replace('next_month', str(next_month))
        if month < 10:
            sql = sql.replace('month', '0' + str(month))
        else:
            sql = sql.replace('month', str(month))
    else:
        sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + sql_table_name + " where created_at > 'year-12-01 00:00:00' group by repo_id"
    sql = sql.replace('year', str(year))

    all_pr_coders = db_object.execute(sql)

    for coders in all_pr_coders:
        if coders['coders'] != '':
            coder = coders['coders'].split(',')
            if len(coder) > 1:
                pr_coders.append(coder)
                repos_id.append(coders['repo_id'])

    print(len(repos_id))


# 网络构建
def network_build(repos_id, pr_coders, link_filename):
    # 网络节点和边
    res_links = []
    # 项目数
    num = len(repos_id)

    # 计算边权重
    for index_i in range(num):

        # 计算边权重
        for index_j in range(index_i + 1, num):
            weight_count = len(set(pr_coders[index_i]) & set(pr_coders[index_j]))
            if weight_count < 1:
                continue
            link_data = [repos_id[index_i], repos_id[index_j], weight_count, 'undirected']
            res_links.append(link_data)

        # 存储边权重
        if len(res_links) > 100000000:
            util.print_list_row_to_csv(link_filename, res_links, 'a')
            res_links = []

    # 存储剩余节点和边数据
    util.print_list_row_to_csv(link_filename, res_links, 'a')


if __name__ == '__main__':
    # 数据库对象
    dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    # 按月构建网络
    for year in range(2011, 2019):
        for month in range(1, 13):
            pr_time = str(year) + "_" + str(month)
            link_filename = init_res_file(pr_time)
            print(link_filename)

            # 获取项目id和对应的pr人员
            repos_id = []
            pr_coders = []
            table_name = "pr_coders_" + str(year)
            get_repos_and_pr_coders(dbObject_GHTorrent, repos_id, pr_coders, table_name, year, month)

            # 网络构建
            network_build(repos_id, pr_coders, link_filename)
