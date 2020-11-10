import csv
from util import mysql_pdbc


# 按行输出到文件
def print_list_row_to_csv(filename, data, type):
    filename = filename
    with open(filename, type, newline='')as f:
        f_csv = csv.writer(f)
        for d in data:
            f_csv.writerow(d)


# 初始化结果文件
def init_res_file(filename):
    # 结果输出文件初始化
    link_filename = "links_" + filename + ".csv"
    print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
    return link_filename


# 获取项目id和对应的pr人员
def get_repos_and_pr_coders(db_object, repos_id, pr_coders, sql_table_name):
    sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + sql_table_name + " group by repo_id"
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
            print_list_row_to_csv(link_filename, res_links, 'a')
            res_links = []

    # 存储剩余节点和边数据
    print_list_row_to_csv(link_filename, res_links, 'a')


if __name__ == '__main__':
    # 数据库对象
    dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    # 按年构建网络
    for year in range(2010, 2019):
        print(year)
        pr_time = str(year)
        link_filename = init_res_file(pr_time)

        # 获取项目id和对应的pr人员
        repos_id = []
        pr_coders = []
        table_name = "pr_coders_" + str(year)
        get_repos_and_pr_coders(dbObject_GHTorrent, repos_id, pr_coders, table_name)

        # 网络构建
        network_build(repos_id, pr_coders, link_filename)
