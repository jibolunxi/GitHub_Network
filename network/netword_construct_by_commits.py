from datetime import datetime, time
from util import mysql_pdbc
from util import util

FORK_WEIGHT = 100
OWNER_WEIGHT = 10


# 通过id获取代码库
def get_pro_by_id(db_object_GHTorrent, pro_id):
    sql = "SELECT * FROM projects WHERE id = " + str(pro_id)
    project = db_object_GHTorrent.execute(sql)[0]
    return [project['id'], project['url'], project['owner_id'], project['language'], project['created_at'], project['forked_from'], project['deleted']]


# 通过id和year获取commits
def get_commits_by_id_and_year(db_object_GHTorrent, pro_id, year):
    sql = "SELECT * FROM commit WHERE project_id=pro_id AND created_at < 'next_year-01-01 00:00:00' " \
          "AND created_at > 'year-01-01 00:00:00' "
    sql = sql.replace('next_year', str(year + 1))
    sql = sql.replace('year', str(year))
    sql = sql.replace('pro_id', str(pro_id))
    commits = db_object_GHTorrent.execute(sql)
    res_commits = []
    for commit in commits:
        res_commits.append(commit['author_id'])
        res_commits.append(commit['committer_id'])

    return res_commits


# 以fork关系确定边
def find_link_by_forked(projects, pro_ids, res_links):
    for project in projects:
        if project[5] in pro_ids:
            res_links.append([project[0], project[5], FORK_WEIGHT, 'undirected'])


# 以owner关系确定边
def find_link_by_owner(projects, res_links):
    length = len(projects)
    for i in range(length):
        for j in range(i + 1, length):
            if projects[i][2] == projects[j][2]:
                res_links.append([projects[i][0], projects[j][0], OWNER_WEIGHT, 'undirected'])


# 以协作关系确定边
def find_link_by_commit(db_object_GHTorrent, year, projects, res_links):
    length = len(projects)
    pro_commits = []
    for project in projects:
        pro_commits.append(get_commits_by_id_and_year(db_object_GHTorrent, project[0], year))
    for i in range(length):
        for j in range(i + 1, length):
            weight = len(set(pro_commits[i]) & set(pro_commits[j]))
            if weight > 0:
                res_links.append([projects[i][0], projects[j][0], weight, 'undirected'])


if __name__ == '__main__':
    # 数据库对象
    db_object_GHTorrent = mysql_pdbc.SingletonModel()

    # 获取筛选的所有项目
    all_pro_ids = []
    all_projects = []
    util.get_data_from_csv(all_pro_ids, 'all.csv')
    for pro_id in all_pro_ids:
        all_projects.append(get_pro_by_id(db_object_GHTorrent, pro_id[0]))

    for year in range(2016, 2019):
        print(2016)
        res_node_labels = []
        res_links = []

        projects = []
        pro_ids = []
        for project in all_projects:
            if project[4] < datetime(year + 1, 1, 1, 0, 0, 0):
                projects.append(project)
                pro_ids.append(project[0])
                res_node_labels.append([project[0], project[1][29:]])

        find_link_by_forked(projects, pro_ids, res_links)
        # find_link_by_owner(projects, res_links)
        find_link_by_commit(db_object_GHTorrent, year, projects, res_links)

        # # 边、节点文件初始化
        # link_filename = "data_3.0\\" + "links_" + str(year) + ".csv"
        # node_filename = "data_3.0\\" + "nodes_" + str(year) + ".csv"
        # util.print_list_row_to_csv(link_filename, [['Source', 'Target', 'Weight', 'Type']], 'w')
        # util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
        #
        # util.print_list_row_to_csv(link_filename, res_links, 'a')
        # util.print_list_row_to_csv(node_filename, res_node_labels, 'a')