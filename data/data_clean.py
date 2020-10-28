import _thread
from util import mysql_pdbc
from util import util
MIN_STAR = 5


# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from has_pr_projects where id = " + str(repo_id)
    repo = db_object.execute(sql)
    if len(repo) == 0:
        repo_name = ''
    else:
        repo_name = repo[0]['name']
    return repo_name


# 检测repo1和repo2是否为fork关系、所有者是否相同，返回总权值
def fork_or_owner_relation(db_object, repo1_id, repo2_id):
    weight = 0
    sql = "select * from has_pr_projects where id = " + str(repo1_id)
    repo1 = db_object.execute(sql)[0]
    sql = "select * from has_pr_projects where id = " + str(repo2_id)
    repo2 = db_object.execute(sql)[0]

    if repo1['forked_from'] is not None and repo1['forked_from'] == repo2_id:
        weight += FORK_WEIGHT
    if repo2['forked_from'] is not None and repo2['forked_from'] == repo1_id:
        weight += FORK_WEIGHT

    if repo1['owner_id'] == repo2['owner_id']:
        weight += SAME_OWNER_WEIGHT

    return weight


# sql获取每年每月项目id和对应的pr提交人员
def get_repos_and_pr_coders(db_object, repos_id, pr_coders, table_name):
    sql = "SELECT repo_id, GROUP_CONCAT(user_id) coders from " + table_name + " group by repo_id"

    all_pr_coders = db_object.execute(sql)

    for coders in all_pr_coders:
        if coders['coders'] != '':
            repos_id.append(coders['repo_id'])
            if ',' in coders['coders']:
                pr_coders.append(coders['coders'].split(','))
            else:
                pr_coders.append(coders['coders'])


# 每年改成每月
def year_to_month(db_object, year):
    table_name = "pr_coders_year"
    table_name = table_name.replace("year", str(year))

    new_table_name = table_name + "_1_6"
    # create_table = "CREATE TABLE " + new_table_name + " LIKE pr_coders_2010"
    # db_object.execute(create_table)
    # print(create_table)

    sql = "insert into " + new_table_name + "(repo_id, user_id, created_at) SELECT * from " + table_name + " where created_at < '" + str(year) + "-07-01 00:00:00' and created_at > '" + str(year) + "-01-01 00:00:00' "

    db_object.execute(sql)

    new_table_name = table_name + "_7_12"
    # create_table = "CREATE TABLE " + new_table_name + " LIKE pr_coders_2010"
    # db_object.execute(create_table)
    # print(create_table)
    sql = "insert into " + new_table_name + "(repo_id, user_id, created_at) SELECT * from " + table_name + " where created_at < '" + str(
        year + 1) + "-01-01 00:00:00' and created_at > '" + str(year) + "-07-01 00:00:00' "
    print(sql)
    db_object.execute(sql)


# 网络构建
def network_build(dbObject, repos_id, pr_coders, node_filename, link_filename):
    res_nodes = []
    res_links = []
    num = len(repos_id)
    pb = ProcessBar(num)
    for index_i in range(num):
        pb.print_next()

        repo_id = repos_id[index_i]
        repo_name = get_name_by_id(dbObject, repos_id[index_i])
        if repo_name == '':
            continue
        node_data = [repo_id, repo_name]
        res_nodes.append(node_data)
        if len(res_nodes) > 10000000:
            util.print_list_row_to_csv(node_filename, res_nodes, 'a')
            res_nodes = []

        for index_j in range(index_i + 1, num):
            weight_count = 0
            # weight_count += fork_or_owner_relation(dbObject, repos_id[index_i], repos_id[index_j])
            weight_count += len(set(pr_coders[index_i]) & set(pr_coders[index_j]))

            if weight_count >= 20:
                link_data = [repos_id[index_i], repos_id[index_j], weight_count, 'undirected']
                res_links.append(link_data)

                if len(res_links) > 100000000:
                    util.print_list_row_to_csv(link_filename, res_links, 'a')
                    res_links = []

    util.print_list_row_to_csv(link_filename, res_links, 'a')
    util.print_list_row_to_csv(node_filename, res_nodes, 'a')


# 删除star数小于MIN_STAR的项目
def delete_star_less_than_min(dbObject):
    sql = "select repo_id id, count(repo_id) count from watchers group by repo_id"
    star_count_projects = dbObject.execute(sql)
    delete_id = '('
    for repo in star_count_projects:
        if repo['count'] < MIN_STAR:
            delete_id = delete_id + str(repo['id']) + ','
    delete_id = delete_id[:-1] + ')'
    print(delete_id)
    # delete_project = "delete from projects where id in (select id from less)"
    dbObject.delete(table='projects', where='id in ' + delete_id)
    # delete_pr = "delete from pull_requests where head_repo_id in (select id from less)"
    dbObject.delete(table='pull_requests', where='head_repo_id in ' + delete_id)
    # delete_pr = "delete from pull_requests where base_repo_id in (select id from less)"
    dbObject.delete(table='pull_requests', where='base_repo_id in ' + delete_id)
    # delete_pr_history = "delete from pull_request_history where pull_request_id not in(select id from projects);"
    dbObject.delete(table='pull_request_history', where='pull_request_id not in(select id from pull_requests)')


def table_init(dbObject_GHTorrent, year):
    # part 0
    drop_sql = 'DROP TABLE IF EXISTS `pr_coders_year`'
    drop_sql = drop_sql.replace('year', str(year))
    dbObject_GHTorrent.execute(drop_sql)
    create_sql = 'CREATE TABLE pr_coders_year(`repo_id` INT(11), `user_id` VARCHAR(25),`created_at` TIMESTAMP);'
    create_sql = create_sql.replace('year', str(year))
    dbObject_GHTorrent.execute(create_sql)


def pr_to_coders(dbObject, year):
    sql = "select pull_request_id, created_at, actor_id from pull_request_history where created_at < 'next_year-01-01 00:00:00' " \
          "and created_at > 'year-01-01 00:00:00' "
    sql = sql.replace('next_year', str(year + 1))
    sql = sql.replace('year', str(year))
    pr_history_list = dbObject.execute(sql)
    size = len(pr_history_list)

    table_name = 'pr_coders_year'
    table_name = table_name.replace('year', str(year))

    count = 0
    index = 0
    sql = "insert into " + table_name + " values"
    for pr_history in pr_history_list:
        count += 1
        index += 1
        if pr_history['actor_id'] is not None:
            pr_sql = "select id, base_repo_id from pull_requests where id = " + str(pr_history['pull_request_id'])
            pr = dbObject.execute(pr_sql)[0]
            if count == size:
                sql += " (" + str(pr['base_repo_id']) + "," + str(pr_history['actor_id']) + ",'" + str(
                    pr_history['created_at']) + "')"
                dbObject.execute(sql)
            elif index < 10000:
                sql += " (" + str(pr['base_repo_id']) + "," + str(pr_history['actor_id']) + ",'" + str(
                    pr_history['created_at']) + "')"
                sql += ","
            else:
                sql += " (" + str(pr['base_repo_id']) + "," + str(pr_history['actor_id']) + ",'" + str(
                    pr_history['created_at']) + "')"
                print(sql)
                dbObject.execute(sql)
                sql = "insert into " + table_name + " values"
                index = 0


def owner_id_to_coders(dbObject, year):
    sql = "select * from projects where forked_from != '' " \
          "and created_at < 'next_year-01-01 00:00:00' " \
          "and created_at > 'pre_year-09-01 00:00:00' "
    sql = sql.replace('next_year', str(year + 1))
    sql = sql.replace('pre_year', str(year - 1))
    projects = dbObject.execute(sql)
    size = len(projects)
    print(sql, '#####', size)

    table_name = 'pr_coders_year'
    table_name = table_name.replace('year', str(year))

    count = 0
    index = 0
    sql = "insert into " + table_name + " values"
    for repo in projects:
        count += 1
        index += 1

        if count == size:
            sql += " (" + str(repo['forked_from']) + "," + str(repo['owner_id']) + ",'" + str(repo['created_at']) + "')"
            dbObject.execute(sql)
        elif index < 10000:
            sql += " (" + str(repo['forked_from']) + "," + str(repo['owner_id']) + ",'" + str(repo['created_at']) + "')"
            sql += ","
        else:
            sql += " (" + str(repo['forked_from']) + "," + str(repo['owner_id']) + ",'" + str(repo['created_at']) + "')"
            dbObject.execute(sql)
            sql = "insert into " + table_name + " values"
            index = 0


def save_repo_pr_coders_by_year(dbObject_GHTorrent):
    for year in range(2010, 2019):
        print(year)
        table_init(dbObject_GHTorrent, year)
        # owner_id_to_coders(dbObject, year)
        pr_to_coders(dbObject_GHTorrent, year)


def get_repo_cerate_num_by_year(dbObject_GHTorrent):
    for year in range(2010, 2019):
        sql = "select count(*) from projects where created_at < 'next_year-01-01 00:00:00' " \
            "and created_at > 'year-01-01 00:00:00' "
        sql = sql.replace('next_year', str(year + 1))
        sql = sql.replace('year', str(year))
        res = dbObject_GHTorrent.execute(sql)
        print(res)


def get_top_pr_star_projects_by_year(dbObject_GHTorrent, num):
    for year in range(2010, 2019):
        top_pr_star_filename = "top_" + str(num) + "_pr_star_projects_" + str(year) + ".csv"
        util.print_list_row_to_csv(top_pr_star_filename, [['repo_id']], 'w')
        pr_sql = 'select repo_id, count(*) pr_count from pr_coders_year group by repo_id'
        pr_sql = pr_sql.replace('year', str(year))
        pr_repos = dbObject_GHTorrent.execute(pr_sql)

        star_sql = "select repo_id, count(*) star_count from watchers where created_at < 'next_year-01-01 00:00:00' and created_at > 'year-01-01 00:00:00' group by repo_id"
        star_sql = star_sql.replace('next_year', str(year + 1))
        star_sql = star_sql.replace('year', str(year))
        star_repos = dbObject_GHTorrent.execute(star_sql)

        id_res = []

        pr_repo_res = []
        for repo in pr_repos:
            pr_repo_res.append([repo['repo_id'], repo['pr_count']])
        pr_repo_res = sorted(pr_repo_res, key=lambda x: x[1], reverse=True)[:num]
        for pr_repo in pr_repo_res:
            id_res.append(pr_repo[0])

        star_repo_res = []
        for repo in star_repos:
            star_repo_res.append([repo['repo_id'], repo['star_count']])
        star_repo_res = sorted(star_repo_res, key=lambda x: x[1], reverse=True)[:num]
        for star_repo in star_repo_res:
            id_res.append(star_repo[0])

        print_res = []
        for id in set(id_res):
            print_res.append([id])

        util.print_list_row_to_csv(top_pr_star_filename, print_res, 'a')




