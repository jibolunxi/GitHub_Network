from util import mysql_pdbc
from util.ProcessBar import ProcessBar
from util import util


# 初始化结果文件
def init_res_file(filename):
    node_filename = "nodes_" + filename + ".csv"
    util.print_list_row_to_csv(node_filename, [['id', 'label']], 'w')
    return node_filename



# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object.execute(sql)
    if len(repo) == 0:
        repo_name = ''
    else:
        repo_name = repo[0]['url'][29:]
    return repo_name


if __name__ == '__main__':
    # 数据库对象
    dbObject_GHTorrent = mysql_pdbc.SingletonModel()

    for year in range(2011, 2013):
        print(year)
        pr_time = str(year)
        node_filename = init_res_file(pr_time)
        link_filename = "links_" + pr_time + ".csv"
        # ['Source', 'Target', 'Weight', 'Type'] ['id', 'label']
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


