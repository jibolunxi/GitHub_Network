from util import mysql_pdbc
from util.ProcessBar import ProcessBar


if __name__ == '__main__':
    dbObject = mysql_pdbc.SingletonModel()
    sql = 'select base_repo_id,count(*) pr_num from pull_requests group by base_repo_id'
    projects = dbObject.execute(sql)
    num = len(projects)
    print(num)
    pb = ProcessBar(num)

    table_name = 'has_pr_projects'
    count = 0
    index = 0
    sql = "insert into " + table_name + " values"
    for project in projects:
        count += 1
        index += 1
        pb.print_next()
        if project['pr_num'] > 20:
            se_sql = 'select * from projects where id = ' + str(project['base_repo_id'])
            items = dbObject.execute(se_sql)
            if len(items) > 0:
                item = items[0]
                if item['forked_from'] == None:
                    item['forked_from'] = 0
                item['pr_number'] = project['pr_num']
                sql += " (" + str(item['id']) + "," + str(item['owner_id']) + ",'" + item['name'] + "','" + str(
                    item['created_at']) + "'," + str(item['forked_from']) + "," + str(item['deleted']) + "," + str(item['pr_number']) + ")"
                if count == num:
                    dbObject.execute(sql)
                elif index < 10000:
                    sql += ","
                else:
                    dbObject.execute(sql)
                    sql = "insert into " + table_name + " values"
                    index = 0

