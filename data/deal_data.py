from util import ProcessBar


# 合并pull_requests和pull_request_history表内容
def pull_requests_coders_insert(db_object):

    # part1 插入pr_id，repo_id，actor_id和创建时间
    get_pr_his_sql_1 = "SELECT pull_request_id, created_at, actor_id FROM pull_request_history WHERE action = 'opened'"
    pr_history_res = db_object.execute(get_pr_his_sql_1)

    count = 0
    index = 0
    size = len(pr_history_res)
    insert_sql = "REPLACE INTO pull_requests_coders VALUES"

    for pr_history in pr_history_res:
        count += 1
        index += 1
        print(count/size)

        if pr_history['pull_request_id'] is None:
            continue
        if pr_history['actor_id'] is None:
            continue
        if pr_history['created_at'] is None:
            continue

        pr_sql = "SELECT id, base_repo_id FROM pull_requests WHERE id = " + str(pr_history['pull_request_id'])
        pr = db_object.execute(pr_sql)[0]
        if pr is None or pr['base_repo_id'] is None:
            continue

        insert_sql += " (" + str(pr_history['pull_request_id']) + "," + str(pr['base_repo_id']) + "," + str(
            pr_history['actor_id']) + ",'" + str(pr_history['created_at']) + "', 0)"

        if count == size:
            db_object.execute(insert_sql)
        elif index < 10000:
            insert_sql += ","
        else:
            db_object.execute(insert_sql)
            insert_sql = "REPLACE INTO pull_requests_coders VALUES"
            index = 0

    # part2 获取merge id
    get_pr_his_sql_2 = "SELECT pull_request_id FROM pull_request_history WHERE action = 'merged'"
    pr_history_res = db_object.execute(get_pr_his_sql_2)

    count = 0
    index = 0
    size = len(pr_history_res)
    insert_sql = "REPLACE INTO pull_requests_merged_coders VALUES"
    for pr_history in pr_history_res:
        count += 1
        index += 1
        print(count / size)

        if pr_history['pull_request_id'] is None:
            continue
        get_pr_sql = "SELECT pull_request_id, repo_id, user_id, created_at FROM pull_requests_coders WHERE pull_request_id=" + str(pr_history['pull_request_id'])
        pr_res = db_object.execute(get_pr_sql)[0]
        if pr_res is None:
            continue

        insert_sql += " (" + str(pr_res['pull_request_id']) + "," + str(pr_res['repo_id']) + "," + str(
            pr_res['user_id']) + ",'" + str(pr_res['created_at']) + "', 1)"

        if count == size:
            db_object.execute(insert_sql)
        elif index < 10000:
            insert_sql += ","
        else:
            db_object.execute(insert_sql)
            insert_sql = "REPLACE INTO pull_requests_merged_coders VALUES"
            index = 0
