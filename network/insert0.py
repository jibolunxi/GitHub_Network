import csv

from util import mysql_pdbc
from util import util


if __name__ == '__main__':
    # 数据库对象
    db_object_GHTorrent = mysql_pdbc.SingletonModel()

    ids = []
    res_ids = []
    util.get_data_from_csv(ids, 'all.csv')
    for id in ids:
        res_ids.append(id[0])

    for num in [8, 9, 24]:
        commits_file = open('F:/GHTorrent/mysql-2019-01-01/commits_' + str(num) + '.csv')  # 打开csv文件
        csv_reader_commits = csv.reader(commits_file)  # 逐行读取csv文件
        index = 0
        insert_sql = "insert into commit VALUES"
        for commit in csv_reader_commits:
            if commit[4] in res_ids:
                print(commit[0], commit[2], commit[3], commit[4], commit[5])
                index += 1
                insert_sql += " (" + str(commit[0]) + "," + str(commit[2]) + "," + str(commit[3]) + "," + str(
                    commit[4]) + ",'" + str(commit[5]) + "')"
                if index == 10000:
                    db_object_GHTorrent.execute(insert_sql)
                    insert_sql = "insert into commit VALUES"
                    index = 0
                else:
                    insert_sql += ","
        if "insert into commit VALUES" != insert_sql:
            db_object_GHTorrent.execute(insert_sql[:-1])