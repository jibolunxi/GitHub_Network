import csv


FILE_DIRECTORY = 'D:\\workspace\\小论文-网络结构演化\\实验结果\\network_data\\'


# 字典类型输出到文件
def print_dist_lines_to_csv(filename, data, headers, type):
    filename = FILE_DIRECTORY + filename
    with open(filename, type, newline='')as f:
        f_csv = csv.DictWriter(f, headers)
        f_csv.writeheader()
        f_csv.writerows(data)


# 按行输出到文件
def print_list_row_to_csv(filename, data, type):
    filename = FILE_DIRECTORY + filename
    with open(filename, type, newline='')as f:
        f_csv = csv.writer(f)
        for d in data:
            f_csv.writerow(d)


# 读取文件
def get_data_from_csv(read_data, filename):
    filename = FILE_DIRECTORY + filename
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for read in reader:
            read_data.append(read)


# 通过年月计算起始和结束时间
def calculate_start_end_time(year, month):
    start_time = 'year-month-01 00:00:00'
    start_time = start_time.replace('year', str(year))
    if month < 10:
        start_time = start_time.replace('month', '0' + str(month))
    else:
        start_time = start_time.replace('month', str(month))

    end_time = 'year-month-01 00:00:00'
    next_month = month + 1
    if next_month < 10:
        end_time = end_time.replace('year', str(year))
        end_time = end_time.replace('month', '0' + str(next_month))
    elif next_month < 13:
        end_time = end_time.replace('year', str(year))
        end_time = end_time.replace('month', str(next_month))
    else:
        end_time = end_time.replace('year', str(year + 1))
        end_time = end_time.replace('month', str(1))

    return start_time, end_time


def intersect(nums1,nums2):
    record={}
    result=[]
    for i in nums1:
        # 注意python3.x用contains()方法取代has_key()
        if(record.__contains__(i)):
            record[i]+=1                      #将字典的key值数+1
        else:
            record[i]=1                       #将i加入字典中 key为1
    for j in nums2:
        if(record.__contains__(j) and record[j]>0):
            record[j]-=1                      #i的key值减1
            result.append(j)
    return result




# sql = "select repo_id id, count(repo_id) count from watchers where created_at > '2013" \
#           "-01-01 00:00:00' group by repo_id ORDER BY count DESC"
#     star_count_projects = db_object_GHTorrent.execute(sql)
#     JavaScript = []
#     Java = []
#     Python = []
#     PHP = []
#     Cplus = []
#     Cplusplus = []
#     TypeScript = []
#     Shell = []
#     C = []
#     Ruby = []
#     for repo in star_count_projects:
#         print(repo['count'])
#         if(repo['count'] < 100):
#             break
#         sql = "select id, language from projects where id="+str(repo['id'])
#         project = db_object_GHTorrent.execute(sql)[0]
#         if len(JavaScript) < 2000 and str(project['language']).lower() == 'javascript':
#             JavaScript.append(str(project['id']))
#         elif len(Java) < 2000 and str(project['language']).lower() == 'java':
#             Java.append(str(project['id']))
#         elif len(Python) < 2000 and str(project['language']).lower() == 'python':
#             Python.append(str(project['id']))
#         elif len(PHP) < 2000 and str(project['language']).lower() == 'php':
#             PHP.append(str(project['id']))
#         elif len(Cplus) < 2000 and str(project['language']).lower() == 'c++':
#             Cplus.append(str(project['id']))
#         elif len(Cplusplus) < 2000 and str(project['language']).lower() == 'c#':
#             Cplusplus.append(str(project['id']))
#         elif len(TypeScript) < 2000 and str(project['language']).lower() == 'typescript':
#             TypeScript.append(str(project['id']))
#         elif len(Shell) < 2000 and str(project['language']).lower() == 'shell':
#             Shell.append(str(project['id']))
#         elif len(C) < 2000 and str(project['language']).lower() == 'c':
#             C.append(str(project['id']))
#         elif len(Ruby) < 2000 and str(project['language']).lower() == 'ruby':
#             Ruby.append(str(project['id']))
#
#     util.print_list_row_to_csv("JavaScript.txt", JavaScript)
#     util.print_list_row_to_csv("Java.txt", Java)
#     util.print_list_row_to_csv("Python.txt", Python)
#     util.print_list_row_to_csv("PHP.txt", PHP)
#     util.print_list_row_to_csv("C++.txt", Cplus)
#     util.print_list_row_to_csv("C#.txt", Cplusplus)
#     util.print_list_row_to_csv("C.txt", C)
#     util.print_list_row_to_csv("Shell.txt", Shell)
#     util.print_list_row_to_csv("TypeScript.txt", TypeScript)
#     util.print_list_row_to_csv("Ruby.txt", Ruby)


