import csv


FILE_DIRECTORY = 'D:\\code\\Gephi\\data\\'


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


