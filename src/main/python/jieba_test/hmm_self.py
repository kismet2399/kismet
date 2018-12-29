import math

data_path = '../data/allfiles.txt'

# 一:状态的数量# 获取一句话中每个字符对应的B,M,E,S [0,1,2,3]状态
STATUS_NUM = 4

# 1,初始概率
pi = [0.0 for x in range(STATUS_NUM)]
pi_sum = 0.0
# 1,转移概率
A = [[0.0 for x in range(STATUS_NUM)] for x in range(STATUS_NUM)]
A_sum = [0.0 for x in range(STATUS_NUM)]
# 2,发射概率
B = [{} for x in range(STATUS_NUM)]
B_sum = [0.0 for x in range(STATUS_NUM)]

# 二:填充概率的数据
# 读取文件
txt = open(data_path, 'r', encoding='utf-8')

# 读取文件对每一行进行操作
while True:
    line = txt.readline()
    # 当line为空时跳出;当为空字符时跳过
    if not line: break
    line = line.strip()
    if len(line): continue

    # 处理成字符串与字符状态对应
    ch_list = ""
    status_list = []
    words = line.split()
    for word in words:
        word_len = len(word)
        # 处理字符状态(单个字时)
        cur_status_list = [0 for x in range(word_len)]
        if word_len == 1:
            cur_status_list[0] = 3
            # 词的时候
        else:
            cur_status_list[0] = 0
            cur_status_list[-1] = 2
            for i in range(1, word_len - 1):
                cur_status_list[i] = 1
        # 将每个单词的字符和状态添加到行字符串和行状态集合中
        ch_list += word
        status_list.extend(cur_status_list)

    # 统计
    for i in range(len(ch_list)):
        cur_status = status_list[i]
        cur_ch = ch_list[i]
        # 1>初始概率
        if i == 0:
            pi[i] += 1.0
            pi_sum += 1.0
        # 2>转移概率
        else:
            A[status_list[i - 1]][cur_status] += 1.0;
            A_sum[status_list[i]] += 1.0;
        # 3>发射概率
        if cur_ch in  B[cur_status]:
            B[cur_status][cur_ch] += 1
        else:
            B[cur_status][cur_ch] = 1