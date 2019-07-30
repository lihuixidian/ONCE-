# coding=gbk

import time
from collections import Counter, deque
from multiprocessing import Pool
import math
# A unified view of Auto-mata-based algorithms for Frequent Episode Discovery
# Kuo-Yu Huang and Chi-a-Hui Chang. Efficient mining of frequent episodes from
#  complex sequences.InformationSystems, 33(1):96–114, Mar 2008
# Discovering frequent episodes and learning Hidden Markov Models: A formal connection


class QTree:
    LoE, QoT = [], []
    tag, ct, l = 0, 0, 0

    def show(self):
        d = dict()
        d['LoE'] = str(self.LoE)
        d['QoT'] = str(self.QoT)
        d['tag'] = str(self.tag)
        d['ct'] = str(self.ct)
        d['l'] = str(self.l)
        print(str(d))

    def __init__(self, loe):
        self.event_set = set(loe)
        self.LoE = loe  # 当前树的信号序列
        self.QoT = [[] for i in range(len(loe))]  # 当前树各层队列构成的队列组
        self.tag = 0  # 当前有效层
        self.ct = 0  # 当前树的计数值
        self.l = len(loe)  # 当前树的层数

    @staticmethod
    def filter_list(lt, thr):  # 过滤保留队列lt里大于thr的时间戳
        return list(filter(lambda z: z > thr, lt))

    # 检测当前各队列是否满足时间约束
    # @profile
    def find_seq(self, t):
        q0 = self.QoT[self.l - 1]  # 倒数第一个序列
        max_time = q0[0]  # 该序列的最后一位为目标片段当前最后时间
        a = deque([max_time])  # 树中各节点的最后时间构成待确定时间戳序列
        for j in range(self.l - 2, -1, -1):  # 从倒数第2层开始
            for i in range(len(self.QoT[j]) - 1, -1, -1):  # 从每层队列最后位置开始
                if self.QoT[j][i] < max_time:  # 从小于maxT的第一个
                    max_time = self.QoT[j][i]  # 更新maxT
                    a.appendleft(self.QoT[j][i])  # 待确定时间戳序列首位插入该层时间戳
                    break  # 该层处理完毕，进行下一层
        if a[- 1] - a[0] <= t:  # 检测待确认时间戳序列是否满足约束条件
            self.ct += 1
            self.QoT = [[] for i in range(self.l)]  # 如果满足约束条件，则计数值加1，所有层队列全置空（最后一层的唯一信号一定是最晚的）
        else:
            if len(a) != len(self.QoT):
                self.show()
                print('出错:当从序列中找到待匹配模式却不满足约束时进行调整时出错。')
                print('a:', a)
                self.QoT = [[] for i in range(self.l)]
            else:
                #  否则，各层队列弹出满足a的所有信号，即最可能满足约束的位置
                self.QoT = list(map(QTree.filter_list, self.QoT, a))
                for i in range(1, self.l):  # 从第一层开始检查
                    if len(self.QoT[i - 1]) and len(self.QoT[i]):  # 如果上层和本层不为空
                        # 如果本层不为空，且第一个元素的出现时间小于等于上层的第一个元素
                        while len(self.QoT[i]) and self.QoT[i][0] <= self.QoT[i - 1][0]:
                            self.QoT[i].pop(0)  # 弹出该元素，其不符合逻辑
                    else:
                        break  # 完成调整。
        for i in range(self.l):  # 更新当前有效层的标记位
            if len(self.QoT[i]) == 0:
                self.tag = i
                return


#  @profile
def counting(arg):
    tree, date, t, sup = arg
    find_seq = tree.find_seq
    event_set = tree.event_set
    date = [i for i in date if i[0] in event_set]  # 原本是只进行一次扫描即可，如果i不在event_set中，那么就不处理；否则处理。
    for i in date:  # 每个event
        if tree.tag == 0:  # 计数器的首个序列为空，即计数器为空
            if i[0] == tree.LoE[0]:  # 当前的event匹配此QTree中的第一个节点
                tree.QoT[0].append(i[1])  # 插入新的纪录
                tree.tag += 1  # QTree的有效层数加1.
        else:  # 计数器非空
            for k in range(tree.tag + 1):  # 检测计数器所有非空序列
                if i[0] == tree.LoE[k]:  # 如果该序列与下一信号匹配
                    tree.QoT[k].append(i[1])  # 该序列插入新的时间戳
                    if k == tree.tag:  # 如果该序列为当前最后非空序列，则计数器标记更新到下一序列
                        tree.tag += 1
                        if tree.tag == tree.l:  # 如果计数器标记更新到最大值，则更新计数器
                            find_seq(t)
    return [tree.LoE, tree.ct]


class Item:
    def __init__(self, items):
        # the type of items must be tuple or list or set.
        # self.status = []
        self.item_tuple = items
        self.item_set = set(items)
        item_dict = {}
        item_list = [deque() for i in range(len(items))]
        for i, value in enumerate(items):
            if item_dict.get(value):
                item_dict[value][i] = item_list[i]
            else:
                item_dict[value] = {i: item_list[i]}
        self.item_list = item_list
        self.item_dict = item_dict
        # self.counter = 0
        # self.current_enabled_level = 0
        self.max_level = len(items) - 1

    def __str__(self):
        return {
            # "counter": self.counter,
            # "current_enable_level": self.current_enabled_level,
            "item_dict:": self.item_dict,
            "item_set": self.item_set,
            'item_tuple': self.item_tuple,
            'item_list': self.item_list,
            "max_level": self.max_level}


# @profile
def counting_item(arg):
    item, date, max_time_interval, sup = arg
    item_set = item.item_set
    item_dict = item.item_dict
    current_level = 0
    counter = 0
    max_level = item.max_level
    item_list = item.item_list
    for i in date:
        if i[0] in item_set:  # useful data.
            for k, v in item_dict[i[0]].items():  # update show time of i[0]
                if k <= current_level:  # and (k > 0 and len(v) < len(item_list[k - 1]))  # can be added
                    # if k > 0 and i[1] in item_dict[i[0]][k-1]:
                    #     continue
                    v.append(i[1])
                    if k == current_level:  # the first show of i[0] in level k
                        current_level += 1
                        if current_level > max_level:  # arrived the last level
                            max_time = item_list[-1][0]
                            a = deque([max_time])  # 树中各节点的最后时间构成待确定时间戳序列
                            for j in range(max_level - 1, -1, -1):  # 从倒数第2层开始
                                item_list_j = item_list[j]
                                for it in range(len(item_list_j) - 1, -1, -1):  # 从每层队列最后位置开始
                                    if item_list_j[it] < max_time:  # 从小于maxT的第一个
                                        max_time = item_list_j[it]  # 更新maxT
                                        a.appendleft(item_list_j[it])  # 待确定时间戳序列首位插入该层时间戳
                                        break  # 该层处理完毕，进行下一层
                            if a[- 1] - a[0] <= max_time_interval:  # 检测待确认时间戳序列是否满足约束条件
                                for l in item_list:
                                    l.clear()  # 所有层队列全置空
                                current_level = 0  # 当前层为0
                                counter += 1  # 计数值加1
                            else:  # 否则，各层队列弹出满足a的所有信号，即最可能满足约束的位置
                                for index, value in enumerate(a):
                                    item_list_index = item_list[index]
                                    while 1:
                                        if item_list_index and item_list_index[0] <= value:
                                            item_list_index.popleft()
                                        else:
                                            break
                                for it in range(1, max_level + 1):  # 从第一层开始检查
                                    if len(item_list[it - 1]) and len(item_list[it]):  # 如果上层和本层不为空
                                        # 如果本层不为空，且第一个元素的出现时间小于等于上层的第一个元素
                                        while len(item_list[it]) and item_list[it][0] <= item_list[it - 1][0]:
                                            item_list[it].popleft()  # 弹出该元素，其不符合逻辑
                                    else:
                                        break  # 完成调整。
                            for l in range(len(item_list)):
                                    if len(item_list[l]) == 0:
                                        current_level = l
                                        break
                        else:
                            break
    return [item.item_tuple, counter]


class Apriori:
    def show(self):
        d = dict()
        #  d['data'] = self.data
        d['size'] = self.size
        d['freq_L'] = self.freq_L
        d['min_sup'] = self.min_sup
        d['min_sup_val'] = self.min_sup_val
        d['time_thr'] = self.time_thr
        d['L1'] = self.L1
        print(d)

    def __init__(self, t, min_sup=0.2, data_seq=list()):
        self.data = data_seq  # 目标序列
        self.size = len(data_seq)  # 目标序列长度
        self.min_sup = min_sup  # 最小支持度的相对阈值
        self.min_sup_val = math.ceil(min_sup * self.size)  # 最小支持度绝对阈值
        self.time_thr = t  # 时间约束
        self.L1 = {}  # 频繁事件（长度为1频繁片段）
        self.freq_L = {}  # 频繁片段字典

    def find_frequent_1_events(self):  # 查找长度为1的频繁序列
        freq_eve = dict(Counter((x[0]) for x in self.data))
        for event in freq_eve.keys():
            if freq_eve[event] >= self.min_sup_val:  # 过滤掉小于最小支持度阈值的物品
                self.L1[(event,)] = freq_eve[event]
        self.freq_L = self.L1.copy()
        return freq_eve

    #  @profile
    @staticmethod
    def gen(last_l_keys, l_keys):
        ck = set()
        for fre_event in l_keys:
            for fre_epi1 in last_l_keys:
                length_fre_epi1 = len(fre_epi1)
                for i in range(len(fre_epi1) + 1):  # 外层循环控制需要插入的位置，生成新的频繁集
                    c = list(fre_epi1)
                    c.insert(i, fre_event[0])  # 插入一个元素
                    t_c = tuple(c)
                    if t_c in ck:  # 已经在ck里面了
                        continue  # 已经记录了,生成下一个
                    for j in range(length_fre_epi1 + 1):  # 内层循环生成子频繁集
                        if i == j:  # 这是插入的位置
                            continue  # 跳过
                        cc = c.copy()
                        cc.pop(j)  # 弹出第j个，形成其一个子频繁集
                        if tuple(cc) not in last_l_keys:  # 如果有一个子频繁集不是频繁的
                            break  # 此次生成的频繁集不符合条件，跳出，生成下一个
                    if j == length_fre_epi1:  # 子频繁集都是频繁的
                        ck.add(t_c)  # 增加到结果集
        return ck

    def count_ins(self, date_list, c_k, t, sup):
        result = {}  # 返回的结果:频繁集
        #  length 为频繁集中频繁项的大小
        cps = [QTree(i) for i in c_k]  # 遍历频繁集，分别建树
        for i in date_list:  # 每个event
            # 不同的计数器, !!!这里可并行!!!
            for j in cps:
                if j.tag == 0:  # 计数器的首个序列为空，即计数器为空
                    if i[0] == j.LoE[0]:  # 当前的event匹配此QTree中的第一个节点
                        j.QoT[0].append(i[1])  # 插入新的纪录
                        j.tag += 1  # QTree的有效层数加1.
                else:  # 计数器非空
                    for k in range(j.tag + 1):  # 检测计数器所有非空序列
                        if i[0] == j.LoE[k]:  # 如果该序列与下一信号匹配
                            j.QoT[k].append(i[1])  # 该序列插入新的时间戳
                            if k == j.tag:  # 如果该序列为当前最后非空序列，则计数器标记更新到下一序列
                                j.tag += 1
                                if j.tag == j.l:  # 如果计数器标记更新到最大值，则更新计数器
                                    j.find_seq(t)
                if j.ct >= sup:
                    self.freq_L[tuple(j.LoE)] = j.ct
                    result[tuple(j.LoE)] = j.ct
        return result

    # @profile
    def count(self, data_list, c_k, t, sup, multiprocess=True, processes=None, method=1):
        # counting_item((Item(('D', 'BT', 'T')), data_list, t, sup))
        if multiprocess:
            pool = Pool(processes)
            if method:
                iter_data_set = [(Item(i), data_list, t, sup) for i in c_k]
                pool_result = pool.map_async(counting_item, iter_data_set)
            else:
                iter_data_set = [(QTree(i), data_list, t, sup) for i in c_k]
                pool_result = pool.map_async(counting, iter_data_set)
            pool.close()
            pool.join()
            result = {tuple(i[0]): i[1] for i in pool_result.get() if i and i[1] > sup}
        else:
            if method:
                iter_data_set = [(Item(i), data_list, t, sup) for i in c_k]
                pool_result = [counting_item(i) for i in iter_data_set]
            else:
                iter_data_set = [(QTree(i), data_list, t, sup) for i in c_k]
                pool_result = [counting(i) for i in iter_data_set]
            result = {tuple(i[0]): i[1] for i in pool_result if i and i[1] > sup}
        self.freq_L.update(result)
        return result

    #  @profile
    def do(self, filename, multiprocess=True, processes=3, method=1):
        time_thr = self.time_thr
        min_sup = self.min_sup
        min_sup_val = self.min_sup_val
        gen = self.gen
        time_start = time.time()
        freq_eve = self.find_frequent_1_events()
        print('one item cost:', time.time() - time_start)
        print(freq_eve, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, 1, len(freq_eve), 'ck'), 'w'))
        l_last = self.L1
        filtered_data = [t for t in self.data if (t[0],) in l_last.keys()]
        print(l_last, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, 1, len(l_last), 'l_last'), 'w'))
        for i in range(2, int(self.size / min_sup_val) + 1):
            if len(l_last) == 0:
                return
            print('len(last) * len(L1):%d * %d' % (len(l_last), len(self.L1)))
            time_start = time.time()
            ck = gen(l_last.keys(), self.L1.keys())  # 合并形成新的频繁项集.
            print('generator cost:', time.time() - time_start)
            print(ck, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, i, len(ck), 'ck'), 'w'))
            if len(ck) == 0:
                return
            print('新的频繁项集共 %d 项，每个有 %d 个元素' % (len(ck), i))
            time_start = time.time()
            l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes, method)
            print("countIns cost:", time.time() - time_start)
            print(l_last, file=open("%d_%.3f_%s_%d_%d.%s"
                                    % (time_thr, min_sup, filename, i, len(l_last), 'l_last'), 'w'))
        return


def test():
    # files = [ '汇总', '黔东南', '铜仁地区', '黔南']
    files = ['without1all']
    for file in files:
        print(file)
        f = open(file + '.txt')
        s = f.readline()
        f.close()
        s = eval(s)
        ap = Apriori(1000, 0.01, s)
        ap.do(file, multiprocess=True, processes=3, method=1)

if __name__ == '__main__':
    test()
