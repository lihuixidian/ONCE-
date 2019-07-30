# coding=utf-8

#If you find the library or some part of it useful, please contact
#us and you can use this project only you have been allowed.
#lijian021@gmail.com
import time
from collections import Counter, deque
from multiprocessing import cpu_count,freeze_support
from multiprocessing import Pool
import math

class Episode:
    def __init__(self, items):
        # the type of items must be tuple or list or set.
        # self.status = []
        self.item_tuple = items
        self.item_set = set(items)
        item_dict = {}
        item_list = [deque() for i in range(len(items))]
        #print(item_list)
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
def counting_episode(arg):
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
                if k <= current_level:  # and (k > 0 and len(v) < len(item_list[k - 1])) # can be added
                    v.append(i[1])
                    if k == current_level:  # the first show of i[0] in level k
                        current_level += 1
                        if current_level > max_level:  # arrived the last level
                            for lev in range(max_level - 1,-1,-1):
                                for index in range(1, len(item_list[lev])):
                                    if item_list[lev][1] < item_list[lev + 1][0]:
                                        item_list[lev].popleft()
                                    else:
                                        break
                            if item_list[-1][0] - item_list[0][0] <= max_time_interval:
                                [l.clear() for l in item_list]
                                current_level = 0  # reset
                                counter += 1  # couting
                            else:
                                item_list_0_0 = item_list[0][0]
                                item_list[0].popleft()
                                while len(item_list[0]) and item_list[0][0] == item_list_0_0:
                                    item_list[0].popleft()
                                
                                if len(item_list[0]):
                                    flag = False
                                    for it in range(1,len(item_list)):  # from the level 1, not level 0
                                        if flag:
                                          item_list[it].clear()
                                        else:
                                            temp = item_list[it - 1][0]
                                            while 1:
                                                if len(item_list[it]) and item_list[it][0] <= temp:
                                                    item_list[it].popleft()
                                                    if len(item_list[it]) == 0:
                                                        current_level = it
                                                        flag = True
                                                        break
                                                else:
                                                    break
                                else:
                                    [l.clear() for l in item_list]
                                    current_level = 0
                        else:
                            break
    return [item.item_tuple, counter]


# @profile
def counting_episode_overlap(arg):
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
                if k <= current_level:  # and (k > 0 and len(v) < len(item_list[k - 1])) # can be added
                    v.append(i[1])
                    if k == current_level:  # the first show of i[0] in level k
                        current_level += 1
                        if current_level > max_level:  # arrived the last level
                            for lev in range(max_level - 1,-1,-1):
                                for index in range(1, len(item_list[lev])):
                                    if item_list[lev][1] < item_list[lev + 1][0]:
                                        item_list[lev].popleft()
                                    else:
                                        break
                            if item_list[-1][0] - item_list[0][0] <= max_time_interval:
                                counter += 1  # couting
                            #    [l.clear() for l in item_list]
                            #    current_level = 0  # reset
                            #else:
                            item_list_0_0 = item_list[0][0]
                            item_list[0].popleft()
                            while len(item_list[0]) and item_list[0][0] == item_list_0_0:
                                item_list[0].popleft()
                            if len(item_list[0]):
                                flag = False
                                for it in range(1,len(item_list)):  # from the level 1, not level 0
                                    if flag:
                                      item_list[it].clear()
                                    else:
                                        temp = item_list[it - 1][0]
                                        while 1:
                                            if len(item_list[it]) and item_list[it][0] <= temp:
                                                item_list[it].popleft()
                                                if len(item_list[it]) == 0:
                                                    current_level = it
                                                    flag = True
                                                    break
                                            else:
                                                break
                            else:
                                [l.clear() for l in item_list]
                                current_level = 0
                        else:
                            break
    return [item.item_tuple, counter]


class count_episode_time_constraint:
    def show(self):
        d = dict()
        #  d['data'] = self.data
        d['size'] = self.size
        d['freq_L'] = self.freq_L
        d['min_sup'] = self.min_sup
        d['min_sup_val'] = self.min_sup_val
        d['time_thr'] = self.time_thr
        d['L1'] = self.L1
        d['ck'] = self.ck
        print(d)

    def __init__(self, t, min_sup=0.2, data_seq=list()):
        self.data = data_seq  # series
        self.size = len(data_seq)  # series size
        self.min_sup = min_sup  # min support(0-1)
        self.min_sup_val = math.ceil(min_sup * self.size)  # abstract min support
        self.time_thr = t  # time
        self.L1 = {}  # episode
        self.freq_L = {}# frequent episode set
        self.ck = {}


    # @profile
    def count(self, data_list, c_k, t, sup, multiprocess=False, processes=None):
        if multiprocess:
            pool = Pool(processes)
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = pool.map_async(counting_episode, iter_data_set)
            #pool_result = pool.map_async(counting_episode_overlap, iter_data_set)
            pool.close()
            pool.join()
            result = {tuple(i[0]): i[1] for i in pool_result.get() if i and i[1] > sup}
        else:
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = [counting_episode(i) for i in iter_data_set]
            #pool_result = [counting_episode_overlap(i) for i in iter_data_set]
            result = {tuple(i[0]): i[1] for i in pool_result if i and i[1] > sup}    
        self.freq_L.update(result)
        return result
    #  @profile
    def do(self, filename, ck, multiprocess=False, processes=3):
        time_thr = self.time_thr
        min_sup = self.min_sup
        min_sup_val = self.min_sup_val
        candidate = set()
        for target in ck:
            for event in target:
                candidate.add(event)
        filtered_data = [t for t in self.data if (t[0]) in candidate]
        #print('target episdoe size: %d' % len(list(ck)[0]))
        #time_start = time.time()
        l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes)
        #print("countIns cost:", (time.time() - time_start))
        #print(l_last, file=open("%d_%.3f_%s_%d.%s" % (33, min_sup, filename,
        #len(l_last), 'exp1'), 'w'))
        return l_last


    def do_mining(self, filename, multiprocess=True, processes=3, method=1):
        time_thr = self.time_thr
        min_sup = self.min_sup
        min_sup_val = self.min_sup_val
        gen = self.gen
        time_start = time.time()
        freq_eve = self.find_frequent_1_events()
        print('one item cost:', time.time() - time_start)
        print(freq_eve, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, 1, len(freq_eve), 'ck'), 'w'))
        l_last = self.L1

        candidate = set()
        for target in l_last:
            for event in target:
                candidate.add(event)

        filtered_data = [t for t in self.data if (t[0]) in candidate]
        print(l_last, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, 1, len(l_last), 'l_last'), 'w'))
        max_i = 0
        if min_sup_val==0:
            max_i = self.size
        else:
            max_i = int(self.size / min_sup_val) + 1
        for i in range(2, max_i):
            if len(l_last) == 0:
                return l_last
            print('len(last) * len(L1):%d * %d' % (len(l_last), len(self.L1)))
            time_start = time.time()
            ck = gen(l_last.keys(), self.L1.keys())  # 合并形成新的频繁项集.
            print('generator cost:', time.time() - time_start)
            print(ck, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, i, len(ck), 'ck'), 'w'))
            if len(ck) == 0:
                return l_last
            print('新的频繁项集共 %d 项，每个有 %d 个元素' % (len(ck), i))
            time_start = time.time()
            l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes)
            print("countIns cost:", time.time() - time_start)
            print(l_last, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, i, len(l_last), 'l_last'), 'w'))
        return l_last


    def find_frequent_1_events(self):  # 查找长度为1的频繁序列
        freq_eve = dict(Counter((x[0]) for x in self.data))
        for event in freq_eve.keys():
            if freq_eve[event] >= self.min_sup_val:  # 过滤掉小于最小支持度阈值的物品
                self.L1[(event,)] = freq_eve[event]
        self.freq_L = self.L1.copy()
        return freq_eve


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


class count_episode_time_constraint_with_overlap:
    def show(self):
        d = dict()
        #  d['data'] = self.data
        d['size'] = self.size
        d['freq_L'] = self.freq_L
        d['min_sup'] = self.min_sup
        d['min_sup_val'] = self.min_sup_val
        d['time_thr'] = self.time_thr
        d['L1'] = self.L1
        d['ck'] = self.ck
        print(d)

    def __init__(self, t, min_sup=0.2, data_seq=[(False,[])]):
        if data_seq[0]:
            self.countFunc = counting_episode_overlap
        else:
            self.countFunc = counting_episode
        data_seq = data_seq[1]
        self.data = data_seq  # series
        self.size = len(data_seq)  # series size
        self.min_sup = min_sup  # min support(0-1)
        self.min_sup_val = 0  # abstract min support
        self.time_thr = t  # time
        self.L1 = {}  # episode
        self.freq_L = {}# frequent episode set
        self.ck = {}
        

    # @profile
    def count(self, data_list, c_k, t, sup, multiprocess=False, processes=None):
        func = self.countFunc
        if multiprocess:
            pool = Pool(processes)
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = pool.map_async(func, iter_data_set)
            pool.close()
            pool.join()
            result = {tuple(i[0]): i[1] for i in pool_result.get() if i and i[1] > sup}
        else:
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = [func(i) for i in iter_data_set]
            #result_detail = {tuple(i[0]): i[1] for i in pool_result if i and
            #len(i[1]) > sup}
            result = {tuple(i[0]): len(i[1]) for i in pool_result if i and len(i[1]) > sup}    
        self.freq_L.update(result)
        return pool_result
     # @profile
    def countNumber(self, data_list, c_k, t, sup, multiprocess=False, processes=None):
        func = self.countFunc
        if multiprocess:
            pool = Pool(processes)
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = pool.map_async(func, iter_data_set)
            pool.close()
            pool.join()
            result = {tuple(i[0]): i[1] for i in pool_result.get() if i and i[1] > sup}
        else:
            iter_data_set = [(Episode(i), data_list, t, sup) for i in c_k]
            pool_result = [func(i) for i in iter_data_set]
            result = {tuple(i[0]): len(i[1]) for i in pool_result if i and len(i[1]) > sup}    
        self.freq_L.update(result)
        return result
    #  @profile
    def do(self, filename, ck, multiprocess=False, processes=3):
        time_thr = self.time_thr
        min_sup = self.min_sup
        min_sup_val = self.min_sup_val
        candidate = set()
        for target in ck:
            for event in target:
                candidate.add(event)
        filtered_data = [t for t in self.data if (t[0]) in candidate]
        #print('target episdoe size: %d' % len(list(ck)[0]))
        #time_start = time.time()
        l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes)
        #print("countIns cost:", (time.time() - time_start))
        #print(l_last, file=open("%d_%.3f_%s_%d.%s" % (33, min_sup, filename,
        #len(l_last), 'exp1'), 'w'))
        return l_last


def test():
    #files = ['total']
    files = ['without1all']
    for file in files:
        print(file)
        f = open(file + '.txt')
        s = f.readline()
        f.close()
        s = eval(s)
                
        f = open('e.txt')
        e = f.readline()
        f.close()
        e = eval(e)

        time_start = time.time()
        ap = count_episode_time_constraint_with_overlap(100000, 0.0, (True,s))
        #print(ap.do(file, {(2, 68, 65)}))
        #e = {(2, 68, 65),(2, 68, 65, 44, 22, 12, 44, 22, 8),(2, 68, 65, 44, 22, 12, 44),(2, 68, 65, 44, 59),(2, 13, 56)}
        #e = {(2, 68, 65, 44, 22, 12, 44, 22, 8)}
        print(ap.do(file, e,multiprocess=True, processes=cpu_count()-1))
        #print(ap.do_mining(file, multiprocess=False, processes=cpu_count() - 1))
        print("original version counting cost:", (time.time() - time_start))
        #(2, 68, 65, 44, 22, 12, 44, 22, 8),(2, 68, 65, 44, 22, 12, 44),(2, 68,
        #65, 44, 59),(2, 68, 65)
if __name__ == '__main__':
    freeze_support()
    test()
