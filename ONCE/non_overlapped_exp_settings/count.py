# coding=gbk

import time
from collections import Counter, deque
from multiprocessing import Pool
import math
# A unified view of Auto-mata-based algorithms for Frequent Episode Discovery
# Kuo-Yu Huang and Chi-a-Hui Chang. Efficient mining of frequent episodes from
#  complex sequences.InformationSystems, 33(1):96�C114, Mar 2008
# Discovering frequent episodes and learning Hidden Markov Models: A formal connection




class Item:
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
                            a = deque([max_time])  # ���и��ڵ�����ʱ�乹�ɴ�ȷ��ʱ�������
                            for j in range(max_level - 1, -1, -1):  # �ӵ�����2�㿪ʼ
                                item_list_j = item_list[j]
                                for it in range(len(item_list_j) - 1, -1, -1):  # ��ÿ��������λ�ÿ�ʼ
                                    if item_list_j[it] < max_time:  # ��С��maxT�ĵ�һ��
                                        max_time = item_list_j[it]  # ����maxT
                                        a.appendleft(item_list_j[it])  # ��ȷ��ʱ���������λ����ò�ʱ���
                                        break  # �ò㴦����ϣ�������һ��
                            if a[- 1] - a[0] <= max_time_interval:  # ����ȷ��ʱ��������Ƿ�����Լ������
                                for l in item_list:
                                    l.clear()  # ���в����ȫ�ÿ�
                                current_level = 0  # ��ǰ��Ϊ0
                                counter += 1  # ����ֵ��1
                            else:  # ���򣬸�����е�������a�������źţ������������Լ����λ��
                                for index, value in enumerate(a):
                                    item_list_index = item_list[index]
                                    while 1:
                                        if item_list_index and item_list_index[0] <= value:
                                            item_list_index.popleft()
                                        else:
                                            break
                                for it in range(1, max_level + 1):  # �ӵ�һ�㿪ʼ���
                                    if len(item_list[it - 1]) and len(item_list[it]):  # ����ϲ�ͱ��㲻Ϊ��
                                        # ������㲻Ϊ�գ��ҵ�һ��Ԫ�صĳ���ʱ��С�ڵ����ϲ�ĵ�һ��Ԫ��
                                        while len(item_list[it]) and item_list[it][0] <= item_list[it - 1][0]:
                                            item_list[it].popleft()  # ������Ԫ�أ��䲻�����߼�
                                    else:
                                        break  # ��ɵ�����
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
        d['ck']=self.ck
        print(d)

    def __init__(self, t, min_sup=0.2, data_seq=list()):
        self.data = data_seq  # Ŀ������
        self.size = len(data_seq)  # Ŀ�����г���
        self.min_sup = min_sup  # ��С֧�ֶȵ������ֵ
        self.min_sup_val = 0  # ��С֧�ֶȾ�����ֵ
        self.time_thr = t  # ʱ��Լ��
        self.L1 = {}  # Ƶ���¼�������Ϊ1Ƶ��Ƭ�Σ�
        self.freq_L = {}# Ƶ��Ƭ���ֵ�
        self.ck= {}


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
    def do(self, filename, ck, multiprocess=True, processes=3, method=1):
        time_thr = self.time_thr
        min_sup = self.min_sup
        min_sup_val = self.min_sup_val
        candidate = set()
        for target in ck:
            for event in target:
                candidate.add(event)
        filtered_data = [t for t in self.data if (t[0]) in candidate]
        print('target episdoe ����Ϊ%d'%len(ck))
        time_start = time.time()
        l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes, method)
        print("countIns cost:", time.time() - time_start)
        print(l_last, file=open("%d_%.3f_%s_%d.%s"
                                    % (time_thr, min_sup, filename, len(l_last), 'l_last'), 'w'))
        return


def test():
    # files = [ '����', 'ǭ����', 'ͭ�ʵ���', 'ǭ��']
    files = ['ͭ�ʵ���']
    for file in files:
        print(file)
        f = open(file + '.txt')
        s = f.readline()
        f.close()
        s = eval(s)
        ap = Apriori(100000000000, 0.015, s)
        ap.do(file, {('O', 'O', 'M'),('BU', 'O', 'AA'),('M', 'AI', 'M'),('M', 'M', 'O', 'M', 'O', 'BU')}, multiprocess=True, processes=3, method=1)

if __name__ == '__main__':
    test()
