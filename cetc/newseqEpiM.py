# coding=gbk

import time
from collections import Counter, deque
from multiprocessing import Pool
import math
# A unified view of Auto-mata-based algorithms for Frequent Episode Discovery
# Kuo-Yu Huang and Chi-a-Hui Chang. Efficient mining of frequent episodes from
#  complex sequences.InformationSystems, 33(1):96�C114, Mar 2008
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
        self.LoE = loe  # ��ǰ�����ź�����
        self.QoT = [[] for i in range(len(loe))]  # ��ǰ��������й��ɵĶ�����
        self.tag = 0  # ��ǰ��Ч��
        self.ct = 0  # ��ǰ���ļ���ֵ
        self.l = len(loe)  # ��ǰ���Ĳ���

    @staticmethod
    def filter_list(lt, thr):  # ���˱�������lt�����thr��ʱ���
        return list(filter(lambda z: z > thr, lt))

    # ��⵱ǰ�������Ƿ�����ʱ��Լ��
    # @profile
    def find_seq(self, t):
        q0 = self.QoT[self.l - 1]  # ������һ������
        max_time = q0[0]  # �����е����һλΪĿ��Ƭ�ε�ǰ���ʱ��
        a = deque([max_time])  # ���и��ڵ�����ʱ�乹�ɴ�ȷ��ʱ�������
        for j in range(self.l - 2, -1, -1):  # �ӵ�����2�㿪ʼ
            for i in range(len(self.QoT[j]) - 1, -1, -1):  # ��ÿ��������λ�ÿ�ʼ
                if self.QoT[j][i] < max_time:  # ��С��maxT�ĵ�һ��
                    max_time = self.QoT[j][i]  # ����maxT
                    a.appendleft(self.QoT[j][i])  # ��ȷ��ʱ���������λ����ò�ʱ���
                    break  # �ò㴦����ϣ�������һ��
        if a[- 1] - a[0] <= t:  # ����ȷ��ʱ��������Ƿ�����Լ������
            self.ct += 1
            self.QoT = [[] for i in range(self.l)]  # �������Լ�������������ֵ��1�����в����ȫ�ÿգ����һ���Ψһ�ź�һ��������ģ�
        else:
            if len(a) != len(self.QoT):
                self.show()
                print('����:�����������ҵ���ƥ��ģʽȴ������Լ��ʱ���е���ʱ����')
                print('a:', a)
                self.QoT = [[] for i in range(self.l)]
            else:
                #  ���򣬸�����е�������a�������źţ������������Լ����λ��
                self.QoT = list(map(QTree.filter_list, self.QoT, a))
                for i in range(1, self.l):  # �ӵ�һ�㿪ʼ���
                    if len(self.QoT[i - 1]) and len(self.QoT[i]):  # ����ϲ�ͱ��㲻Ϊ��
                        # ������㲻Ϊ�գ��ҵ�һ��Ԫ�صĳ���ʱ��С�ڵ����ϲ�ĵ�һ��Ԫ��
                        while len(self.QoT[i]) and self.QoT[i][0] <= self.QoT[i - 1][0]:
                            self.QoT[i].pop(0)  # ������Ԫ�أ��䲻�����߼�
                    else:
                        break  # ��ɵ�����
        for i in range(self.l):  # ���µ�ǰ��Ч��ı��λ
            if len(self.QoT[i]) == 0:
                self.tag = i
                return


#  @profile
def counting(arg):
    tree, date, t, sup = arg
    find_seq = tree.find_seq
    event_set = tree.event_set
    date = [i for i in date if i[0] in event_set]  # ԭ����ֻ����һ��ɨ�輴�ɣ����i����event_set�У���ô�Ͳ�����������
    for i in date:  # ÿ��event
        if tree.tag == 0:  # ���������׸�����Ϊ�գ���������Ϊ��
            if i[0] == tree.LoE[0]:  # ��ǰ��eventƥ���QTree�еĵ�һ���ڵ�
                tree.QoT[0].append(i[1])  # �����µļ�¼
                tree.tag += 1  # QTree����Ч������1.
        else:  # �������ǿ�
            for k in range(tree.tag + 1):  # �����������зǿ�����
                if i[0] == tree.LoE[k]:  # �������������һ�ź�ƥ��
                    tree.QoT[k].append(i[1])  # �����в����µ�ʱ���
                    if k == tree.tag:  # ���������Ϊ��ǰ���ǿ����У����������Ǹ��µ���һ����
                        tree.tag += 1
                        if tree.tag == tree.l:  # �����������Ǹ��µ����ֵ������¼�����
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
        print(d)

    def __init__(self, t, min_sup=0.2, data_seq=list()):
        self.data = data_seq  # Ŀ������
        self.size = len(data_seq)  # Ŀ�����г���
        self.min_sup = min_sup  # ��С֧�ֶȵ������ֵ
        self.min_sup_val = math.ceil(min_sup * self.size)  # ��С֧�ֶȾ�����ֵ
        self.time_thr = t  # ʱ��Լ��
        self.L1 = {}  # Ƶ���¼�������Ϊ1Ƶ��Ƭ�Σ�
        self.freq_L = {}  # Ƶ��Ƭ���ֵ�

    def find_frequent_1_events(self):  # ���ҳ���Ϊ1��Ƶ������
        freq_eve = dict(Counter((x[0]) for x in self.data))
        for event in freq_eve.keys():
            if freq_eve[event] >= self.min_sup_val:  # ���˵�С����С֧�ֶ���ֵ����Ʒ
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
                for i in range(len(fre_epi1) + 1):  # ���ѭ��������Ҫ�����λ�ã������µ�Ƶ����
                    c = list(fre_epi1)
                    c.insert(i, fre_event[0])  # ����һ��Ԫ��
                    t_c = tuple(c)
                    if t_c in ck:  # �Ѿ���ck������
                        continue  # �Ѿ���¼��,������һ��
                    for j in range(length_fre_epi1 + 1):  # �ڲ�ѭ��������Ƶ����
                        if i == j:  # ���ǲ����λ��
                            continue  # ����
                        cc = c.copy()
                        cc.pop(j)  # ������j�����γ���һ����Ƶ����
                        if tuple(cc) not in last_l_keys:  # �����һ����Ƶ��������Ƶ����
                            break  # �˴����ɵ�Ƶ����������������������������һ��
                    if j == length_fre_epi1:  # ��Ƶ��������Ƶ����
                        ck.add(t_c)  # ���ӵ������
        return ck

    def count_ins(self, date_list, c_k, t, sup):
        result = {}  # ���صĽ��:Ƶ����
        #  length ΪƵ������Ƶ����Ĵ�С
        cps = [QTree(i) for i in c_k]  # ����Ƶ�������ֱ���
        for i in date_list:  # ÿ��event
            # ��ͬ�ļ�����, !!!����ɲ���!!!
            for j in cps:
                if j.tag == 0:  # ���������׸�����Ϊ�գ���������Ϊ��
                    if i[0] == j.LoE[0]:  # ��ǰ��eventƥ���QTree�еĵ�һ���ڵ�
                        j.QoT[0].append(i[1])  # �����µļ�¼
                        j.tag += 1  # QTree����Ч������1.
                else:  # �������ǿ�
                    for k in range(j.tag + 1):  # �����������зǿ�����
                        if i[0] == j.LoE[k]:  # �������������һ�ź�ƥ��
                            j.QoT[k].append(i[1])  # �����в����µ�ʱ���
                            if k == j.tag:  # ���������Ϊ��ǰ���ǿ����У����������Ǹ��µ���һ����
                                j.tag += 1
                                if j.tag == j.l:  # �����������Ǹ��µ����ֵ������¼�����
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
            ck = gen(l_last.keys(), self.L1.keys())  # �ϲ��γ��µ�Ƶ���.
            print('generator cost:', time.time() - time_start)
            print(ck, file=open("%d_%.3f_%s_%d_%d.%s" % (time_thr, min_sup, filename, i, len(ck), 'ck'), 'w'))
            if len(ck) == 0:
                return
            print('�µ�Ƶ����� %d �ÿ���� %d ��Ԫ��' % (len(ck), i))
            time_start = time.time()
            l_last = self.count(filtered_data, ck, self.time_thr, self.min_sup_val, multiprocess, processes, method)
            print("countIns cost:", time.time() - time_start)
            print(l_last, file=open("%d_%.3f_%s_%d_%d.%s"
                                    % (time_thr, min_sup, filename, i, len(l_last), 'l_last'), 'w'))
        return


def test():
    # files = [ '����', 'ǭ����', 'ͭ�ʵ���', 'ǭ��']
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
