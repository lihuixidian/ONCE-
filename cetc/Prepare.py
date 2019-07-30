from collections import deque, Iterable
def combine(l1,l2):
    if len(l1)==0:
        return l2
    if len(l2)==0:
        return l1
    result=deque()
    i=0
    j=0
    len_l1 = len(l1)
    len_l2 = len(l2)
    while i<len_l1 and j<len_l2:
        if l1[i][0]<l2[j][0]:
            result.append(l1[i])
            i=i+1
        elif l1[i][0]>l2[j][0]:
            result.append(l2[j])
            j=j+1
        else:
            result.append(l1[i])
            i=i+1
            j=j+1
    while i<len_l1:
        result.append(l1[i])
        i=i+1
    while j<len_l2:
        result.append(l2[j])
        j=j+1
    r = result
    result = [r[0]]
    for i in range(1,len(r)):
        if r[i][0]>result[-1][-1]:
            result.append(r[i])
    return list(result)
   

def deoverlap(x):
    r = sorted(list(x));
    result = deque()
    result.append(r[0])
    for i in range(1,len(r)):
        if r[i][0]>result[-1][-1]:
            result.append(r[i])
    return list(result)
def deoverlap(x,y=None):
    if y is None:
        if len(x)==0:
            return x
        r = sorted(list(x));
        result = deque()
        result.append(r[0])
        for i in range(1,len(r)):
            if r[i][0]>result[-1][-1]:
                result.append(r[i])
        return list(result)
    else:
        if len(x)==0:
            return y
        if len(y)==0:
            return x
        r=list(x);
        r.extend(list(y))
        #r.sort()   
        result = [r[0]]
        for i in range(1,len(r)):
            if r[i][0]>result[-1][-1]:
                result.append(r[i])
        return list(result)

def printDict(d):
    for k,v in d.items():
        print(k,':')
        if isinstance(v,Iterable):
            for i in v:
                print(i)
        else:
            print(v)