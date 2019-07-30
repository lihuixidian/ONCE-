from pyspark import SparkContext
import time
import cetc
import cetc2
from multiprocessing import cpu_count,freeze_support
from Prepare import *

if __name__ == '__main__':
    freeze_support()
    import count_episode_time_constraint
    count_episode_time_constraint.test()
    #cetc.test()

    sc = SparkContext("local[%d]"%(cpu_count()-1), "NetworkWordCount")
    sc.setLogLevel('ERROR')
    files = ['without1all']
    segmentations = 20;
    for file in files:
        print(file)
        f = open(file + '.txt')
        s = f.readline()
        f.close()
        s = eval(s)

        f=open('e.txt')
        e = f.readline();
        f.close()
        e = eval(e)

        time_start = time.time()

        len_s = len(s)
        size = len_s//segmentations
        seg1 = [(True,s[0+i*size:(i+1)*size]) if i<segmentations-1 else (True,s[0+i*size:(i+1)*size])  for i in range(segmentations)]
        if len_s != sum([len(i) for i in seg1]):
            print('error occurs when do segementa');
        #seg2 = [s[0:size//2+1]]
        seg2 = [(True,s[size//2+1+i*size:size//2+1+(i+1)*size]) for i in range(segmentations-1)]
        #temp2 = [s[size//2+1+(segmentations-1)*size:len_s]]
    
        if len_s != sum([len(i) for i in seg2]):
            print('error occours when do segementa step 2')
        seg = seg1;
        seg.extend(seg2)
        time_after_seg = time.time()
        print("seg cost:", time_after_seg - time_start)
        segRDD = sc.parallelize(seg)
        time_after_parallelize = time.time()
        print("parallelize cost:", time_after_parallelize - time_after_seg )
        #e={(2, 68, 65),(2, 68, 65, 44, 22, 12, 44, 22, 8),(2, 68, 65, 44, 22, 12, 44),(2, 68, 65, 44, 59),(2, 13, 56)};
        
        #mapedRDD = segRDD.flatMap(lambda x:cetc2.count_episode_time_constraint(100000, 0, x).do(file, e))
        #mapedRDD.persist()
        #print('记录数： ',mapedRDD.count())
        #time_after_map = time.time()
        #print("map cost:", time_after_map - time_after_parallelize)
        #reducedRDD = mapedRDD.reduceByKey(combine).mapValues(lambda x:len(x))
        
        
        mapedRDD = segRDD.flatMap(lambda x:cetc.count_episode_time_constraint_with_overlap(100000, 0, x).do(file, e)).persist()
        #print(mapedRDD.count())
        time_after_map = time.time()
        print("map cost:", time_after_map - time_after_parallelize)
        reducedRDD = mapedRDD.reduceByKey(lambda x,y:x|y).mapValues(lambda x:len(deoverlap(x)))
        #reducedRDD = mapedRDD.reduceByKey(lambda x,y:x|y).mapValues(lambda x:deoverlap(x))
        #reducedRDD = mapedRDD.reduceByKey(lambda x,y:x|y).mapValues(lambda x:len(x))
        #reducedRDD = mapedRDD.reduceByKey(lambda x,y:x|y).mapValues(lambda x:sorted(x))
    
        d=reducedRDD.collectAsMap()
        time_after_reduce = time.time()
        print("reduce cost:", time_after_reduce - time_after_map)
        #printDict(d)
        print("parallel version counting cost:", (time.time() - time_start))
    sc.stop()
