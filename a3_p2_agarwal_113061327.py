import json
from pyspark import SparkContext
from pprint import pprint
import sys
import numpy as np




def func1(d):
    new_d = {}
    new_d['reviewerID'] = d['reviewerID']
    new_d['reviewTime'] = d['reviewTime']
    new_d['asin'] = d['asin']
    new_d['overall'] = d['overall']
    return ((d['reviewerID'], d['asin']), d['overall'])


def func2(d):
    # print(d)
    (k, v) = d
    l = len(v)
    return (k, v[l - 1])


def function4(x):
    (k, v) = x
    t = []
    for ele in v:
        t.append((ele[0], (k, ele[1])))
    return t


def function5(x):
    if len(x[1]) >= 5:
        return True
    return False


def function6(d):
    lst = []
    for x in d[1]:
        lst.append((x[0], (d[0], x[1])))
    return lst


def function3(x):
    if len(x[1]) >= 25:
        return True
    return False


def funct4(d):
    lst = []
    for x in d[1]:
        lst.append((x[0], (d[0], x[1])))
    return lst


def sim(d):
    brd_list = list(brdcast1.value)
    tup=tuple(brd_list[0])
    # print(tup)
    brdcst_vect_len=0.0
    sum1=0.0
    for e in tup[1]:
        sum1=sum1+(e[2]*e[2])
        brdcst_vect_len=np.sqrt(sum1)
    other_item_len=0.0
    sum2=0.0
    for el in d[1]:
        sum2=sum2+(el[2]*el[2])
        other_item_len=np.sqrt(sum2)
    denr=brdcst_vect_len*other_item_len

    dict1 = {a: c for a, b, c in d[1]}
    dict2 = {a: c for a, b, c in tup[1]}

    commn_user_count_for_itempair = 0
    num = 0.0
    for i in dict1.keys():
        if i in dict2:
            commn_user_count_for_itempair = commn_user_count_for_itempair + 1
            num = num + (dict1[i] * dict2[i])

    cosine_sim=0.0
    if denr > 0.0:
        cosine_sim = num / denr
    # if commn_user_count_for_itempair > 2:
    #     cosine_sim = num / denr

    return (d[0], cosine_sim, commn_user_count_for_itempair), d[1]

def function7(m):
    new_list=[]
    sum_val=0.0
    length=len(m[1])
    for x in m[1]:
        sum_val = sum_val + x[1]
    for x in m[1]:
        v=x[1]-(sum_val/length)
        new_list.append((x[0],x[1],v))
    return m[0],new_list


def neighbour(d):
    if d[0][1]>0 and d[0][1] !=1 and d[0][2]>=2:
        return True
    return False

def getOnlyUsers(x):
    brd_list = list(brdcast1.value)
    tup = tuple(brd_list[0])
    users_rated_item = []
    users_not_rated_item_and_gt2_rating_avail = []
    for e in tup[1]:
        users_rated_item.append(e[0])
    for ele in x[1]:
        if ele[0] not in users_rated_item:
            users_not_rated_item_and_gt2_rating_avail.append((ele[0],1))
    return users_not_rated_item_and_gt2_rating_avail

def findUserxforwhomRatingsMiss(x):
    brd_list = list(brdcast1.value)
    tup = tuple(brd_list[0])
    users_rated_item=[]
    users_not_rated_item_and_gt2_rating_avail=[]
    for e in tup[1]:
        users_rated_item.append(e[0])
    for ele in x:
        # print(ele)
        cnt = 0
        if ele[0] not in users_rated_item:

            if ele[0] in users_not_rated_item_and_gt2_rating_avail:
                cnt=cnt+1

            else:
                cnt=1
            users_not_rated_item_and_gt2_rating_avail.append((ele[0], cnt))
    return users_not_rated_item_and_gt2_rating_avail


def requiredData(t):
    li=[]
    for e in t[1]:
        weightedSim=e[1]*t[0][1]
        li.append((e[0],(weightedSim,t[0][1])))
    return li

def filterDataToCalforRequiredUsers(d):
    brd_list = list(brdcast2.value)
    for e in brd_list:
        if d[0] in e[0]:
            return True
    return False

def calculateSimilarity(v):
    rating_userX_for_item = 0.0
    num_sum=0.0
    den_sum=0.0
    for e in v[1]:
        num_sum = num_sum+e[0]
        den_sum = den_sum+e[1]
    rating_userX_for_item = num_sum/den_sum
    return (v[0],rating_userX_for_item)

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Incorrect number of args")
    #     sys.exit(-1)
    input_file = sys.argv[1]
    item_list = sys.argv[2]
    # item_list = "['B00EZPXYP4', 'B00CTTEKJW']"
    # print("this was needed",eval(item_list))
    # for item in eval(item_list):
    #     print(item)
    # input_file = r"C:\Users\Yojana\Desktop\Sem1\BigData\Assignment3\Software_5.json.gz"
    sc = SparkContext('local', 'First App')

list_asin=sc.broadcast(eval(item_list))
rdd = sc.textFile(input_file)
rdd1 = rdd.map(lambda line: json.loads(line))
rdd2 = rdd1.map(lambda d: func1(d))
# print(rdd2.take(5))
rdd3 = rdd2.groupByKey().mapValues(list)
# print(rdd3.take(50))
# rdd3.saveAsTextFile("rs1.txt")
rdd4 = rdd3.map(lambda d: func2(d))
# rdd4.saveAsTextFile("rs2.txt")
rdd5 = rdd4.map(lambda d: (d[0][1], (d[0][0], d[1])))
# print(rdd5.take(5))
rdd6 = rdd5.groupByKey().mapValues(list)
# rdd6.saveAsTextFile("rs3.txt")
rdd7 = rdd6.filter(lambda x: function3(x))
# print(rdd7.take(20))
rdd8 = rdd7.flatMap(lambda d: funct4(d))
# rdd8.saveAsTextFile("rs4.txt")
rdd9 = rdd8.groupByKey().mapValues(list)
# rdd9.saveAsTextFile("rs5.txt")
rdd10 = rdd9.filter(lambda x: function5(x))
# rdd10.saveAsTextFile("rs6.txt")
# bc2=rdd10.map(lambda d: d[0])
# brdcast2=sc.broadcast(bc2.collect())
# print(list(brdcast2.value))
rdd11 = rdd10.flatMap(lambda d: function6(d))
# rdd11.saveAsTextFile("rs8.txt")
# print(len(rdd11.countByKey()))
# rdd12=rdd11.groupBy(lambda x: x[1][0])
# rdd12.saveAsTextFile("rs9.txt")
rdd12 = rdd11.groupByKey().mapValues(list)
# val = sc.broadcast(lambda d: )
# rdd12.saveAsTextFile("rs9.txt")
rdd13=rdd12.map(lambda m : function7(m))
# rdd13.saveAsTextFile("rs10.txt")


for item in list_asin.value:
    # print(item)
    rdd14 = rdd13.filter(lambda d: d[0] == item)
    var1=rdd14.take(1)
    brdcast1 = sc.broadcast(var1)
    # print(list(brdcast1.value))
    rdd15=rdd13.map(lambda d: sim(d))
    # rdd15.saveAsTextFile("rs11.txt")
    rdd16=rdd15.filter(lambda d: neighbour(d))
    # rdd16.saveAsTextFile("rs12.txt")
    rdd17=rdd16.flatMap(lambda x: getOnlyUsers(x))
    # rdd17.saveAsTextFile("rs13.txt")
    rdd18=rdd17.reduceByKey(lambda v1,v2:v1+v2)
    # rdd18.saveAsTextFile("rs14.txt")
    rdd19=rdd18.filter(lambda d : d[1]>=2)
    # rdd19.saveAsTextFile("rs15.txt")
    brdcast2=sc.broadcast(rdd19.collect())
    # print(list(brdcast2.value))
    rdd20=rdd16.flatMap(lambda t:requiredData(t))
    # rdd20.saveAsTextFile("rs16.txt")
    rdd21=rdd20.groupByKey().mapValues(list)
    # rdd21.saveAsTextFile("rs17.txt")
    rdd22=rdd21.filter(lambda d: filterDataToCalforRequiredUsers(d))
    # rdd22.saveAsTextFile("rs18.txt")
    rdd23 = rdd22.map(lambda v: calculateSimilarity(v))
    print("Recommend ratings for item :" , item)
    print(list(rdd23.collect()))
    # rdd23.saveAsTextFile("rs20.txt")


