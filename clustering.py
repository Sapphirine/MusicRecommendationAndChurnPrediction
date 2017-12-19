# -*- coding:utf-8 -*-
from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from os.path import expanduser, join, abspath
from pyspark.mllib.classification import SVMWithSGD,SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.recommendation import *

vec_user_dict = {}

def parse_data(array):
    array = array.split(',')[:-1]
    return [float(array[i]) for i in range(len(array)-1)]


def cal_dist(list1, list2):
    dist = 0
    for i in range(len(list1)):
        dist += (list1[i] - list2[i])**2
    return dist**0.5

print "Hello World"

conf = SparkConf().setAppName("Simon1994").setMaster("local")
sc = SparkContext(conf=conf)


data = sc.textFile("./dataset_for_cluster.csv")
parsedData = data.map(parse_data)

x = data.collect()

for line in x:
    line = line.split(',')[:-1]
    msno = line[-1]
    line = [float(line[i]) for i in range(len(line)-1)]
    key = str(line)
    vec_user_dict[key] = str(msno)
    if key not in vec_user_dict:
        vec_user_dict[key] = []
    vec_user_dict[key].append(msno)

input_user_name = data.first()
input_user_vec, input_user_id = input_user_name.split(",")[:-2], input_user_name.split(",")[-2]
input_user_vec = [float(x) for x in input_user_vec]

clusters = KMeans.train(parsedData, 182, maxIterations=300, initializationMode="random")

cluster_centers = clusters.centers

nearest_center_index = clusters.predict(input_user_vec)
print nearest_center_index

dist = 2**100000

nearest_user = "hhh"

for d in parsedData.collect():
    if clusters.predict(d) == nearest_center_index:
        curr_dist = cal_dist(input_user_vec, d)
        if curr_dist < dist and vec_user_dict[str(d)] != vec_user_dict[str(input_user_vec)]:
            dist = curr_dist
            nearest_user = vec_user_dict[str(d)]

print str(nearest_user[0])
print type(nearest_user[0])


# userId = "hys"
#
# # cha fe
#
#
# def error(point):
#     center = clusters.centers[clusters.predict(point)]
#     return sqrt(sum([x**2 for x in (point - center)]))
#
#
# WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)/parsedData.count();
# print("The standardized error is " + str(WSSSE))


'''
# Save and load model
clusters.save(sc, "/Users/yiming/Documents/BigData/HW2/log")
sameModel = KMeansModel.load(sc, "/Users/yiming/Documents/BigData/HW2/log")
'''
print "Goodbye World"

'''
cen = clusters.centers

count = [0, 0, 0]
for p in parsedData.collect():
    count[cen.index(clusters.predict(p))] += 1


print count


'''
