from pyspark import *
from pyspark.mllib.clustering import KMeans


conf = SparkConf().setMaster("local[*]").setAppName("Test App")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


rdd = sc.textFile("data/5000_points.txt")
rdd = rdd.map(lambda x:x.split())
rdd = rdd.map(lambda x:[int(x[0]),int(x[1])])

rdd.persist(StorageLevel.MEMORY_ONLY)


for clusters in range(1,30):
    model = KMeans.train(rdd, clusters)
    print clusters, model.computeCost(rdd3)


for trials in range(10): # Try ten times to find best result
    for clusters in range(12, 16): # Only look in interesting range
        model = KMeans.train(rdd, clusters)
        cost = model.computeCost(rdd)
        centers = model.clusterCenters # Let's grab cluster centers
        if cost<1e+13:                 # If result is good, print it out
            print clusters, cost
            for coords in centers:
                print int(coords[0]), int(coords[1])
            break