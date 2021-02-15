# This is the implementation of k-means algorithm used for geo-clustering
# Usage: spark-submit --master local[2] kmeans.py [Euclidean|GreatCircle] [k] [input file path]
############################################################################
import sys, os
from pyspark import SparkContext
from pyspark import SparkConf
from math import radians, degrees, sqrt, sin, atan2, cos

##################################  1  #####################################
# given a (latitude, longitude) point and an array of current center points,
# returns the index in the array of the center closest to the given point
def closestPoint(pointX, arrayCenters, distMeasure): 
    min_dist = distMeasure(pointX, arrayCenters[0])
    min_index = 0
    for i in range(len(arrayCenters)):
        cur = distMeasure(pointX, arrayCenters[i])
        if cur < min_dist:
            min_dist = cur
            min_index = i
    return min_index

# given two points, return a point which is the sum of the two points
def addPoints(pointX, pointY):
    x = pointX[0] + pointY[0]
    y = pointX[1] + pointY[1]
    z = pointX[2] + pointY[2]
    return (x, y, z)

# convert the (latitude, longitude) to (x, y, z) coordinates
def toXYZ(point):
    lat = radians(point[0])
    lon = radians(point[1])
    x = cos(lat) * cos(lon)
    y = cos(lat) * sin(lon)
    z = sin(lat)
    return (x, y, z)

# convert the (x, y, z) coordinates to (latitude, longitude) 
def toLatLong(x, y, z):
    newLat = atan2(z, sqrt(x*x + y*y))
    newLong = atan2(y, x)
    return (degrees(newLat), degrees(newLong))

# given two points, returns the Euclidean distance of the two
def EuclideanDistance(pointX, pointY): 
    leng = 110.25*1000
    latX = pointX[0]
    longX = pointX[1]
    latY = pointY[0]
    longY = pointY[1]

    x = latY - latX
    y = (longY-longX)*cos(latX)
    return leng*sqrt(x*x + y*y)

# given two points, returns the great circle distance of the two
def GreatCircleDistance(pointX, pointY): 
    latX = radians(pointX[0])
    longX = radians(pointX[1])
    latY = radians(pointY[0])
    longY = radians(pointY[1])
    deltaLat = radians(pointX[0] - pointY[0])
    deltaLong = radians(pointX[1] - pointY[1])

    a = sin(deltaLat / 2) ** 2 + cos(latX) * cos(latY) * sin(deltaLong / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    R = 6371000
    return R*c

# minimum distance from the selected points
def minimumDist(point, sePoints, distMeasure):
    return (point, min([distMeasure(point, sepoint) for sepoint in sePoints]))

# Initially choose k points that are likely to be in different clusters
def initKCenters(allcoordinates, distMeasure): 
    kPoints = list()
    firstPoint = allcoordinates.takeSample(False, 1, seed=0)
    kPoints.append(firstPoint[0])

    # WHILE there are fewer than k point DO
    while len(kPoints) < k:
        newRDD = allcoordinates.map(lambda point: minimumDist(point, kPoints, distMeasure))
        kPoints.append(newRDD.max(lambda pair: pair[1])[0])
    return kPoints

# convert to csv file format
def toCSVLine(data):
    return ','.join(str(d) for d in data)

# stop printing much logging info
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )



if __name__ == "__main__":
    sconf = SparkConf().setAppName("k-means Geo-clustering Spark").set("spark.ui.port","4141")
    sc = SparkContext(conf=sconf)

    quiet_logs(sc)

    # check the number of parameters in command line
    if len(sys.argv) != 4: 
        print >> sys.stderr, 'Correct Usage: spark-submit kmeans.py [Euclidean|GreatCircle] [k] [input file path]'
        exit(-1)

    # determine which distance measure is used
    if sys.argv[1] == 'Euclidean':
        distMeasure = EuclideanDistance
    elif sys.argv[1] == 'GreatCircle':
        distMeasure = GreatCircleDistance
    else:
        print 'Correct Usage: spark-submit kmeans.py [Euclidean|GreatCircle] [k] [input file path]'

    # read in command line k value
    k = int(sys.argv[2])

    # read in a file of coordinates (format: latitude,longitude)
    # sys.argv[3]="file:/home/training/sharewithHost/fp3/input/Coordinates/part-*"
    # persist the RDD for efficient use in iteration loop
    latlongs = sc.textFile(sys.argv[3]).map(lambda line: line.split(","))\
       .map(lambda latlongs: (float(latlongs[0]),float(latlongs[1]))).persist()

    # Initially choose k points that are likely to be in different clusters
    # Make these points the centroids of their clusters
    kCentroids = sorted(initKCenters(latlongs, distMeasure))

    # Iterate until convergeDist < 0.1
    convergeDist = 1
    x = 1
    while (convergeDist >= 0.1):
        convergeDist = 0

        # cluster the points by labeling them with cluster index, and persist the RDD
        latlongs_clustered = latlongs.map(lambda point: (closestPoint(point, kCentroids, distMeasure), toXYZ(point))).persist()

        # calculate the sum of all points' coordinates
        sum_clustered = latlongs_clustered.reduceByKey(lambda v1,v2: addPoints(v1,v2))

        # print out the number of points in each cluster
        count_clustered = latlongs_clustered.countByKey()
        print count_clustered

        # calculate the new k centroids of k clusters
        newKCentroids = sorted(sum_clustered.map(lambda pair: toLatLong(pair[1][0],pair[1][1],pair[1][2])).collect())

        # calculate the sum of distances between the old k centroids and the new k centroids 
        for p1,p2 in zip(kCentroids, newKCentroids):
            convergeDist += distMeasure(p1, p2)

        # update the k centroids
        kCentroids = newKCentroids

        # print out the iteration number and the convergeDist 
        print "Iteration ",x
        x += 1
        print "convergeDist ",convergeDist

        # display the k center points
        print "kCentroids ",kCentroids
        

    output_dir = "file:/home/training/sharewithHost/fp3/"

    # store the k clusters (all data points plus cluster information)
    csvLines = latlongs_clustered.map(lambda pair: (pair[0], toLatLong(pair[1][0],pair[1][1],pair[1][2]))).sortByKey().map(toCSVLine)
    csvLines.saveAsTextFile(output_dir + "output_csv")

    # save the k centroids into the file "map-vis.csv"
    out = open("map-vis.csv", "w+")
    out.write('Cluster,Latitude,Longitude\n')
    i = 0
    for center in kCentroids: 
        out.write("Center " + str(i) + "," + str(center[0]) + "," + str(center[1]) + "\n")
        i += 1
    out.close()

    sc.stop()
