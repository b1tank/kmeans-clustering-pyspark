import sys
from pyspark import SparkContext
from Getlatlong import *

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

if __name__ == "__main__":
    sc = SparkContext()

    quiet_logs(sc)

    rdd1 = sc.pickleFile('file:/home/training/Prj_output/All')


    # Schema: (DonorID, GiftAmt, GiftDate, Addr, Lat, Long)
    # In previous step, if Google Geocode fails, the Lat/Long would be '0's

    # Haoran For Zhichao: Upto this line, you can use the gift data to process

    rdd2 = rdd1.filter(lambda fields: (fields[4] != '0' or fields[5] != '0'))


    # In order to plot the coordinations on a map
    # Distinct the coordination: 
    # because a donor may give multiple gift with the same addresses
    rdd3 = rdd2.map(lambda fields: (fields[4], fields[5]))\
               .distinct()
    

    # dump into CSV
    rdd2csv = rdd3.map(toCSVLine)

    rdd2csv.saveAsTextFile('file:/home/training/Prj_output/Coordination')
    

    sc.stop()
