from pyspark import SparkConf,SparkContext
from operator import add
import sys
import re

def part1(context):
    file = context[0]
    #splitting the words
    words = re.sub('[^a-z0-9]+',' ',context[1].lower()).split()  
    file = file.split("_")[-1]

    #appending the file name
    list = [x + '@' + file for x in words]
    return list


conf = SparkConf()
conf.setAppName( "part1" )
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf = conf)
#map phase
tokenize =sc.wholeTextFiles("/cosc6339_s17/books-longlist").flatMap(part1).map(lambda w: (w,1))
#reduce phase
counts=tokenize.reduceByKey(lambda x,y: x+y, numPartitions=1)
counts.saveAsTextFile("/bigd12/output")

