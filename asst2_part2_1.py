from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import HashingTF, IDF, NGram
from pyspark.sql import SparkSession
from operator import add

import re
import sys


def part2(context):
    file = context[0]
    words = re.sub('[^a-z0-9]+',' ',context[1].lower()).split()
    file = file.split("/")[-1]
    return (file,words)


#configuring spark
conf = SparkConf()
conf.setAppName( "part2_uni" )
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf = conf)

#reading input
lines =sc.wholeTextFiles("/cosc6339_s17/books-longlist/")
#configuring SparkSession
spark=SparkSession(sc)
hasattr(lines, "toDF")

#tokeinizing the words and converting into dataframes
tokenize=lines.map(part2).toDF(["bookname", "words"])

#converting into unigrams
unigram = NGram(n=1, inputCol = "words", outputCol = "unigrams")
unigramdataframe = unigram.transform(tokenize)

#finding the tf value
hashingTF = HashingTF(inputCol = "unigrams", outputCol = "unigram-tf")
tf = hashingTF.transform(unigramdataframe)

#finding the idf value
idf = IDF(inputCol = "unigram-tf", outputCol = "unigram-tf-idf")
idfModel = idf.fit(tf)
tfidfignore = idfModel.transform(tf)

#saving the output
tfidfignore.rdd.saveAsTextFile("/bigd12/output2_1")

