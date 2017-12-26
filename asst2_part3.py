from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec
from operator import add

import sys
import re

if __name__ == "__main__":

    sc = SparkContext(appName= "part3")
    input_lines = sc.textFile("/cosc6339_s17/books-longlist", 1)

    word2vec = Word2Vec()
    model = word2vec.fit(input_lines.map(lambda row: row.split(" ")))
    
    print("End Timer")
   

    wordlist = []
    wordlist.append("amazing")
    wordlist.append("answer")
    wordlist.append("himself")
    wordlist.append("through")
    wordlist.append("without")
    wordlist.append("thought")
    wordlist.append("nothing")
    wordlist.append("another")
    wordlist.append("something")
    wordlist.append("because")
    
    for each in wordlist:
	synonyms=model.findSynonyms(each,2)
	print("Synonyms for : {}".format(each))
	for s,d in synonyms:
		print("{}".format(s))

