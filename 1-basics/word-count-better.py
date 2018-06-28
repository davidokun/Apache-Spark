"""
Count the times a word appear in a text.
Using regular expressions
"""
import re
from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("file:///SparkCourse/files/book.txt")
words = lines.flatMap(normalize_words)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
