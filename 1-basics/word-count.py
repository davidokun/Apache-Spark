"""
Count the times a word appear in a text
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

# Read each line in the text
lines = sc.textFile("file:///SparkCourse/files/book.txt")

# Split each line by white space and create an unique entry in the RDD
words = lines.flatMap(lambda x: x.split())

# Count each entry in the RDD
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    # Sanitize the encoding to print in console
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
