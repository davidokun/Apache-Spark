from pyspark import SparkConf, SparkContext
import collections

# Create configuration
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# Get Spark Context from configuration
sc = SparkContext(conf=conf)

# Creates and RDD object from the data file
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Apply a map function and store the result in another RDD object
ratings = lines.map(lambda x: x.split()[2])

# Then call a terminal operation. In these case result is a tuple
result = ratings.countByValue()

# Sort values and print result
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
