from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    """
    Parse each line of the file to get age and number of friends
    :param line: a line form the file
    :return: a Tuple with age and number of friends
    """
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])

    return age, num_friends


# Reade the file and assigned to a RDD lines.
lines = sc.textFile("file:///SparkCourse/files/friends-by-age.csv")

# Apply Map function to each line to get age and number of friends
rdd = lines.map(parse_line)

# For each key/value, sum the the values x and y
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Get average for each value
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect the Average by Age
results = averagesByAge.collect()

# Print the results
for result in results:
    print(result)
