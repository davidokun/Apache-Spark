"""
Calculates the total amount of money a customer spent
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpentAmountByCustomer")
sc = SparkContext(conf=conf)


def parse_line(line):
    """
    Parse each file's line to separate the customer from the amount
    :param line: a line from the data file
    :return: a tuple with customer and amount
    """
    value = line.split(",")
    customer = value[0]
    amount = int(value[1])
    return customer, amount


lines = sc.textFile("file:///SparkCourse/files/customers_data.csv")
customers = lines.map(parse_line)
addedAmounts = customers.reduceByKey(lambda x, y: x + y)

results = addedAmounts.collect()

for c, v in results:
    print("{} spent a total of $US {:,}".format(c, v))
