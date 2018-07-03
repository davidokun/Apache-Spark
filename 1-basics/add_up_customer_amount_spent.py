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
    customer = int(value[0])
    amount = float(value[2])
    return customer, amount


lines = sc.textFile("file:///SparkCourse/files/customer-orders.csv")
customers = lines.map(parse_line)
addedAmounts = customers.reduceByKey(lambda x, y: x + y)
addedAmountsSorted = addedAmounts.map(lambda x: (x[1], x[0])).sortByKey()

results = addedAmountsSorted.collect()

for c, v in results:
    print("Customer {} spent a total of $US {:.2f}".format(v, c))
