from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)


def count_co_occurrences(line):
    """
    Create a dictionary with super hero ID and number of appearances
    :param line: a line from the text file
    :return: a dictionary
    """
    elements = line.split()
    return int(elements[0]), len(elements) - 1


def parse_names(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1].encode("utf8")


names = sc.textFile("file:///SparkCourse/files/marvel-names.txt")
namesRdd = names.map(parse_names)

lines = sc.textFile("file:///SparkCourse/files/marvel-graph.txt")

pairings = lines.map(count_co_occurrences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda x: (x[1], x[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(mostPopularName + " is the most popular superhero, with ".encode("utf8") +
      str(mostPopular[0]).encode("utf8") + " co-appearances.".encode("utf8"))
