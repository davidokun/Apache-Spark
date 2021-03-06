"""
Get the mos popular movie base on ratings.
"""
from pyspark import SparkConf, SparkContext


def load_movie_names():
    """
    Creates a dictionary with movie ID and name
    :return: a dictionary with movie Id and name
    """
    movie_names = {}
    with open("/SparkCourse/files/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

# Broadcast the dictionary of movies to all executors
nameDict = sc.broadcast(load_movie_names())

lines = sc.textFile("file:///SparkCourse/files/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

# Create new k/v pair with movie name and number of ratings
sortedMoviesWithNames = sortedMovies.map(lambda count_movie: (nameDict.value[count_movie[1]], count_movie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
