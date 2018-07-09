import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


def load_movie_names():
    """
    Map movie ID to Movie name
    :return: a dictionary with movie id and movie name
    """
    movie_names = {}
    with open("files/ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


def make_pairs(user_ratings):
    """
    Make pairs for movie and ratings
    :param user_ratings:
    :return:
    """
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return (movie1, movie2), (rating1, rating2)
