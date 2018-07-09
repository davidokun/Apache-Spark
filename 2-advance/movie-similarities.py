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


def filter_duplicates(user_ratings):
    """
    Remove the duplicates for movies and ratings
    :param user_ratings:
    :return:
    """
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def compute_cosine_similarity(rating_pairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in rating_pairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)
