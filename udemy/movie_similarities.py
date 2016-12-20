import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def load_movie_names():
    movie_names = {}
    with open("../ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movie_id = int(fields[0])
            movie_name = fields[1]
            movie_names[movie_id] = movie_name
    return movie_names


def make_pairs(user, ratings):
    movie1, rating_1 = ratings[0]
    movie2, rating_2 = ratings[1]
    return (movie1, movie2), (rating_1, rating_2)

def filter_duplicates(user_id, ratings):
    movie1, rating1 = ratings[0]
    movie2, rating2 = ratings[1]
    return movie1 < movie2


