import sys
import collections
import re
from math import sqrt
from datetime import datetime
from pyspark import SparkConf, SparkContext

def load_movie_names():
    movie_names = {}
    count = 0
    with open('../ml-100k/u.item') as f:
        for line in f:         
            fields = line.split("|")
            movie_id = int(fields[0])
            movie_name = str(fields[1])
            movie_names[movie_id] = movie_name
    return movie_names

def make_pairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return (movie1, movie2), (rating1, rating2)

def filter_duplicates((user_id, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def compute_cosine_similarity(rating_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingx, ratingy in rating_pairs:
        sum_xx += ratingx*ratingx
        sum_yy += ratingy*ratingy
        sum_xy += ratingx*ratingy
        num_pairs += 1
    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator :
        score = float(numerator)/denominator
    return score, num_pairs


def parse_line(line):   
    fields = line.split()
    user_id = int(fields[0])
    movie_id = int(fields[1])
    rating = float(fields[2])
    
    return user_id, (movie_id, rating)

startTime = datetime.now()

conf = SparkConf().setMaster('local[*]').setAppName("CollaborativFiltering")
sc = SparkContext(conf=conf)

print "\nLoading MovieNames"

name_dict = load_movie_names()

data = sc.textFile('../ml-100k/u.data')

lines = data
#header = data.first()
#data = data.sample(False, 0.00000001)
#lines = data.filter(lambda x: x != header)

ratings = lines.map(parse_line)

joined_ratings = ratings.join(ratings)

# RDD is user_id --> (movieid, rating), (movie_id, rating)

unique_joined_ratings = joined_ratings.filter(filter_duplicates)

movie_pairs = unique_joined_ratings.map(make_pairs)

#Collecting all ratings for movie pairs
movie_pair_ratings = movie_pairs.groupByKey()

#Compute similarities

movie_similarities = movie_pair_ratings.mapValues(compute_cosine_similarity).cache()

if len(sys.argv) > 1:

    score_threshold = 0.97
    co_occurence_threshold = 20
       
    movie_id = int(sys.argv[1])
    filtered_results = movie_similarities.filter(lambda( pair, sim): (pair[0] == movie_id or pair[1] == movie_id) \
                                                        and sim[0] > score_threshold and sim[1] > co_occurence_threshold)

    results = filtered_results.sortBy(lambda x: x[1], ascending=False).take(10)
  
    print "Top 10 similar movies for ", name_dict[movie_id], results
    for result in results:
        (pair, sim) = result
        similar_movie_id = pair[0]
        if  similar_movie_id == movie_id:
             similar_movie_id = pair[1]
        
        print "", name_dict[similar_movie_id], "score", sim[0], 'strength', sim[1]

print "Script took", datetime.now() - startTime, "seconds to run" 
