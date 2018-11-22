import sys
import string
import re
import MapReduce

mr = MapReduce.MapReduce()
scores = {}
count = 1

def mapper(each_tweet):
    global count
    word_list = [word for line in each_tweet for word in line.split()]
    
    for word in word_list:    
        if word in scores:
            mr.emit_intermediate(count,scores[word])
        else:
            mr.emit_intermediate(count,0)
    
    count += 1

def reducer(key, list_of_values):
    total = 0
    for value in list_of_values:
        total += value
    mr.emit((key,float(total)))

if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)
    tweet_data = open(sys.argv[2]).readlines()
    mr.execute(tweet_data, mapper, reducer)