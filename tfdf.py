import sys
import string
import re
import MapReduce 

mr = MapReduce.MapReduce()
count = 1

def mapper(each_tweet):
    global count
    counter = {}
    word_list = [word for line in each_tweet for word in line.split()]
    
    for word in word_list:    
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1
            
    for word in counter:
        mr.emit_intermediate(word, (count, counter[word]))        
            
    count += 1

def reducer(key, list_of_values):
    total = 0
    li = []
    for id, value in list_of_values:
        total += 1
        li.append((id, value))
    mr.emit((key, total, li))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)