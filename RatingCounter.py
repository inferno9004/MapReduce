
# coding: utf-8

# In[3]:

from mrjob.job import MRJob


# In[5]:

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating,1
        
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)


# In[6]:

if __name__ == '__main__':
    MRRatingCounter.run()

