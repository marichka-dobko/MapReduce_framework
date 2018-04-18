from mapreduce import MapReduce
import sys

class WordCount(MapReduce):

    def __init__(self, input_dir, output_dir, n_mappers, n_reducers):
        MapReduce.__init__(self,  input_dir, output_dir, n_mappers, n_reducers)

    def mapper(self, key, value):
        pass