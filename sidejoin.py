#! /usr/bin/python3
# sidejoin.py


#from unicodedata import name
from mrjob.job import MRJob
from mrjob.step import MRStep

THEMES_HEADER = 'id|name|parent_id'
SETS_HEADER = 'set_num|name|year|theme_id|num_parts'
FIELD_SEP = '|'

class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        
        # Skip the header lines in both files
        if line != THEMES_HEADER and line != SETS_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 3: # We have the THEMES dataset
                key = fields[0] # The key is in the attribute id
                #name = fields[1]
                #parent_id = fields[2]
                #value = (name, parent_id)
                yield (key, ('T', 1))
            elif len(fields) == 5: # We have the Sets dataset
                key = fields[3] # The key is in the attribute theme_id
                year = fields[2]
                num_parts =(4)
                value = (year, num_parts)
                yield (key, ('S', value))
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        id = key # join key
        themes_tuples = []
        sets_tuples = []
        for value in values:
            if value[0] == 'T':
                themes_tuples.append(value[1])
                #parent_id = value[1][1]
            elif value[0] == 'S':
                sets_tuples.appen(value[1])
            else:
                pass # TOO handle error
        if len(themes_tuples) >0 and len(sets_tuples) > 0:
            for value in sets_tuples:
                yield (id, value) 


if __name__ == '__main__':
    ReduceSideJoinJob.run()