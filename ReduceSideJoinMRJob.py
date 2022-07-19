#! /usr/bin/python3
# ReduceSideJoinMRJob.py

from unicodedata import name
from mrjob.job import MRJob

THEMES_HEADER = 'id|name|parent_id'
SETS_HEADER = 'set_num|name|year|theme_id|num_parts'
FIELD_SEP = '|'

class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        
        # Skip the header lines in both files
        if line != THEMES_HEADER and line != SETS_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 3: # We have the THEMES dataset
                key = int(fields[0]) # The key is in the attribute id
                name = fields[1]
                parent_id = fields[2]
                value = (name, parent_id)
                yield key, ('T', value)
            elif len(fields) == 5: # We have the Sets dataset
                key = int(fields[3]) # The key is in the attribute theme_id
                year = fields[2]
                num_parts =(4)
                value = (year, num_parts)
                yield key, ('S', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        total = 0
        count = 0
        for value in list(values):
            if value[0] == 'T':
                name = value[1][0]
                parent_id = value[1][1]
            elif value[0] == 'S':
                year = value[1][0]
                num_parts = value[1][1]
        yield key, (name, parent_id, year, num_parts)


if __name__ == '__main__':
    ReduceSideJoinJob.run()