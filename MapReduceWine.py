#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_alcohol_all,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_alcohol_all(self, _, line):
        # yield alcohol content for all classes
        data = DATA_RE.findall(line)
        if len(data) >= 14:
            classes = data[0]  # Get the species name
            alcohol = float(data[1])  # Get alcohol content
            yield (classes, alcohol)

    def reducer_get_avg(self, key, values):
        # get average alcohol content for each class
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield (f"Class {key} - alcohol content avg", round(total / size, 2))

if __name__ == '__main__':
    MRProb2_3.run()
