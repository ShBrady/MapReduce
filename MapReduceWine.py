#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sepW_all,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_sepW_all(self, _, line):
        # yield sepal width for all species
        data = DATA_RE.findall(line)
        if len(data) >= 5:
            species = data[4]  # Get the species name
            sep_W = float(data[1])  # Get sepal width
            yield (species, sep_W)

    def reducer_get_avg(self, key, values):
        # get average sepal width for each species
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield (f"{key} - sepal width avg", round(total / size, 2))
if __name__ == '__main__':
    MRProb2_3.run()
