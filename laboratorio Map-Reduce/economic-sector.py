from itertools import islice
from statistics import mean
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        palabras = line.split(',')
        sector = []

        for palabra_sector in islice(palabras, 1, None, 3):
            for palabra_salary in islice(palabras, 2, None, 4):
                if palabra_salary != 'salary' and palabra_sector != 'economic sector':
                    sector.append([palabra_sector, int(palabra_salary)])

        # x[0] = sector  x[1] = salary
        for x in sector:
            yield x[0], x[1]

    def reducer(self, key, values):
        yield key, mean(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
