from abc import ABC
from itertools import islice
import pandas

from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob, ABC):

    def mapper(self, _, line):
        palabras = line.split(',')
        empleado = []

        for palabra_empleado in islice(palabras, 0, None, 4):
            for palabra_sector in islice(palabras, 1, None, 3):
                if palabra_sector != 'economic sector' and palabra_empleado != 'idemp':
                    empleado.append([palabra_empleado, palabra_sector])

        # x[0] = empleado  x[1] = sector

        for x in empleado:
            yield x[0], x[1]

    def reducer(self, key, values):
        list_sector = list(values)
        yield key, len(list_sector)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
