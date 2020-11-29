from abc import ABC
from itertools import islice
from statistics import mean
from mrjob.job import MRJob


class SalarioEmpleado(MRJob, ABC):

    def mapper(self, _, line):
        palabras = line.split(',')
        empleado = []

        for palabra_empleado in islice(palabras, 0, None, 4):
            for palabra_salary in islice(palabras, 2, None, 4):
                if palabra_salary != 'salary' and palabra_empleado != 'idemp':
                    empleado.append([palabra_empleado, int(palabra_salary)])

        # x[0] = empleado  x[1] = salary
        for x in empleado:
            yield x[0], x[1]

    def reducer(self, key, values):
        yield key, mean(values)


if __name__ == '__main__':
    SalarioEmpleado.run()
