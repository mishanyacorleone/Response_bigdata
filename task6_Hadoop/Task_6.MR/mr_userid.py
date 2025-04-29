import os
import re
import json
import socket
import subprocess
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import utils as pu
from pyspark.sql import functions as F
from pyspark.sql import types as pt
import sys


def file_content(path):
    """
    Возвращает содержимое файла.
    Аналогично команде `cat`.
    
    """
    process = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', path], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE
    )
    out, err = process.communicate()
    return out.decode('unicode_escape')


class MapReduce:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def mapper(self, line):
        fields = line.strip().split(',')
        if len(fields) > 0:
            courseid = fields[0]
            userid = fields[1]
            yield courseid, userid
            
    def reducer(self, courseid, values):
        yield courseid, list(set(values))

    def run(self):
        intermediate = {}
        raw_data = file_content(self.input_file)
        lines = raw_data.splitlines()
        print(lines[0])
        lines = lines[1:]  # Пропускаем заголовок
        
        for line in lines:
            for key, value in self.mapper(line):
                if key not in intermediate:
                    intermediate[key] = []
                intermediate[key].append(value)

        results = {}
        for key, values in intermediate.items():
            for result_key, result_value in self.reducer(key, values):
                results[result_key] = result_value

        # Запись результатов в файл
        with open(self.output_file, 'w') as f:
            for courseid, count in results.items():
                f.write(f"{courseid}: {count}\n")

if __name__ == '__main__':
    print(sys.argv)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    mr_job = MapReduce(input_file, output_file)
    mr_job.run()