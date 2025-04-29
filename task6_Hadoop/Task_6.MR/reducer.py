import sys
from itertools import groupby
from operator import itemgetter

def reducer():
    # Чтение данных из stdin и сортировка по ключу (courseid)
    data = [line.strip().split('\t') for line in sys.stdin]
    data.sort(key=itemgetter(0))


    # Группировка по courseid
    user_activity = []
    for userid, group in groupby(data, key=itemgetter(0)):
        scores = [float(value) for _, value in group]
        avg_activity = sum(scores) / len(scores)
        user_activity.append((userid, avg_activity))

    user_activity.sort(key=itemgetter(1))
    for userid, act in user_activity:
        print(f'{userid}\t{act}')

if __name__ == '__main__':
    reducer()