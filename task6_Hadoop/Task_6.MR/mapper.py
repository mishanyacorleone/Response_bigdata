import sys

def mapper():
    for line in sys.stdin:
        fields = line.strip().split(',')
        if len(fields) > 1 and fields[1].isdigit():  # Пропуск пустых строк
            userid = fields[1]
            s_all = fields[3]
            print(f"{userid}\t{s_all}")

if __name__ == '__main__':
    mapper()