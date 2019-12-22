import time
import sys
from create_files import create_output_path
from pyspark.sql import SparkSession


def fib(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b


def load_data(spark, l_path, l_format):
    if l_format in ['csv', 'csv.gz']:
        return spark.read.csv(l_path, header=True)
    elif l_format in ['snappy.parquet']:
        return spark.read.parquet(l_path)


def do_benchmark(spark, s, v, w, l_format):

    load_path = create_output_path(s, v, w)
    load_path = '{}{}'.format(load_path, l_format)

    df = load_data(spark, load_path, l_format)
    print(load_path)
    print(df.count())


def main():
    ps = sys.argv[1]
    pv = sys.argv[2]
    pw = sys.argv[3]
    f = sys.argv[4]
    spark = SparkSession.builder.appName('abc').getOrCreate()
    do_benchmark(spark, ps, pv, pw, f)


if __name__ == '__main__':
    main()
