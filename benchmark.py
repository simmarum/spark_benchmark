import time
import sys
import random
from create_files import create_output_path
from pyspark.sql import SparkSession


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
    t_distinct_1_3(df)
    t_self_join(df)


def t_distinct_1_3(df):
    c = df.schema.names
    c_l = int(len(c) / 3)
    c_l = c_l if c_l > 0 else 1
    c_new = random.choices(c, k=c_l)
    df.select(c_new).distinct().show()


def t_self_join(df):
    c = df.schema.names
    c_l = int(len(c) / 2)
    c_l = c_l if c_l > 0 else 1
    c_new_l = random.choices(c, k=c_l)
    c_new_r = random.choices(c, k=c_l)

    c_new_l = list(set(c_new_l).union(set([c[0]])))
    c_new_r = list(set(c_new_r).union(set([c[0]])))
    df\
        .select(c_new_l)\
        .alias("l")\
        .join(
            df
            .select(c_new_r)
            .alias("r"),
            c[0]
        )\
        .show()


# def t_self_join_2(df):
#     c = df.schema.names
#     c_l = int(len(c) / 2)
#     c_l = c_l if c_l > 2 else 3
#     c_new = c[0:c_l]
#     df.alias("l").join(df.alias("r"), c_new)\
#         .where("c0 % 2 = 0").alias("la").join(df.alias("ra"), c_new[0:1])\
#         .show()


def main():
    ps = sys.argv[1]
    pv = sys.argv[2]
    pw = sys.argv[3]
    f = sys.argv[4]
    spark = SparkSession.builder.appName('abc').getOrCreate()
    print("$$", ps, pv, pw, f)
    if f != '0':
        do_benchmark(spark, ps, pv, pw, f)


if __name__ == '__main__':
    main()
