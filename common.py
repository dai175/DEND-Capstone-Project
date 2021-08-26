import os

def read_parquet(spark, bucket, folder):
    df = spark.read.parquet(os.path.join(bucket, folder))
    print('count: ', df.count())
    return df

def check_null_key(df, key):
    return df.filter(df[key].isNull()).count() == 0

def match_count(df1, df2):
    return df1.count() == df2.count()
