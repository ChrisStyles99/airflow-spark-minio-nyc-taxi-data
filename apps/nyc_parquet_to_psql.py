from pyspark.sql import SparkSession
import os

sc = SparkSession.builder.appName('nyc_taxi_raw')\
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar")\
    .config("fs.s3a.access.key", "minio")\
    .config("fs.s3a.secret.key", "password")\
    .config("fs.s3a.endpoint", "http://minio:9000")\
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .config("fs.s3a.path.style.access", "true")\
    .getOrCreate()

print('hola despues de sc')
df_taxi = sc.read.format("parquet").load('s3a://nyc-taxi-data/yellow_tripdata_2023-01.parquet')

df_zone = sc.read.option("inferSchema", "true").option("header", "true").csv('s3a://nyc-taxi-data/taxi+_zone_lookup.csv')

print(df_zone.show(5), df_zone.schema)

df_taxi.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
        .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
        .option("dbtable", "raw.taxi_data")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .save()

df_zone.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
        .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
        .option("dbtable", "raw.taxi_zone")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .save()