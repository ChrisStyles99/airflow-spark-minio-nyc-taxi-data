from pyspark.sql import SparkSession

sc = SparkSession.builder.appName('nyc_raw_to_staging')\
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar")\
    .getOrCreate()

df_raw = sc.read.format("jdbc").option("driver", "org.postgresql.Driver")\
            .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
            .option("dbtable", "raw.taxi_data")\
            .option("user", "airflow")\
            .option("password", "airflow")\
            .load()

print(df_raw.show(5))