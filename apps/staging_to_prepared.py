from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

sc = SparkSession.builder.appName('nyc_staging_to_prepared')\
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar")\
    .getOrCreate()

df_staging = sc.read.format("jdbc").option("driver", "org.postgresql.Driver")\
            .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
            .option("dbtable", "staging.taxi_data")\
            .option("user", "airflow")\
            .option("password", "airflow")\
            .load()

df_zone = sc.read.format("jdbc").option("driver", "org.postgresql.Driver")\
            .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
            .option("dbtable", "staging.taxi_zone")\
            .option("user", "airflow")\
            .option("password", "airflow")\
            .load()

df_payment = df_staging.select("payment_type", "payment_type_name").distinct().sort("payment_type", ascending=True)
df_ratecode = df_staging.select("rate_code_id", "rate_code_type").distinct().sort("rate_code_id", ascending=True)
df_vendor = df_staging.select("vendor_id", "vendor_name").distinct().sort("vendor_name", ascending=True)

df_staging = df_staging.drop('vendor_id', 'vendor_name', 'payment_type_name', 'pickup_borough', 'pickup_zone', 'pickup_service_zone',
                         'dropout_borough', 'dropout_zone', 'dropout_service_zone', 'rate_code_type').withColumn("fact_id", expr("uuid()"))

df_staging.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
          .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
          .option("dbtable", "prepared.fact_trip")\
          .option("user", "airflow")\
          .option("password", "airflow")\
          .save()

df_payment.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
          .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
          .option("dbtable", "prepared.dim_payment")\
          .option("user", "airflow")\
          .option("password", "airflow")\
          .save()

df_ratecode.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
          .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
          .option("dbtable", "prepared.dim_ratecode")\
          .option("user", "airflow")\
          .option("password", "airflow")\
          .save()

df_vendor.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
          .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
          .option("dbtable", "prepared.dim_ratecode")\
          .option("user", "airflow")\
          .option("password", "airflow")\
          .save()

df_zone.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
          .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
          .option("dbtable", "prepared.dim_location")\
          .option("user", "airflow")\
          .option("password", "airflow")\
          .save()