from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

sc = SparkSession.builder.appName('nyc_raw_to_staging')\
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar")\
    .getOrCreate()

df_raw = sc.read.format("jdbc").option("driver", "org.postgresql.Driver")\
            .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
            .option("dbtable", "raw.taxi_data")\
            .option("user", "airflow")\
            .option("password", "airflow")\
            .load()

df_zone = sc.read.format("jdbc").option("driver", "org.postgresql.Driver")\
            .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
            .option("dbtable", "raw.taxi_zone")\
            .option("user", "airflow")\
            .option("password", "airflow")\
            .load()

df_raw = df_raw.filter(df_raw.passenger_count.isNotNull())

df_raw = df_raw.withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
df_raw.createOrReplaceTempView("df_raw")
df_zone.createOrReplaceTempView("df_zone")

sc.sql("SELECT * FROM df_raw").show()

df_staging = sc.sql("""SELECT 
            df_raw.VendorID as vendor_id,
            df_raw.tpep_pickup_datetime as pickup_datetime,
            df_raw.tpep_dropoff_datetime as dropoff_datetime,
            df_raw.RatecodeID as rate_code_id,
            df_raw.PULocationID as pickup_location_id,
            df_raw.DOLocationID as dropout_location_id,
            df_raw.passenger_count, df_raw.trip_distance, df_raw.payment_type, df_raw.fare_amount,
            df_raw.extra, df_raw.mta_tax, df_raw.improvement_surcharge, df_raw.tip_amount,
            df_raw.tolls_amount, df_raw.total_amount, df_raw.congestion_surcharge, df_raw.airport_fee,
            CASE WHEN df_raw.VendorID = 1 THEN 'Creative Mobile Technologies, LLC' ELSE 'VeriFone Inc.' END AS vendor_name,
            CASE df_raw.RatecodeID WHEN 1 THEN 'Standard rate' WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark' WHEN 4 THEN 'Nassau or Westchester'
            WHEN 5 THEN 'Negotiated fare' WHEN 6 THEN 'Group ride' ELSE 'Other' END AS rate_code_type,
            CASE df_raw.payment_type WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge' WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided trip' ELSE 'Other' END AS payment_type_name,
            zone_pu.Borough as pickup_borough,
            zone_pu.Zone as pickup_zone,
            zone_pu.service_zone as pickup_service_zone,
            zone_do.Borough as dropout_borough,
            zone_do.Zone as dropout_zone,
            zone_do.service_zone as dropout_service_zone
        FROM df_raw INNER JOIN df_zone zone_pu ON df_raw.PULocationID = zone_pu.LocationID
                    INNER JOIN df_zone zone_do ON df_raw.DOLocationID = zone_do.LocationID""")

df_zone_staging = sc.sql("SELECT LocationID as location_id, Borough as borough_name, Zone as zone FROM df_zone")

df_staging.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
        .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
        .option("dbtable", "staging.taxi_data")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .save()

df_zone_staging.write.format("jdbc").mode("overwrite").option("driver", "org.postgresql.Driver")\
        .option("url", "jdbc:postgresql://postgres/nyc_taxi_data")\
        .option("dbtable", "staging.taxi_zone")\
        .option("user", "airflow")\
        .option("password", "airflow")\
        .save()