'''
Data Ingestion : Multiple Files csv
'''

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
from pyspark.sql.functions import current_timestamp, col, concat, lit

lap_times_schema = StructType(fields = [
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv("lap_times_split*.csv") #Here we set star because we have 5 csv files, i.e lap_times_split1.csv, lap_times_split2.csv etc


lap_times_final_df = lap_times_df\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumn("Ingestion_date", current_timestamp())

'''
Qualifying JSON files
'''

qualifying_schema = StructType(fields = [
                                      StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
])

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline", True)\ #remember again that we work with JSON files
.json("qualifying_split*.json")


qualifying_final_df = qualifying_df\
.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("Ingestion_date", current_timestamp())