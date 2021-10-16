'''
Constructors file - Read Data
'''

import pyspark
from pyspark.sql.functions import current_timestamp, col, concat, lit

spark = pyspark.sql.SparkSession.builder.appName('DEng2').getOrCreate()

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema) \
.json("constructors.json")

'''
Transform and Write Data
'''
constructor_dropped_df = constructor_df.drop("url")

constructor_final_df = constructor_dropped_df\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("Ingestion_date", current_timestamp())

'''
Drivers File
'''

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType

name_schema = StructType(fields = [
                                   StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)
])


drivers_schema = StructType(fields = [
                                      StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
])

drivers_df = spark.read \
.schema(drivers_df) \
.json("drivers.json")

drivers_with_columns_df = drivers_df\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("driverRef", "driver_ref")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

drivers_final_df = drivers_with_columns_df.drop(col("url"))



'''
Results File
'''

results_schema = StructType(fields = [
                                      StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("Laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusId", StringType(), True)
                                      
])

                                    
results_df = spark.read\
.schema(results_schema)\
.json("results.json")

results_with_columns_df = results_df\
.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("positionText", "position_text")\
.withColumnRenamed("positionOrder", "position_order")\
.withColumnRenamed("fastestLap", "fastest_lap")\
.withColumnRenamed("fastestLapTime", "fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
.withColumn("ingestion_date", current_timestamp())

results_final_df = results_with_columns_df.drop(col("statusId"))

'''
Pitstops File ==> HINT : Multiline JSON
'''

pit_stops_schema = StructType(fields = [
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

pit_stops_df = spark.read\
.schema(pit_stops_schema)\
.option("multiline", True)\ #remember that the JSON has multiline construction
.json("pit_stops.json")

pit_stops_final_df = pit_stops_df\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumn("Ingestion_date", current_timestamp())