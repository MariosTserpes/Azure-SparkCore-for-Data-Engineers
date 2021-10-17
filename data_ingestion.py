'''
Specify Schema
'''

import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

spark = pyspark.sql.SparkSession.builder.appName('DEng').getOrCreate()

circuits_schema = StructType(fields = [
                                       StructField('circuitId', IntegerType(), False),
                                       StructField('circuitRef', StringType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('location', StringType(), True),
                                       StructField('country', StringType(), True),
                                       StructField('lat', DoubleType(), True),
                                       StructField('lng', DoubleType(), True),
                                       StructField('alt', IntegerType(), True),
                                       StructField('url', StringType(), True)
                                       
])

circuits_df = spark.read.option("header", True)\
.schema(circuits_schema)\
.csv("circuits.csv")

display(circuits_df.describe().show())
display(circuits_df.printSchema())
display(circuits_df.show())


'''
Select Columns
'''

circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 
                                          'location', 'country', 'lat',
                                          'lng', 'alt',)

display(circuits_selected_df.show())

'''
Renaming the columns
'''

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "Latitude")\
.withColumnRenamed("lng", "Longtitude")\
.withColumnRenamed("alt", "Altitude")

'''
Add Ingestion Date to the dataframe
'''

circuits_final_df = circuits_renamed_df.withColumn("Ingestion_date", pyspark.sql.functions.current_timestamp())
display(circuits_final_df.show())

'''
Now, let's ingesting races.csv file from our container 
to process container
'''

races_schema = StructType(fields = [
                                    StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True),

])

races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv("races.csv")

races_df_with_timestamp_df = races_df.withColumn("Ingestion_date",  pyspark.sql.functions.current_timestamp())\
.withColumn("race_timetamp",  pyspark.sql.functions.to_timestamp(pyspark.sql.functions.concat(pyspark.sql.functions.col("date"), 
                                                                                              pyspark.sql.functions.lit(" "), 
                                                                                              pyspark.sql.functions.col("time")), 
                                                              "yyyy-MM-dd HH:mm:ss"))

races_selected_df = races_df_with_timestamp_df.select("raceId", "year", "round",
                                                      "circuitid", "name", 
                                                      "Ingestion_date", "race_timetamp")

races_selected_df = races_selected_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("year", "race_year")\
.withColumnRenamed("circuitid", "circuit_id")


# we can partition the data being returned to the storage.
races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("races")

spark.read.parquet("races")

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


constructor_final_df.write.mode("overwrite").parquet("constructors")
drivers_final_df.write.mode("overwrite").parquet("drivers")
results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("results")
pit_stops_final_df.write.mode("overwrite").parquet("pit_stops")
lap_times_final_df.write.mode("overwrite").parquet("lap_times")
qualifying_final_df.write.mode("overwrite").parquet("qualifying")