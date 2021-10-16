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