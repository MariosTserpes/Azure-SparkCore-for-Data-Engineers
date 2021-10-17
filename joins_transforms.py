'''
Filter & Join Transformations
'''

spark = pyspark.sql.SparkSession.builder.appName('DEng3').getOrCreate()
races_df = spark.read.parquet("races")\
.withColumnRenamed("name", "race_name")

circuits_df = spark.read.parquet("circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name", "circuit_name")


races_filtered_df = races_df.filter(races_df["race_year"] == 2019) #pythonic way
#races_filtered_df = races_df.filter("race_year = 2019") #sequel way


race_circuits_df = circuits_df\
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)


'''
Outer joins: Left, Right, Full outer
'''
#Right Outer Join
race_circuits_df = circuits_df\
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)


'''
Semi, Anti, Cross Joins
'''

#A. SEMI JOIN is very similar to inner join. So you will get the records which satisfy the condition on both tables.
#Hint: THE DIFFERENCE is that we are only given the columns from the left side of the join which is the left data frame

race_circuits_df = circuits_df\
.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

#B. ANTI JOIN is the opposite of Semi join. Will give you everything from the left table WHICH IS NOT FOUND on the right frame

#C. CROSS JOIN gives you a Cartesian Product.It is going to take every record from the left to the record joint on the right and will give you te product of the two.
#HINT: The difference is in the syntax: .croosJoin() instead of .join()


'''
Join Race and Results parquet
'''

drivers_df = spark.read.parquet("drivers")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.parquet("constructors")\
.withColumnRenamed("name", "team")

circuits_df2 = spark.read.parquet("circuits")\
.withColumnRenamed("location", "circuit_location")

races_df2 = spark.read.parquet("races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")


results_df = spark.read.parquet("results")\
.withColumnRenamed("time", "race_time")

races_circuits_df2 = races_df2\
.join(circuits_df2, circuits_df2.circuit_id == races_df2.circuit_id, "inner")\
.select(races_df2.race_id, races_df2.race_year, races_df2.race_name,  circuits_df2.circuit_location)

race_results_df = results_df\
.join(races_circuits_df2, results_df.race_id == races_circuits_df2.race_id)\
.join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

final_df = race_results_df.select(
    "race_year", "race_name",  "circuit_location", "driver_name",
    "driver_number", "driver_nationality", "team", "grid", "fastest_lap",
    "race_time", "points" 
)\
.withColumn("created_date", pyspark.sql.functions.current_timestamp())

query_1 = final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'")\
.orderBy(final_df.points.desc())