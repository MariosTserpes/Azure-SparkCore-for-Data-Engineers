'''
Aggregate Functions
'''
import pyspark
from pyspark.sql.functions import count, countDistinct, sum, desc, rank, when, col
from pyspark.sql.window import Window

spark = pyspark.sql.SparkSession.builder.appName('DEng4').getOrCreate()

race_results_df = spark.read.parquet("race_results")

demo_df = race_results_df.filter("race_year = 2020")

demo_df.select(countDistinct("race_name")).show()
demo_df.select(sum("points")).show()

demo_df.where("driver_name = 'Lewis Hamilton'")\
.select(sum("points"), countDistinct("race_name"))\
.withColumnRenamed("sum(points)", "total_points")\
.withColumnRenamed("count(DISTINCT race_name)", "No_of_races")\
.show()

'''
Group Aggregations
'''
demo_df\
.groupBy("driver_name")\
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_race"))\
.show()


'''
Window Functions:
3 things to think: a. How do you want to partition the data(in this case by "race_year")
                   b. We want to order by something(in this case by total points descending)
                   c. Apply a function called rank to produce the output
'''
demo_df2 = race_results_df.filter("race_year in (2019, 2020)")

demo_groupped_df = demo_df2\
.groupBy("race_year", "driver_name")\
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_race"))\
.show()

driverRankSpec = Window.partitionBy("race_year")\
.orderBy(desc("total_points"))

#demo_groupped_df\
#.withColumn("rank", rank().over(driverRankSpec)).show(5)

'''
Driver Standings
'''
driver_standings_df = race_results_df\
.groupBy("race_year", "driver_name", "driver_nationality", "team")\
.agg(sum("points").alias("total_points"), 
     count(when(col("position") == 1, True)).alias("wins"))