'''
Objective [A]. a.Create temporary View on a dataframe 
               b.and then wll access the view from SQL cell
               c.as well as from Python cell
'''

import pyspark

spark = pyspark.sql.SparkSession.builder.appName('DEng5').getOrCreate()

race_results_df = spark.read.parquet("race_results")

#Create a view o Top of this dataframe
race_results_df\
.createOrReplaceTempView("v_race_results") #very important in order to avoid errors

#Access the View
#%sql #you should run the magic %
#SELECT count(*)
#FROM v_race_results
#WHERE race_year = 2020

p_race_year = 2019 #more useful in order to change dynamically the year
race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")


'''
Objective B. a: Create Global Temp Views
             b: Access the view from SQL cell
             c: Access the view from Python cell
             d: Access the view from another notebook
'''

#Create a global view o Top of this dataframe
race_results_df\
.createOrReplaceGlobalTempView("gv_race_results") #very important in order to avoid errors

#Try access it from sql firstly
#%sql
#SELECT *
#  FROM gv_race_results; #hint: This will not work because we created a global View
                                #our spark will registered that view against a database called 
                                #global registered view

#you could see that running the below command
#%sql
#SHOW TABLES IN global_temp;

#So the only differencies is that in order to access the global temp View you need to define this
#%sql
#SELECT *
#  FROM global_temp.gv_race_results;


spark.sql("SELECT * \
FROM global_temp.gv_race_results").show()