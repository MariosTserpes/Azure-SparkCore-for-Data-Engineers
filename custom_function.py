'''
Including a Child Notebook.
Firstly, as you can see for data_ingestion.py we are adding the ingestion_date.
So, instead of writing the code specifically in each of the notebook
you can have a function in another chlid notebook and you can invoke that in every single notebook.
HINT: In Azure Databricks you could se the path where your custom function is located and using the below magic %run 
you can call and reuse your custom function. For Example:
Step. 1: Set the path in a different notebook, i.e configuration in includes file.
Step. 2: Call your function in your main notebook, i.e %run "../includes/configuration"
Step. 3: Reuse your function
'''
from pyspark.sql.functions import current_timestamp, lit

def add_ingestion_date(input_df):
    '''
    Input: This Function will take a dataframe.
    Output: Will return a dataframe.
    '''
    # we want to add the column to the input dataframe
    output_df = input_df.withColumn("ingestion_date", current_timestamp()) 
    return output_df

'''
Passing Parameters to Notebooks(widgets)
Parameters help us reuse the entire notebook.
For example, if you are processing data from two different sources with the same characteristics
instead of writing two different notebooks you could write one notebook
and send the data source name as parameter.
'''

#using this widget you could see in the top of the screen a box where you can pass the parameter
dbutils.widgets.text("p_data_source", "") 

#get the value of the parameter into a variable
v_data_source = dbutils.widget.get() # so we should add the data scource in our frame before save it as parquet

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "Latitude")\
.withColumnRenamed("lng", "Longtitude")\
.withColumnRenamed("alt", "Altitude")\
.witColumn("data_source", lit(v_data_source)) #lit function because the variable is not of column type