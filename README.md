# PYSPARK-NOTES

CHP 1 (CREATE SPARK SESSION)

from pyspark.sql import SparkSession

# Create or get a Spark session
spark = SparkSession.builder \
    .appName("spark introduction") \
    .master("local[*]") \
    .getOrCreate()

# CREATE DATAFRAME AND SCHEMA 
Emp_data = [

 ("E001", "D001", "John", "30", "M", "3000", "2020-01-15"), 
 ("E002", "D002", "Alice", "28", "F", "4000", "2019-04-18"),  
("E003", "D003", "Bob", "35", "M", "5000", "2018-07-12") 

 ] 

emp_schema = "employee_id string,  department_id string,  name string, age string,  gender string,  salary string,  hire_date string" 

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema) 
emp_df.show()

# CHECK NUMBER OF PARTITIONS 
emp_df.rdd.getnumpartitions()

# TRANSFORM DATA 
# Assuming emp_df is already created from emp_data
emp_final = emp_df.where(emp_df.salary > "3000")

# Show the filtered DataFrame
emp_final.show()

# CSV FILE (ACTION SAVE MY WORK)
emp_final.write.csv("data/output/1/emp.csv",header=True)
emp_final.printschema()
schema_info = emp_final.schema() / print(schema_info)




