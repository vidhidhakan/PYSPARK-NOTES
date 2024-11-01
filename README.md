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

CHP - 2(TRANSFORMATION PART 1)

# If i want to use integertype() /stringtype() - i can do by calling structure field/type 
from pyspark.sql.types import StructField,StructType,StringType,IntegerType 

emp_df= StructType([ 
    StructField("name",StringType(),True), 
    StructField("age", IntegerType(),True) 
]) 

 
# Change emp_id to employee_id 
emp_df = emp_final.withColumnRenamed("empid", "employee_id") 

# Change age from string to integer 
from pyspark.sql.functions import col 
from pyspark.sql.types import IntegerType 

emp_df = emp_final.withColumn("age", col("age").cast(IntegerType())) 

# Single changes  
from pyspark.sql.functions import col 
emp_df = emp_final.withColumn("salary",col("salary")*1.1) 

# Multiple changes  
from pyspark.sql.functions import col 
emp_df = emp_final.select( 
    col("name"), 
    (col("salary")+500), 
    (col('age')*2) 
) 

CHAPTER 3 (PART 2 TRANSFORMATION)
# Adding a new column 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import lit 
emp_final = emp_final.withColumn("new_column", lit(100)) 

# Droping columns 
emp_df = emp_final.drop("new_column") 
Emp_df.show() 

# Rename columns 
emp_final = emp_final.withColumnRenamed("newcolumn", "mycolumn") 
emp_final.show() 

# Using where and limit 
df1 = emp_final.select("employee_id","name","age").where("salary > 4000") 
df1.show() 

df1 = emp_final.select("employee_id","name","age").where("salary>4000").limit(1) 
df1.show() 

# TO CHANGE SALARY FROM INT TO DOUBLE TYPE 

from pyspark.sql.functions import col 
from pyspark.sql.types import DoubleType 

emp_df = emp_final.select("name", "age", "salary") \ 
    .withColumn("age", col("age").cast(DoubleType())) \ 
    .withColumn("salary", col("salary").cast(DoubleType())) 
emp_df.show() 

CHAPTER 4 (PART 2 TRANSFORMATION)

# CASE
from pyspark.sql.functions import col, when

emp_df = emp_final.select("employee_id",
                           "name",
                           "salary",
                           "gender",
                           col(("gender") when "gender" == "M","m"),
                           col(("gender") when "gender" == "F", "f")
                           otherwise("None").alias("newgender")
                           
# REGEXREPLACE
from pyspark.sql.functions import regex_replace

emp_df = emp_final.withcolumn("name",regex_replace("name","N","J"))

# CONVERT DATE STRING INTO DATE FORMAT

from pyspark.sql.types import dataType
from pyspark.sql.functions import col

emp_df = emp_final.withcolumn("hiredate",col("hiredate").cast(dataType()))


# ADD CURRENT DATE/CURRENT TIMESTAMP

from pyspark.sql.functions import current_timestamp, current_date

emp_df = emp_final.withcolumn("Currentdate",current_date()) \
           .withcolumn("currenttimestamp", Current_timestamp())


# REMOVE NULL VALUES

emp_df = emp_final.dropna()
emp_df.show()

# FILL NULL VALUES

emp_df = emp_final.fillna(0)
emp_df.show()

# DROP OLD COLUMNS WITH NEW COLUMNS

emp_df = emp_final.withColumn("newname",emp_final["name"])
emp_df = emp_final.withColumn("newgender",emp_final["newgender"])

emp_df = emp_final.drop("name","gender").withColumnRenamed("name","newname").withColumnRenamed("gender","newgender")

CHAPTER 5 (TRANSFORMATION 2)

# UNION
emp_df = emp_df_1.union(emp_df_2)
emp_df.show()

# SORTING
  emp_df = emp.orderBy(col('salary')).desc()
  emp_df.show()

# AGG
   emp_df = emp.orderBy(col('salary')).agg(count("salary")).alias("totalsalary").desc()
   emp_df = emp.orderBy(col('salary')).agg(max("salary")).alias("maxsalary")
   emp_df = emp.orderBy(col('salary')).agg(min('salary')).alias("minsalary")

   emp_df = emp.groupBy(col("name")).agg(sum('salary')).alias("totalsalary")

# DISTINCTS
     emp_distinct = emp_df.select("name").distinct()
     emp_distinct.show()

# WINDOWS
 # lead,lag,rank,denserank,rownumber / agg(max/min/avg/sum/count)

 from pyspark.sql.functions import rownumber
 from pyspark.sql.window import Window

 window_spec = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())
 max_func = max(col("salary")).over(window_spec)
 emp_1 = emp.withColumn("maxsalary",max_func)
 emp_1.show()
 
--------------------------------------
# rownum/denserank/rank/lead/lag

from pyspark.sql.functions import desc,col, row_number
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("department_id").orderBy(col('salary').desc())
emp_1 = emp.withcolumn("rn",row_number().over(window_func)).where("rn = 2")

emp_1 = emp.withColumn("lead",lead('salary',1).over(window_func))
emp_1 = emp.withColumn("lag",lag('salary',1).over(window_func))

emp_1 = emp.withColumn('denserank',denserank().over(window_func))
emp_1 = emp.withColumn('rank',rank().over(window_func))

 
 







