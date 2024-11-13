##  # PYSPARK-NOTES

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
 # agg(max/min/avg/sum/count)

 from pyspark.sql.functions import col, min, desc
from pyspark.sql.window import Window

emp_sorted = Window.partitionBy(col("department_id")).orderBy(col("salary"))

emp_3 = emp.withColumn("minsalary", min(col("salary")).over(emp_sorted))
emp_3.show()


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

============================       revision day                   =========================================
CHP - 6
# repartitiong/ colasec 

* repartition(n) is used for both increasing or decreasing partitions, and it triggers a shuffle.
* coalesce(n) is best for reducing partitions without a shuffle, as it minimizes computation cost.

emp_partitioned = emp.repartition(10)
emp.rdd.getnumpartitions()

 emp_partitioned = emp.repartition(10,"deptid")
 emp.rdd.getnumpartitions()

 # coalesc

 emp_partitioned = emp.coalesce(4)
 emp.rdd.getnumpartitions()

# to see parition infor

from pyspark.sql.functions import spark_partition_id

emp1 = emp.withColumn("repartition",spark_partition_id())
emp1.show()

# to segegrate into same num
from pyspark.sql.functions import spark_partition_id

emp1 = emp.repartition(4,"departmentid").withColumn("repartition",spark_partition_id())
emp1.show()
 
 # joins 

 emp_joined = emp.join(dept, how= "inner", on= emp.department_id == dept.department_id)
 emp_joined.show()
 emp_joined.select("e.dep_name","d.salary").show()


 emp_joined = emp.alias("e")join(dept.alias("d"), how= "left", on= emp.department_id == dept.department_id)
 emp_joined.show()
 emp_joined.select("e.dep_name","d.salary").show()

# cluster & deployment 

The driver tells the cluster manager what needs to be done.
The cluster manager assigns workers to carry out the tasks.
The worker nodes execute the tasks, and the driver gathers the results.

# CLUSTER TYPES
1. STANDALONE - LOCAL USE
2. HADDOOP - BIG DATA 
3. MESOS - MULTIPLE 
4. KUBERNETER - CLOUD

# CLUSTER DEPLOYMNT TYPES
1. CLIENT NODE - CLIENT LAPTOP/USE FOR SAVE AND DEVELP TESTING
2. CLUSTER NODE - CLUSTER ITSELF / USE FOR PRODUCTION ENVT

# CLUSTER MODES 
STANDARAD  - SINGLE PERSON
HIGH CONCURENCY - TEAM WORK
SINGLE MODE  - DRIVER NODE ONLY THERE NO WORKER NODE

# DEPLOYMENT 

# Configuring the Cluster: Deciding where your code will run—this could be on a cluster that’s managed by Spark itself, or by systems like YARN (in Hadoop) or Kubernetes. You’re setting up the infrastructure so it can handle the workload.

# Submitting Jobs: After setting up, you send your PySpark job (your code) to the cluster to execute. This is called submitting a job—just like assigning a task to a team.

# Scheduling and Monitoring: Once the job is running on the cluster, you’ll set it up to run at specific times if needed (like every day or every week). You also keep an eye on the job to make sure it runs smoothly, without errors.

======================================
# CATCHE & PERSIST 

- cache and persist are used to save a DataFrame or RDD in memory so that if you need to use it multiple times, you don't have to re-compute it from scratch.

- What it does: When you use cache(), Spark saves the DataFrame/RDD in memory (RAM).

- When to use: If you need to use the same data several times in a program, caching helps because Spark doesn’t have to load or process it repeatedly.

- How to use: Simply call df.cache() on your DataFrame.

df_1 = df.cache()
df_1.count()  # First time, it loads and caches
df_1.show()   # Second time, it uses the cached data


# PERSIST (IN MEMORY + DISK + MORE/ BOTH)

What it does: persist() is more flexible. You can choose to store the data in memory, on disk, or both.

When to use: Use persist() if:

The data is too large for memory, so you want to save some on disk as well.

# SYNTAX 
from pyspark import StorageLevel
df_1= df.persist(StorageLevel.MEMORY_AND_DISK)

df_1.unpersist() == to clear save data 

# ERROR HANDLING 
----------------------------- 3 METHODS ---------------------
MODE OPTION

1. PERMISSIVE- BAD RECORDS WILL BE IGNORE AND NULL WILL BE REPLACED 
2. DROPMALFORMED - IT WILL DROP VALUES INSTEAD OF NULL
3. FAILFAST - IT WILL THROW AN ERROR 

# SYNTAX 
df = spark.read.option("mode", "PERMISSIVE").csv("data.csv") 

# BAD RECORD PATH -- THIS IS USEFUL WHEN I NEED TO MAKE ONE SEPARATE FOLDER WHICH HOLDS ONLY BAD RECORDS
df = spark.read \ 
    .option("badRecordsPath", "path/to/save/bad/records") \ 
    .csv("data.csv") 



