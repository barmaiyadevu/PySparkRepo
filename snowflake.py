
from pyspark.sql    import SparkSession
from pyspark    import SparkContext



spark = SparkSession.builder \
.master("local[1]") \
.appName("SparkByExamples.com") \
.config("spark.jars.packages", "net.snowflake:snowflake-jdbc-3.13.21,net.snowflake:spark-snowflake_2.13-2.10.0-spark_3.2") \
.getOrCreate()



simpleData = (("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Maria","Finance",3000),
    ("Raman","Finance",3000),
    ("Scott","Finance",3300),
    ("Jen","Finance",3900),
    ("Jeff","Marketing",3000),
    ("Kumar","Marketing",2000)
  )
rdd = spark.sparkContext.parallelize(simpleData)
df = rdd.toDF()
df.show()

sfOptions = {
    "sfURL"       : "https://krb79337.us-east-1.snowflakecomputing.com/",
    "sfAccount"   : "krb79337",
    "sfUser"      : "barmaiya123", 
    "sfPassword"  : "#Dev@123#",
    "sfDatabase"  : "Demo",
    "sfSchema"    : "PUBLIC",
    "sfRole"      : "ACCOUNTADMIN",
 
}

SNOWFLAKE_SOURCE_NAME= "net.snowflake.spark.snowflake"
df.write.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", "EMPLOYEE") \
    .mode('append') \
    .save()


