
from pyspark.sql    import SparkSession
from pyspark    import SparkContext

spark = SparkSession.builder \
.master("local[1]") \
.appName("SparkByExamples.com") \
.config("spark.jars", "spark-redshift_2.10-2.0.0.jar,RedshiftJDBC42-no-awssdk-1.2.36.1060.jar,minimal-json-0.9.4.jar,spark-avro_2.11-3.0.0.jar") \
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
#df.show()

# df.write.format('jdbc').options(
#       url='jdbc:redshift://redshift-cluster-1.cbx4agcrv2tq.us-east-1.redshift.amazonaws.com:5439/dev',	  
#       driver='com.amazon.redshift.jdbc42.Driver',
#       dbtable='public.df_load_test',
#       user='admin',
#       password='Devesh123').mode('overwrite').save() 

jdbcURL='jdbc:redshift://redshift-cluster-1.cbx4agcrv2tq.us-east-1.redshift.amazonaws.com:5439/dev',	  

df.write.format("com.databricks.spark.redshift"). \
option("url", jdbcURL). \
option("dbtable", "public.demo"). \
option("aws_iam_role", "arn:aws:iam::921907484086:role/copy_s3_redshift"). \
mode("overwrite").save()