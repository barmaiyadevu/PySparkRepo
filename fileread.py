from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())

sc = SparkContext.getOrCreate()
words = sc.parallelize ([("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
)

df = words.toDF()
df.show()

file =spark.read.csv("2008-2021_US_Movies.csv")
columns1 = ["Release_Date","Title","company","Cast ","Cast","Genre"]

file.show()