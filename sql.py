#just add mysql-connector-java-5.1.27-bin.jar to $SPARK_HOME/jars.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('myapp'). \
     config("spark.driver.extraClassPath", "mysql:mysql-connector-java-5.1.47") \
     .getOrCreate()

data = spark.read.format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver"). \
    option("url","jdbc:mysql://localhost:3306?useSSL=false") \
    .option("dbtable","dev.customers" ) \
    .option("user","root") \
    .option("password","admin").load()

data.createOrReplaceTempView("tblcustomers")
res = spark.sql("select customerName,contactLastName,contactFirstName from tblcustomers limit 8")
res.show()