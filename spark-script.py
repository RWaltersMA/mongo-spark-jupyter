from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.input.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0").\
        config("spark.mongodb.output.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()

#reading dataframes from MongoDB
df = spark.read.format("mongo").load()

df.printSchema()

#let's change the data type to a timestamp
df = df.withColumn("tx_time", df.tx_time.cast("timestamp"))

#Here we are calculating a moving average
from pyspark.sql.window import Window
from pyspark.sql import functions as F

movAvg = df.withColumn("movingAverage", F.avg("price").over( Window.partitionBy("company_symbol").rowsBetween(-1,1)) )
movAvg.show()

#Saving Dataframes to MongoDB
movAvg.write.format("mongo").option("replaceDocument", "true").mode("append").save()

#Reading Dataframes from the Aggregation Pipeline in MongoDB
pipeline = "[{'$group': {_id:'$company_name', 'maxprice': {$max:'$price'}}},{$sort:{'maxprice':-1}}]"
aggPipelineDF = spark.read.format("mongo").option("pipeline", pipeline).option("partitioner", "MongoSinglePartitioner").load()
aggPipelineDF.show()

#using SparkSQL with MongoDB
movAvg.createOrReplaceTempView("avgs")

sqlDF=spark.sql("SELECT * FROM avgs WHERE movingAverage > 43.0")

sqlDF.show()
