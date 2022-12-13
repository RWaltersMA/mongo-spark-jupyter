from pyspark.sql.types import StructType,TimestampType, DoubleType
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from pyspark.sql.functions import lit,count

spark = SparkSession.\
        builder.\
        appName("streamingExample").\
        config("spark.jars","mongosparkv2.jar").\
        getOrCreate()


spark.sparkContext.setLogLevel('DEBUG')

priceSchema = ( StructType()
  .add('Date', TimestampType())
  .add('Price', DoubleType())
)

stream_df = (spark.readStream
                    .format("csv")
                    .option("header", "true")
                    .schema(priceSchema)
                    .option("maxFilesPerTrigger", 1)
                    .load("daily_csv*.csv"))

stream_df = stream_df.withColumn("Type", lit("NaturalGas"))


slidingWindows = stream_df.withWatermark("Date", "1 minute").groupBy(stream_df.Type, F.window(stream_df.Date, "7 day")).avg().orderBy(stream_df.Type,'window')

#THIS SECTION WILL WORK AND PRINT TO THE SCREEEN
query = (
  slidingWindows
    .writeStream
    .format("console")
    .queryName("counts2")
    .outputMode("append")
    .start())

#THIS IS WHERE WE WRITE TO MONGODB AND IT WILL JUST HANG

#query=(slidingWindows.writeStream.format("mongodb").option('spark.mongodb.connection.uri', 'mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0')\
#    .option('spark.mongodb.database', 'Pricing') \
#    .option('spark.mongodb.collection', 'NaturalGas') \
#    .option("checkpointLocation", "/tmp") \
#    .queryName("counts2") \
#    .outputMode("complete") \
#    .start())

# query.awaitTermination(1000);
