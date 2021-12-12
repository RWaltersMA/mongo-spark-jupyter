# Using MongoDB with Jupyter Labs

This repository showcases how to leverage MongoDB data in your JupyterLab notebooks via the MongoDB Spark Connector and PySpark.  We will load financial security data from MongoDB, calculate a moving average then update the data in MongoDB with these new data.  This repository has two components:
- Docker files
- Data generator (optional, see end of this text)

The Docker files will spin up the following environment:

![Image of docker environment](https://github.com/RWaltersMA/mongo-spark-jupyter/blob/master/images/diagram.png)

## Getting the environment up and running

Execute the `run.sh` script file.  This runs the docker compose file which creates a three node MongoDB cluster, configures it as a replica set on prt 27017. Spark is also deployed in this environment with a master node located at port 8080 and two worker nodes listening on ports 8081 and 8082 respectively.  The MongoDB cluster will be used for both reading data into Spark and writing data from Spark back into MongoDB.

Note: You may have to mark the .SH file as runnable with the `chmod` command i.e. `chmod +x run.sh`

**If you are using Windows, launch a PowerShell command window and run the `run.ps1` script instead of run.sh.**

To verify our Spark master and works are online navigate to http://localhost:8080

The Jupyter notebook URL which includes its access token will be listed at the end of the script.  NOTE: This token will be generated when you run the docker image so it will be different for you.  Here is what it looks like:

![Image of url with token](https://github.com/RWaltersMA/mongo-spark-jupyter/blob/master/images/url.png)

If you launch the containers outside of the script, you can still get the URL by issuing the following command:

`docker exec -it jupyterlab  /opt/conda/bin/jupyter notebook list`

or

`docker exec -it jupyterlab  /opt/conda/bin/jupyter server list`


## Playing with MongoDB data in a Jupyter notebook

To use MongoDB data with Spark create a new Python Jupyter notebook by navigating to the Jupyter URL and under notebook select Python 3 :

![Image of New Python notebook](https://github.com/RWaltersMA/mongo-spark-jupyter/blob/master/images/newpythonnotebook.png)

Now you can run through the following demo script.  You can copy and execute one or more of these lines :

To start, this will create the SparkSession and set the environment to use our local MongoDB cluster.

```
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
```
Next load the dataframes from MongoDB
```
df = spark.read.format("mongo").load()
```
Let’s verify the data was loaded by looking at the schema:
```
df.printSchema()
```
We can see that the tx_time field is loaded as a string.  We can easily convert this to a time by issuing a cast statement:

`df = df.withColumn(‘tx_time”, df.tx_time.cast(‘timestamp’))`

Next, we can add a new ‘movingAverage’ column that will show a moving average based upon the previous value in the dataset.  To do this we leverage the PySpark Window function as follows:

```from pyspark.sql.window import Window
from pyspark.sql import functions as F

movAvg = df.withColumn("movingAverage", F.avg("price")
             .over( Window.partitionBy("company_symbol").rowsBetween(-1,1)) )
```
To see our data with the new moving average column we can issue a 
movAvg.show().

`movAvg.show()`

To update the data in our MongoDB cluster, we  use the save method.

`movAvg.write.format("mongo").option("replaceDocument", "true").mode("append").save()`

We can also use the power of the MongoDB Aggregation Framework to pre-filter, sort or aggregate our MongoDB data.

```
pipeline = "[{'$group': {_id:'$company_name', 'maxprice': {$max:'$price'}}},{$sort:{'maxprice':-1}}]"
aggPipelineDF = spark.read.format("mongo").option("pipeline", pipeline).option("partitioner", "MongoSinglePartitioner").load()
aggPipelineDF.show()
```

Finally we can use SparkSQL to issue ANSI-compliant SQL against MongoDB data as follows:

```
movAvg.createOrReplaceTempView("avgs")
sqlDF=spark.sql("SELECT * FROM avgs WHERE movingAverage > 43.0")
sqlDF.show()
```

In this repository we created a JupyterLab notebook, leaded MongoDB data, computed a moving average and updated the collection with the new data.  This simple example shows how easy it is to integrate MongoDB data within your Spark data science application.  For more information on the Spark Connector check out the [online documentation](https://docs.mongodb.com/spark-connector/master/).  For anyone looking for answers to questions feel free to ask them in the [MongoDB community pages](https://developer.mongodb.com/community/forums/c/connectors-integrations/48).  The MongoDB Connector for Spark is [open source](https://github.com/mongodb/mongo-spark) under the Apache license.  Comments/pull requests are encouraged and welcomed.  Happy data exploration!


# Data Generator (optional)

This repository comes with a small sample data set already so it is not necessary to use this tool, however, if you are interested in creating a larger dataset with the same type of financial security data you run the Python3 app within the DataGenerator directory.

`python3 create-stock-data.py`

Parameter | Description
--------- | ------------
-s | number of financial stock symbols to generate, default is 10
-c | MongoDB Connection String, default is mongodb://localhost
-d | MongoDB Database name default is Stocks
-w | MongoDB collection to write to default is Source
-r | MongoDB collection to read from default is Sink

This data generator tool is designed to write to one collection and read from another.  It is also used as part of a Kafka connector demo where the data is flowing through the Kafka system.  In our repository example, if you just want to see the data as it is written to the "Source" collection use the -r parameter as follows:

`python3 create-stock-data.py -r Source`

