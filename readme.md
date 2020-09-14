#Using MongoDB with Jupyter Labs

This repository showcases how to leverage MongoDB data in your JupyterLab notebooks via the MongoDB Spark Connector and PySpark.  We will load financial security data from MongoDB, calculate a moving average then update the data in MongoDB with these new data.  This repository has two components:
- Docker files
- Data generator (optional)

The Docker files will spin up the following environment:

![Image of docker environment](https://github.com/RWaltersMA/mongo-spark-jupyter/blob/master/diagram.png)

First run a `build.sh` to create the docker images used for this respoitory.

Next, run the `run.sh` script file runs the docker compose file which creates a three node MongoDB cluster, configures it as a replica set on prt 27017. Spark is also deployed in this environment with a master node located at port 8080 and two worker nodes listening on ports 8081 and 8082 respectively.  The MongoDB cluster will be used for both reading data into Spark and writing data from Spark back into MongoDB. 

Note: You may have to mark these .SH files executable with the `chmod` command i.e. `chmod +x run.sh`

To verify our Spark master and works are online navigate to http://localhost:8080

We can verify that the Jupyter Lab is up and running by navigating to the URL: http://localhost:8888

To use MongoDB data with Spark create a new Python Jupyter notebook and SparkSession as follows:
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
Next, let’s verify the configuration worked by looking at the schema:
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

```movAvg.write.format("mongo").option("replaceDocument", "true").mode("append").save()```

In this repository we created a JupyterLab notebook, leaded MongoDB data, computed a moving average and updated the collection with the new data.  This simple example shows how easy it is to integrate MongoDB data within your Spark data science application.  For more information on the Spark Connector check out the [online documentation](https://docs.mongodb.com/spark-connector/master/).  For anyone looking for answers to questions feel free to ask them in the [MongoDB community pages](https://developer.mongodb.com/community/forums/c/connectors-integrations/48).  The MongoDB Connector for Spark is [open source](https://github.com/mongodb/mongo-spark) under the Apache license.  Comments/pull requests are encouraged and welcomed.  Happy data exploration!


