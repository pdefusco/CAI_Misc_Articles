# How to Create and Run a Spark Submit in a CAI Job

## Objective

This article provides an introduction to creating and parameterizing a Spark Submit as a CAI Job.

## Motivation

Spark Submits can be executed in CAI leveraging the Spark on Kubernetes runtime. If you want to translate Spark Submits into CAI Jobs you have to create a CAI Job definition specifying Spark Submit options as CAI Job Arguments and update your Spark application code to parse those Spark options as sys args.

## Requirements

A CAI Workbench in Cloudera on Cloud or on Prem.

## Mapping a Spark Submit to CAI Job

Assume starting with a Spark Submit like this:

```
spark-submit my_app.py \
  --app-name MySparkJob \
  --shuffle-partitions 50 \
  --executor-memory 4g
```

If you want to run this as a CAI Job you have to refactor you PySpark code to parse three arguments: app name, shuffle partitions and executor memory:

 ```
 def get_arg(flag, default=None):
     """
     Simple helper to fetch command-line arguments.
     Example usage:
       --app-name MyApp
       --shuffle-partitions 50
     """
     if flag in sys.argv:
         index = sys.argv.index(flag)
         if index + 1 < len(sys.argv):
             return sys.argv[index + 1]
     return default


 # Read arguments from sys.argv
 app_name = get_arg("--app-name", "DefaultSparkApp")
 shuffle_partitions = get_arg("--shuffle-partitions", "200")
 executor_memory = get_arg("--executor-memory", "2g")
 ```

Then, in your application code, you can build your SparkSession object as shown below:

```
# Build SparkSession
spark = (
    SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.executor.memory", executor_memory)
        .getOrCreate()
)
```

## Creating the CAI Job

With the PySpark application provided in ```code/spark-job/simple-spark-app.py``` create the CAI job as shown below.









dd
