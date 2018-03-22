# Readme
This project splits the London Cycle Hire Data into two subsets, Weekend Journeys and Weekday Journeys, and saves the result as Parquet. This action is implemented twice, both in a slow and quick fashion, as discussed in this [blog post](https://matdeb-sl.github.io/blog/2018/03/20/apache-spark-performance.html), and will produce ~2GB of output data.

To work correctly you will need to download all the [data files](http://cycling.data.tfl.gov.uk/) and put them in a folder called `data/full-bike`

It is configured to be built and passed to Spark-Submit on a Yarn cluster, with a command as follows:

```
spark-submit   \
--class com.scottlogic.blog.analysis.BikeDataAnalysis \
--master yarn   \
--deploy-mode client  \
--executor-memory 4g   \
--num-executors 2 \
--conf spark.executor.instances=2   \
--total-executor-cores 6   \
spark-shuffle-performance-1.0-SNAPSHOT.jar   10000
```

Alternatively it may be run locally by uncommenting line 18 in BikeDataAnalysis.java, and commenting line 19.
