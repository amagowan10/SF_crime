# SF_crime

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It was noticeable how by setting maxRatePerPartition and maxOffsetsPerTrigger to low values reduced the throughput and latency, as could be seen in the numInputRows and processedRowsPerSecond fields respectively. Increasing these parameters then increased the throughput and latency, as seen with higher numInputRows and processedRowsPerSecond values. I also saw how the shuffle read and write memory increased quite significantly with increasing both parameters. Increasing the number of kafka partitions (in server.properties) seemed to significantly increase the throughput, but memory also.


2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Using high values of maxRatePerPartition and maxOffsetsPerTrigger produced the best results in terms of high latency and throughput, but with a trade-off of higher memory consumption. The memory consumption was still comparitively low though given the dataset size, so in this case was not an issue. In my case, setting maxRatePerPartition=1000 and maxOffsetsPerTrigger=1000 seemed optimal. The memory could be limited/managed by setting spark.driver.memory or spark.executor.memory, but in our case probably not necessary because we are still well below the storage memory limit seen in Executors Summary in Spark UI.
