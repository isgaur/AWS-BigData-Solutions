At times AWS Glue ETL Job fails with the error message : 

An error occurred while calling o85.pyWriteDynamicFrame. error while calling spill() on 
  org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter@5f4bb34a : No space left on device
  
Recommendation : 

This error message is thrown whenever a Spark application tries to spill data to disk to clear memory space and the underlying disk space becomes full. If a glue job is using a worker type say : G.1X worker type, which runs your jobs in nodes with 64 GiB volumes.

The most common cause for this issue is a lack of proper parallelism: if one of your job's Spark executors receives too much data (which can be caused by data skew, or by an improper repartition operation) it will try to save the excess data that cannot fit in memory to disk (spill to disk). The easiest way to address this is to ensure all executors receive an equal amount of load, and that there's enough executors to handle your dataset.

Resolution :

1) Enable metrics logging for your job, which will let you to check how many Spark executors your ETL Job is running with.

2) Run your job again to generate metrics for it.

3) Check the number of executors (metric 'glue.driver.ExecutorAllocationManager.executors.numberAllExecutors'). 

        3a) If the metric is reporting a value lower than that, there's a partitioning issue. There's several reasons as to why this could be happening, but the easiest way to address it typically is to add a repartition call after your read operation. The number of partitions should be 4 times the number of executors you can have, so 40 in this case.

        3b) If not, your job is partitioning properly and you will need to provision more resources (workers) to handle the amount of data. In order to understand how many you can check the 'glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors' metric, which will tell you how many executors are needed to process your dataset with maximum parallelism.
