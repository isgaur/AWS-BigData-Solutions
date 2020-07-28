     1. OOM exceptions commonly happen when an Apache Spark job reads a large number of small files from Amazon Simple Storage Service (Amazon S3). Resolve driver OOM exceptions with DynamicFrames using one or more of the following methods. use useS3ListImplementation - It is explained here. [1]

    
    2. Add more DPU's - Please take a deeper look at the job execution metric which shows the required vs  allocated max dpu(s) and accordingly increase the number of DPU's. This is explained well here[4] how to interpret the metrics.

    3. If you are using Parquet format for the output datasets while writing , you can definitely use  --enable-s3-parquet-optimized-committer  —this   Enables the EMRFS S3-optimized committer for writing Parquet data into Amazon S3. You can supply the parameter/value pair via the AWS Glue console when creating or updating an AWS Glue job. Setting the value to true enables the committer. By default the flag is turned off. The details are provided here [5]

    4. Input Datasets Compression type - In case you are using parquet format for the input datasets , please ensure those are compressed . for example with parquet - snappy compression type goes really well.

    5. Too many small files or some large files in input ? - The input datasets as a general practice should not contain too many small file like in Kb's or MB's or few large files like in GB's. The number of input files for the datasets must be good in size and fairly distributed across the partitions.

    6. if the dataset (SOURCE) is partitioned - You can try using pushdown predicate as well within glue. this restricts the amount of data being read by spark in the first place. Explained here [6].

    7. Another thumb rule of spark while reading the input datasets please read the data only which is required. If there are certain attributes(columns) in the sourced which are not required in the output datasets - please drop/filter them in the first place. or there might be certain attributes which are used as a look-up/Reference - once these are used you can drop them early rather than taking them until the end write them in the output. This will save overall execution time and the memory on the driver and the executors.




Reference documentation:

[1] https://aws.amazon.com/premiumsupport/knowledge-center/glue-oom-java-heap-space-error/
[2] https://docs.aws.amazon.com/glue/latest/dg/monitor-profile-debug-oom-abnormalities.html
[3] https://aws.amazon.com/blogs/big-data/best-practices-to-scale-apache-spark-jobs-and-partition-data-with-aws-glue/
[4] https://docs.aws.amazon.com/glue/latest/dg/monitor-debug-capacity.html
[5] https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
[6] https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-partitions.html#aws-glue-programming-etl-partitions-pushdowns
