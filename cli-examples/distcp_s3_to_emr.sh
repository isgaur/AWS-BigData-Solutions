https://aws.amazon.com/blogs/big-data/seven-tips-for-using-s3distcp-on-amazon-emr-to-move-data-efficiently-between-hdfs-and-amazon-s3/#6

Hadoop is optimized for reading a fewer number of large files rather than many small files, whether from S3 or HDFS. You can use S3DistCp to aggregate small files into fewer large files of a size that you choose, which can optimize your analysis for both performance and cost.

$ s3-dist-cp --src /data/incoming/hourly_table --dest s3://my-tables/processing/daily_table --targetSize=10 --groupBy=’.*/hourly_table/.*/(\d\d)/.*\.log’
