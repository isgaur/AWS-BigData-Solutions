You can control the number of files/ size-of-files being written out by choosing to reparation the data before the write. If the number of output files are many, I'd recommend calling dataframe.repartition(x) just before the write operation.  A code snippet would look like this:
''''

logs_DyF = glueContext.create_dynamic_frame.from_catalog(database="amzn_review", table_name="mydata_amazonreviews", transformation_ctx = "datasource0")
logs_DF=logs_DyF.toDF()
logs_DF.show()
print (logs_DF.show())
print ("The number of partitions in source is")
print (logs_DF.rdd.getNumPartitions())
logs_DF=logs_DF.repartition(50)

logs_DyF2=DynamicFrame.fromDF(logs_DF, glueContext, "logs_DyF2")
datasink2 = glueContext.write_dynamic_frame.from_options( frame = logs_DyF2, connection_type = "s3", connection_options = {"path": "s3://xx-xx/7017122531/output/", "partitionKeys" : ["product_category"] }, format = "parquet", transformation_ctx = "datasink2")
'''

Test the values of the X(partition number) to see if this work with your dataset. This link[2] has a reference to how you can get the number of ideal partitions:
Total input dataset size / partition size =>  number of partitions


---
[1]https://aws.amazon.com/premiumsupport/faqs/
[2]https://dzone.com/articles/apache-spark-performance-tuning-degree-of-parallel
