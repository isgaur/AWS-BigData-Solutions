df = sqlContext.createDataFrame([
         (7,"ishan","kompressor","mbenz"),
         (14,"john","wrangler","jeep"),],
         ["HOUR","NAME","car","brand"])

df.write.parquet("s3://xx-xx-xx/Glue/oge/cars/")

datasource0 = glueContext.create_dynamic_frame_from_options("s3", {'paths': ["s3://xx-xx-xx/Glue/ogenew/"], "recurse":True}, format="parquet",transformation_ctx = "datasource0")


datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": "s3://xx-xx-xx/Glue/ogenew_pp/", "partitionKeys": ["brand","car"]}, format = "parquet",transformation_ctx = "datasink2")
