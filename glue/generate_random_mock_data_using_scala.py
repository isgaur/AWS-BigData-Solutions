//Create a mutable list buffer based on a loop.

import scala.collection.mutable.ListBuffer

var lb = ListBuffer[(Int, Int, String)]()

for (i <- 1 to 5000) {
  lb += ((i, i*i, "Number is " + i + "."))
}

//Convert it to a data frame.

import spark.implicits._

val df = lb.toDF("value", "square", "description")

df.coalesce(5).write.parquet("<your-hdfs-path or s3 >/name.parquet")
