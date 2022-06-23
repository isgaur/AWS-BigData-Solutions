#pre-requisite is to install farsante which requires python > 3.7 
# the below program will generate 1000000 records in the dataset - you can change that per the req.

import farsante
from mimesis import Person
from mimesis import Address
from mimesis import Datetime
from mimesis import Internet

person = Person()
address = Address()
datetime = Datetime()
internet= Internet() 


df = farsante.pyspark_df([datetime.datetime, person.age, person.identifier, internet.ip_v4, internet.ip_v6], 1000000)


df.write.mode('overwrite').parquet("s3://your-s3-bucket/glue/fact_tbl/")

