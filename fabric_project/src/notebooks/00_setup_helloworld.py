# simple test script to check connection
from pyspark.sql.types import *

data = [(1, "Hello"), (2, "World"), (3, "Fabric")]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("message", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# write sanity check
print("writing helloworld table...")
df.write.format("delta").mode("overwrite").saveAsTable("helloworld")

# read back
spark.sql("SELECT * FROM helloworld").show()

print("env ok")
