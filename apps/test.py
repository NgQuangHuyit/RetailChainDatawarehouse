from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Simple Spark App") \
    .getOrCreate()

# Tạo một DataFrame
data = [("John", 25), ("Alice", 33), ("Bob", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Hiển thị DataFrame
print("DataFrame:")
df.show()

# Ghi DataFrame vào HDFS
hdfs_path = "hdfs://hadoop-namenode:9000/test/demo.csv"
df.write \
    .format("csv") \
    .mode("overwrite") \
    .save(hdfs_path)

# Đóng SparkSession
spark.stop()
