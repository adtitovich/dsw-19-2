from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# динамические данные, явным образом задаем структуру json-контента
schema = StructType().add("id",IntegerType()).add("action", StringType())

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()

input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("subscribe", "netology-hw") \
  .option("failOnDataLoss", False) \
  .load()

# статичные данные
users_schema = StructType().add("id",IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

users_data = [(1,"Jimmy",18),(2,"Hank",48),(3,"Johnny",9),(4,"Erle",40)]

users = spark.createDataFrame(data=users_data,schema=users_schema)

#разберем входящий контент из json
json_stream = input_stream.select(col("timestamp").cast("string"), from_json(col("value").cast("string"), schema).alias("parsed_value"))
#проверим что все ок
#json_stream.printSchema()
#json_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#выделем интересующие элементы
clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))
#проверим
#clean_data.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()


#добавим агрегат - отображать число уникальных событий пользователя
stat_stream = clean_data.groupBy("id").count().select(col("id"), col("count").alias("actions_count"))
stat_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()


#делаем join
join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name, users.user_age, clean_data.action, clean_data.timestamp)
join_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

