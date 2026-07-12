# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Event Hubs configuration
EH_NAMESPACE = "UberEventsNS"
EH_NAME = "ubertopic"
# EH_CONN_STR= spark.conf.get("CONNECTION_STRING")
EH_CONN_STR='Endpoint=sb://ubereventsns.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

df = spark.readStream.format("kafka")\
            .options(**KAFKA_OPTIONS)\
            .load()

df = df.withColumn("rides", col("value").cast(StringType()))


# COMMAND ----------

# DBTITLE 1,Display streaming data
# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM uber.bronze.stg_rides

# COMMAND ----------

