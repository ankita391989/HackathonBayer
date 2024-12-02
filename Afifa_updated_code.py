# Databricks notebook source
storageContainer = 'bayerhackathon'
storageAccount = 'adlsaccountbayers'
landingMountPoint = '/mnt/hackathon'
databricksScopeName = 'bayers'
storageAccountSasToken = 'access-key'
SasToken = dbutils.secrets.get(scope=databricksScopeName,key=storageAccountSasToken)
print(SasToken)
  

# COMMAND ----------

dbutils.fs.mount(source="wasbs://{}@{}.blob.core.windows.net".format(storageContainer, storageAccount), mount_point=landingMountPoint, extra_configs={"fs.azure.sas.{}.{}.blob.core.windows.net".format(storageContainer, storageAccount): SasToken})


# COMMAND ----------

dbutils.fs.ls(landingMountPoint)

# COMMAND ----------

df_customer = spark.read.format("csv").option("header", "true").load(landingMountPoint + "/customer.csv")
display(df_customer)

# COMMAND ----------

df_customer_behaviour = spark.read.format("csv").option("header", "true").load(landingMountPoint + "/customer_behaviour.csv")
display(df_customer_behaviour)

# COMMAND ----------

df_orderline = spark.read.format("csv").option("header", "true").load(landingMountPoint + "/order_line.csv")
display(df_orderline)

# COMMAND ----------

df_order = spark.read.format("csv").option("header", "true").load(landingMountPoint + "/order.csv")
display(df_order)
df_order_1 = df_order.drop()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#reading customer file

# COMMAND ----------

df_customer = spark.read.format("csv").option("header", "true").load(landingMountPoint + "/customer.csv")

# COMMAND ----------

#write customer file to bronze layer
bronzeMountPoint='dbfs:/mnt/hackathon/Bronze-bayer/'
df_customer.write.format("delta").mode("overwrite").save(bronzeMountPoint + "/customer")

# COMMAND ----------

#data clensing
df_customer_cleansed=df_customer.filter(col["Phone"]).isNotNull()
#saving cleansed data to silver layer
silverMountPoint = "/mnt/silver"    
df_customer_cleansed.write.format("delta").mode("overwrite").save(silverMountPoint + "/customer")


# COMMAND ----------

# Address split into two lines
transformed_customer_df = df_customer_cleansed.withColumn(
    "Address_Line1", col("Address").substr(0, 50)
).withColumn("Address_Line2", col("Address").substr(51, 100))


