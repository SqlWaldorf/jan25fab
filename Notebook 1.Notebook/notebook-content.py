# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b477bf02-50cf-4c6e-8ed8-8685041ae4a7",
# META       "default_lakehouse_name": "mondaydemo",
# META       "default_lakehouse_workspace_id": "0f6e3759-f9e8-45ed-af4b-8259a7337b94",
# META       "known_lakehouses": [
# META         {
# META           "id": "b477bf02-50cf-4c6e-8ed8-8685041ae4a7"
# META         },
# META         {
# META           "id": "19cb7bc4-a972-48f7-b305-949d45c8f89a"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "cbf230f0-06a3-42ea-b7a7-26e4a9ff811d",
# META       "known_warehouses": [
# META         {
# META           "id": "cbf230f0-06a3-42ea-b7a7-26e4a9ff811d",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM mondaydemo.babs.station_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("MyDemoDf")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE MyNewestTable AS SELECT * FROM MyDemoDf

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM MyDemoDf LIMIT 10


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO NicoTest VALUES(3,1), (4,2)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC show TABLES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mycol = "landmark"
display(df[mycol])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a=10
if a< 2:
    print('ok')
      print('end')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy(df.landmark).sum('dockcount', 'lat'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select((df.lat - df.long).alias("difference"),df.lat, df.long).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select([cn for (cn, dt) in df.dtypes if dt=='int']))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mylistofcolumns = ['lat', 'landmark']
df.select(mylistofcolumns).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
