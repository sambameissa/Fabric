# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "233611bf-63cf-4ede-aabc-db1d05fa2150",
# META       "default_lakehouse_name": "DemoLakehouse",
# META       "default_lakehouse_workspace_id": "f7b3b11b-a43d-4bc8-aadf-82adc583708d",
# META       "known_lakehouses": [
# META         {
# META           "id": "233611bf-63cf-4ede-aabc-db1d05fa2150"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("dimgeography")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df.select(df.City, df.StateProvinceName.alias("State"))
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3 = df.where("GeographyKey >= 200")
display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3=df3.withColumn("IPLocation", df.IpAddressLocator.substr(1,3)).filter((df.CountryRegionCode =="FR") | (df.CountryRegionCode=="GB"))
display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT City,
# MAGIC        StateProvinceName AS State
# MAGIC FROM DemoLakehouse.dimgeography;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *, substr(IpAddressLocator,1,3) as IPLocation
# MAGIC From dimgeography
# MAGIC where GeographyKey >= 200 AND CountryRegionCode IN ("FR", "GB")


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.table("mtomactual")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.describe("Country","Location"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=df.select(df.Country, df.Location, df.Actual.cast("int"))
display(df)

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
