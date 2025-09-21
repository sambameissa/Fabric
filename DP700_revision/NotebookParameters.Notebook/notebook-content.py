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

# PARAMETERS CELL ********************

parameterCountry = "England"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read.table("mtomactual")
df = df.where(df.Country==parameterCountry)
display(df)

df.write.mode("overwrite").format("delta").saveAsTable("MtoMactualExtract")

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
