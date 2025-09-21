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

import pandas as pd

wrangler_sample_df = pd.read_csv("https://aka.ms/wrangler/titanic.csv")
display(wrangler_sample_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# CELL ********************



df = spark.sql("SELECT * FROM DemoLakehouse.dimcustomer LIMIT 1000")
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
