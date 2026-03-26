# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf09a274-6f38-4c70-bc38-a231db5c83cd",
# META       "default_lakehouse_name": "RTL_LH_Bronze",
# META       "default_lakehouse_workspace_id": "6063f389-ce67-4732-bafe-51945b2f2584",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf09a274-6f38-4c70-bc38-a231db5c83cd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# RTL_NB_01_Bronze_Ingestion — RetailIntelligence
# Auteur : Meissa Ndione SAMBA | DP-700 | PMP®
# Source : Files/raw/ dans RTL_LH_Bronze
# Cibles : bronze_magasins · bronze_produits · bronze_transactions · bronze_zones

from pyspark.sql.functions import current_timestamp, lit, input_file_name
import logging
logger = logging.getLogger("RTL_Bronze")

LH_BRONZE = "RTL_LH_Bronze"

SOURCES = {
    "magasins":         ("Files/raw/magasins",         "RETAIL_MASTER"),
    "produits":         ("Files/raw/produits",         "PRODUCT_CATALOG"),
    "transactions":     ("Files/raw/transactions",     "POS_SYSTEM"),
    "zones_chalandise": ("Files/raw/zones_chalandise", "GEO_INTELLIGENCE"),
}

def ingest_bronze(path, table, system):
    df = (spark.read.format("csv")
          .option("header","true").option("inferSchema","true").load(path)
          .withColumn("_bronze_ts",     current_timestamp())
          .withColumn("_source_system", lit(system))
          .withColumn("_source_file",   input_file_name()))
    (df.write.format("delta").mode("overwrite")
       .option("mergeSchema","true")
       .saveAsTable(f"bronze_{table}"))
    n = df.count()
    logger.info(f"Bronze OK: {n} rows -> bronze_{table}")
    return n

results = {t: ingest_bronze(p, t, s) for t, (p, s) in SOURCES.items()}

print("\n" + "="*60)
print("BRONZE INGESTION — RetailIntelligence")
print("="*60)
for t, n in results.items():
    print(f"  OK  bronze_{t:25} {n:>8} rows")
print("="*60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
