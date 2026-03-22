# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "15b396be-f270-487e-9077-81182b434eea",
# META       "default_lakehouse_name": "GOV_LH_Bronze",
# META       "default_lakehouse_workspace_id": "6a73c087-0a74-4700-9367-4cdb58e9eee4",
# META       "known_lakehouses": [
# META         {
# META           "id": "15b396be-f270-487e-9077-81182b434eea"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# GOV_NB_01_Bronze_Ingestion — Governance Workspace
# Auteur : Meissa Ndione SAMBA
# Source : Files/raw/ dans GOV_LH_Bronze
# Cible  : bronze_* dans GOV_LH_Bronze

from pyspark.sql.functions import current_timestamp, lit, input_file_name
import logging
logger = logging.getLogger("GOV_Bronze")

SOURCES = {
    "data_assets":    ("Files/raw/data_assets.csv",    "GOVERNANCE_CATALOG"),
    "pipeline_runs":  ("Files/raw/pipeline_runs.csv",  "FABRIC_MONITOR"),
    "dq_checks":      ("Files/raw/dq_checks.csv",      "DQ_FRAMEWORK"),
}

def ingest_bronze(path, table, system):
    df = (spark.read.format("csv")
          .option("header","true").option("inferSchema","true").load(path)
          .withColumn("_bronze_ts",     current_timestamp())
          .withColumn("_source_system", lit(system))
          .withColumn("_source_file",   input_file_name()))
    (df.write.format("delta").mode("append")
       .option("mergeSchema","true").saveAsTable(f"bronze_{table}"))
    n = df.count()
    logger.info(f"Bronze OK: {n} rows -> bronze_{table}")
    return n

results = {t: ingest_bronze(p, t, s) for t, (p, s) in SOURCES.items()}

print("\n" + "="*55 + "\nBRONZE INGESTION — GOV_LH_Bronze\n" + "="*55)
for t, n in results.items():
    print(f"  OK  bronze_{t:30} {n:>8} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
