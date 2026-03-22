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
# META         },
# META         {
# META           "id": "bb091e02-75ad-496b-80bd-5fb83cb04cba"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# GOV_NB_02_Silver_Transformation — Governance Workspace
# Auteur : Meissa Ndione SAMBA | DP-700
# Source : GOV_LH_Bronze.bronze_*
# Cible  : GOV_LH_Silver.silver_*
# Noms qualifiés — indépendant du Lakehouse par défaut

from pyspark.sql.functions import (col, trim, upper, to_date, to_timestamp,
    current_timestamp, sha2, concat_ws, when, lit)
import logging
logger = logging.getLogger("GOV_Silver")

# Lakehouses
LH_BRONZE = "GOV_LH_Bronze"
LH_SILVER = "GOV_LH_Silver"

def transform_silver(bronze_table, silver_table, dedup_keys,
                     transforms=None, mandatory=None):
    """
    Lit depuis GOV_LH_Bronze.bronze_{x}
    Écrit dans GOV_LH_Silver.silver_{x}
    Noms qualifiés — pas de dépendance au Lakehouse par défaut.
    """
    src = f"{LH_BRONZE}.bronze_{bronze_table}"
    tgt = f"{LH_SILVER}.silver_{silver_table}"

    df = spark.read.table(src)
    n0 = df.count()
    logger.info(f"Read {n0} rows from {src}")

    if mandatory:
        for c in mandatory:
            df = df.filter(col(c).isNotNull())

    df = df.dropDuplicates(dedup_keys)

    if transforms:
        for c, fn in transforms.items():
            df = fn(df, c)

    df = (df
        .withColumn("_silver_hash", sha2(concat_ws("|", *[col(k) for k in dedup_keys]), 256))
        .withColumn("_silver_ts",   current_timestamp())
    )

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tgt)
    )

    n1 = df.count()
    logger.info(f"Silver OK: {n0}→{n1} rows (-{n0-n1} dups) -> {tgt}")
    return n1

results = {}

results["data_assets"] = transform_silver(
    "data_assets", "data_assets",
    dedup_keys=["asset_id"],
    mandatory=["asset_id","asset_name"],
    transforms={
        "asset_name":     lambda df,c: df.withColumn(c, trim(col(c))),
        "layer":          lambda df,c: df.withColumn(c, upper(trim(col(c)))),
        "status":         lambda df,c: df.withColumn(c, upper(trim(col(c)))),
        "classification": lambda df,c: df.withColumn(c, upper(trim(col(c)))),
        "last_updated":   lambda df,c: df.withColumn(c, to_date(col(c))),
        "quality_score":  lambda df,c: df.withColumn(c, col(c).cast("double")),
        "has_pii":        lambda df,c: df.withColumn(c, col(c).cast("boolean")),
        "nb_rows":        lambda df,c: df.withColumn(c, col(c).cast("long")),
    }
)

results["pipeline_runs"] = transform_silver(
    "pipeline_runs", "pipeline_runs",
    dedup_keys=["run_id"],
    mandatory=["run_id","pipeline_name"],
    transforms={
        "start_time":     lambda df,c: df.withColumn(c, to_timestamp(col(c))),
        "end_time":       lambda df,c: df.withColumn(c, to_timestamp(col(c))),
        "rows_processed": lambda df,c: df.withColumn(c, col(c).cast("long")),
        "rows_failed":    lambda df,c: df.withColumn(c, col(c).cast("long")),
        "duration_sec":   lambda df,c: df.withColumn(c, col(c).cast("integer")),
        "status":         lambda df,c: df.withColumn(c, upper(trim(col(c)))),
    }
)

results["dq_checks"] = transform_silver(
    "dq_checks", "dq_checks",
    dedup_keys=["check_id"],
    mandatory=["check_id","asset_id"],
    transforms={
        "check_date":      lambda df,c: df.withColumn(c, to_date(col(c))),
        "value_found":     lambda df,c: df.withColumn(c, col(c).cast("double")),
        "threshold":       lambda df,c: df.withColumn(c, col(c).cast("double")),
        "nb_rows_checked": lambda df,c: df.withColumn(c, col(c).cast("long")),
        "nb_rows_failed":  lambda df,c: df.withColumn(c, col(c).cast("long")),
        "dq_score":        lambda df,c: df.withColumn(c, col(c).cast("double")),
        "status":          lambda df,c: df.withColumn(c, upper(trim(col(c)))),
    }
)

# Audit log — écrit dans GOV_LH_Silver
from pyspark.sql import Row
audit_rows = [Row(table=f"silver_{t}", action="TRANSFORM",
                  rows=n, ts=str(current_timestamp()))
              for t, n in results.items()]
(spark.createDataFrame(audit_rows)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema","true")
    .saveAsTable(f"{LH_SILVER}.silver_audit_log")
)

print("\n" + "="*60)
print("SILVER TRANSFORMATION — GOV_LH_Silver")
print("="*60)
for t, n in results.items():
    print(f"  OK  {LH_SILVER}.silver_{t:25} {n:>8} rows")
print(f"  OK  {LH_SILVER}.silver_audit_log")
print("="*60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
