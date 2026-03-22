# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bb091e02-75ad-496b-80bd-5fb83cb04cba",
# META       "default_lakehouse_name": "GOV_LH_Silver",
# META       "default_lakehouse_workspace_id": "6a73c087-0a74-4700-9367-4cdb58e9eee4",
# META       "known_lakehouses": [
# META         {
# META           "id": "bb091e02-75ad-496b-80bd-5fb83cb04cba"
# META         },
# META         {
# META           "id": "18f4c638-3287-4a68-a95b-093847255e60"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "cdc6c4b0-3e7b-9149-4d2d-61a0c6eb6ced",
# META       "known_warehouses": [
# META         {
# META           "id": "cdc6c4b0-3e7b-9149-4d2d-61a0c6eb6ced",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# GOV_NB_03_Gold_Aggregation — v3
# Auteur : Meissa Ndione SAMBA | DP-700
# Source : GOV_LH_Silver.silver_*
# Cible  : GOV_LH_Gold.gold_*
# Note   : écriture DW via SQL Editor (voir GOV_DW_Gold_load.sql)

from pyspark.sql.functions import (col, count, avg, max, sum as spark_sum,
    round as spark_round, when, current_timestamp, date_format)
import logging
logger = logging.getLogger("GOV_Gold")

LH_SILVER = "GOV_LH_Silver"
LH_GOLD   = "GOV_LH_Gold"

# ── Relecture Silver ─────────────────────────────────────────
df_assets = spark.read.table(f"{LH_SILVER}.silver_data_assets")
df_runs   = spark.read.table(f"{LH_SILVER}.silver_pipeline_runs")
df_dq     = spark.read.table(f"{LH_SILVER}.silver_dq_checks")

print(f"Silver loaded:")
print(f"  silver_data_assets   : {df_assets.count()} rows")
print(f"  silver_pipeline_runs : {df_runs.count()} rows")
print(f"  silver_dq_checks     : {df_dq.count()} rows")

# ── Gold 1 : Catalogue des actifs avec DQ score ──────────────
df_catalog = (
    df_assets.join(
        df_dq.groupBy("asset_id").agg(
            avg("dq_score")   .alias("avg_dq_score"),
            count("check_id") .alias("nb_checks"),
            count(when(col("status")=="FAILED",1)).alias("nb_dq_failed"),
            max("check_date") .alias("last_dq_date"),
        ), on="asset_id", how="left"
    )
    .withColumn("dq_status",
        when(col("avg_dq_score") >= 0.9,  "Good")
        .when(col("avg_dq_score") >= 0.75, "Warning")
        .otherwise("Critical"))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Gold 2 : KPIs pipelines ───────────────────────────────────
df_pipeline_kpis = (
    df_runs.groupBy("pipeline_name","layer")
    .agg(
        count("run_id")                            .alias("nb_runs"),
        count(when(col("status")=="SUCCEEDED",1))  .alias("nb_success"),
        count(when(col("status")=="FAILED",1))     .alias("nb_failed"),
        spark_round(avg("duration_sec"),1)         .alias("avg_duration_sec"),
        spark_round(avg("rows_processed"),0)       .alias("avg_rows_processed"),
        max("start_time")                          .alias("last_run_ts"),
    )
    .withColumn("success_rate",
        spark_round(col("nb_success") * 100.0 / col("nb_runs"), 2))
    .withColumn("pipeline_health",
        when(col("success_rate") >= 95, "Healthy")
        .when(col("success_rate") >= 80, "Degraded")
        .otherwise("Critical"))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Gold 3 : Tableau de bord DQ ──────────────────────────────
df_dq_dashboard = (
    df_dq.groupBy(
        "check_type", "status",
        date_format(col("check_date"),"yyyy-MM").alias("mois")
    )
    .agg(
        count("check_id")                .alias("nb_checks"),
        spark_round(avg("dq_score"),3)   .alias("avg_score"),
        spark_round(avg("value_found"),2).alias("avg_value"),
        spark_sum("nb_rows_failed")      .alias("total_rows_failed"),
    )
    .withColumn("_gold_ts", current_timestamp())
)

# ── Catalogue consolidé ───────────────────────────────────────
df_catalog_consolidated = df_catalog.select(
    "asset_id","asset_name","asset_type","sector","layer",
    "status","classification","quality_score","avg_dq_score",
    "dq_status","nb_checks","last_dq_date","owner"
)

# ── Écriture dans GOV_LH_Gold uniquement ─────────────────────
tables = {
    "data_catalog":          df_catalog,
    "pipeline_kpis":         df_pipeline_kpis,
    "dq_dashboard":          df_dq_dashboard,
    "catalog_consolidated":  df_catalog_consolidated,
}

print("\nÉcriture dans GOV_LH_Gold...")
for name, df in tables.items():
    tgt = f"{LH_GOLD}.gold_{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema","true").saveAsTable(tgt))
    print(f"  OK  {tgt:50} {df.count():>8} rows")

print("\n" + "="*60)
print("GOLD AGGREGATION — terminé")
print("="*60)
print("\nProchaine étape :")
print("  Exécuter GOV_DW_Gold_load.sql dans le SQL Editor de GOV_DW_Gold")
print("  pour copier les tables Gold vers le Warehouse.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
