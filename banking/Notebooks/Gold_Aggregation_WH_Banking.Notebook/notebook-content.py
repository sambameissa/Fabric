# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cfb24935-c623-4d0d-a083-456c8083d59b",
# META       "default_lakehouse_name": "LH_Banking",
# META       "default_lakehouse_workspace_id": "833a6822-5e38-4c83-97a7-16269b6ecd1a",
# META       "known_lakehouses": [
# META         {
# META           "id": "cfb24935-c623-4d0d-a083-456c8083d59b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3d152e97-59fa-a77d-41f6-e197b0337efc",
# META       "known_warehouses": [
# META         {
# META           "id": "3d152e97-59fa-a77d-41f6-e197b0337efc",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🏦 Banking Medallion — 03 Gold Aggregation
# **Source** : `LH_Banking` (silver_*)  
# **Cible** : `WH_Banking` (dbo.*)  
# **Auteur** : Meissa Ndione SAMBA | DP-700

# MARKDOWN ********************

# ## 1. Configuration & connexion WH_Banking

# CELL ********************

# ============================================================
# CONFIGURATION
# ============================================================
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max, min,
    round as spark_round, datediff, current_date,
    when, current_timestamp, lit, date_format,
    countDistinct
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WH_Banking_Gold")

# Lecture Silver depuis LH_Banking (attaché par défaut)
# Écriture Gold dans WH_Banking via shortcut ou JDBC

# ── Récupération du token pour WH_Banking ──────────────────
from notebookutils import mssparkutils

try:
    token = mssparkutils.credentials.getToken(
        "https://analysis.windows.net/powerbi/api"
    )
    print("Token OK")
except Exception as e:
    print(f"Token error: {e}")
    print("Vérifier les permissions du workspace")

print("\nConfiguration :")
print("  Source : LH_Banking (silver_clients_clean, silver_transactions_clean)")
print("  Cible  : WH_Banking (dbo.client_kpis, dbo.monthly_kpis, dbo.suspicious_transactions)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Lecture Silver

# CELL ********************

# ── CELL 2 — Lecture Silver depuis LH_Banking ──────────────
# Chaque notebook est une session indépendante.
# Les DataFrames ne se transmettent pas entre notebooks.
# On relit toujours depuis les tables Delta persistées dans LH_Banking.

df_clients = spark.read.table("silver_clients_clean")
df_tx      = spark.read.table("silver_transactions_clean")

print(f"silver_clients_clean      : {df_clients.count()} rows")
print(f"silver_transactions_clean : {df_tx.count()} rows")

# Vérification rapide
df_clients.select("client_id","segment","city","risk_score","statut").show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Construction des tables Gold

# CELL ********************

# ── TABLE 1 : client_kpis ──────────────────────────────────
# KPIs consolidés par client — vue 360°
df_client_kpis = (
    df_clients.alias("c")
    .join(
        df_tx.filter(col("statut") == "Approuvé")
             .groupBy("client_id")
             .agg(
                 count("transaction_id")                        .alias("nb_transactions"),
                 spark_round(spark_sum("montant"), 2)           .alias("total_montant_chf"),
                 spark_round(avg("montant"), 2)                 .alias("avg_montant_chf"),
                 spark_round(max("montant"), 2)                 .alias("max_transaction_chf"),
                 max("date")                                    .alias("last_tx_date"),
                 count(when(col("is_suspicious") == 1, 1))      .alias("nb_suspicious_tx"),
                 countDistinct("type")                          .alias("nb_types_utilises"),
             ),
        on="client_id", how="left"
    )
    .withColumn("days_since_last_tx",
        datediff(current_date(), col("last_tx_date")))
    .withColumn("statut_activite",
        when(col("days_since_last_tx") > 180, "Inactif")
        .when(col("days_since_last_tx") > 60,  "A risque")
        .otherwise("Actif"))
    .withColumn("risque_fraude",
        when(col("nb_suspicious_tx") > 3,  "Elevé")
        .when(col("nb_suspicious_tx") > 0,  "Modéré")
        .otherwise("Faible"))
    .withColumn("_gold_built_ts", current_timestamp())
    .select(
        col("c.client_id"), col("c.nom"), col("c.prenom"),
        col("c.segment"), col("c.city"), col("c.risk_score"),
        col("c.statut"),
        "nb_transactions", "total_montant_chf", "avg_montant_chf",
        "max_transaction_chf", "last_tx_date", "days_since_last_tx",
        "nb_suspicious_tx", "nb_types_utilises",
        "statut_activite", "risque_fraude", "_gold_built_ts"
    )
)

print(f"client_kpis : {df_client_kpis.count()} rows")
df_client_kpis.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── TABLE 2 : monthly_kpis ─────────────────────────────────
# Agrégats mensuels — idéal pour courbes temporelles Power BI
df_monthly = (
    df_tx
    .withColumn("mois", date_format(col("date"), "yyyy-MM"))
    .groupBy("mois", "type", "canal", "devise")
    .agg(
        count("transaction_id")                             .alias("nb_transactions"),
        spark_round(spark_sum("montant"), 2)                .alias("volume_chf"),
        spark_round(avg("montant"), 2)                      .alias("avg_montant_chf"),
        count(when(col("statut") == "Refusé", 1))           .alias("nb_refuses"),
        count(when(col("is_suspicious") == 1, 1))           .alias("nb_suspects"),
        spark_round(
            count(when(col("statut") == "Refusé", 1)) * 100.0 / count("*"), 2
        )                                                   .alias("taux_refus_pct"),
    )
    .withColumn("_gold_built_ts", current_timestamp())
)

print(f"monthly_kpis : {df_monthly.count()} rows")
df_monthly.orderBy("mois").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── TABLE 3 : suspicious_transactions ─────────────────────
# Toutes les transactions suspectes enrichies — dashboard risque
df_suspicious = (
    df_tx.filter(col("is_suspicious") == 1)
    .join(
        df_clients.select("client_id","nom","prenom","segment","city","risk_score"),
        on="client_id", how="left"
    )
    .withColumn("_gold_built_ts", current_timestamp())
    .orderBy(col("montant").desc())
)

print(f"suspicious_transactions : {df_suspicious.count()} rows")
df_suspicious.select("client_id","transaction_id","nom","segment","montant","type","date").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_client_kpis.write.mode("overwrite").saveAsTable("Clients_kpis_Gold") 
df_monthly.write.mode("overwrite").saveAsTable("monthly_kpis_Gold")
df_suspicious.write.mode("overwrite").saveAsTable("suspicious_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Écriture dans WH_Banking

# CELL ********************

# ============================================================
# CELL 4 — APPROCHE 1 : saveAsTable qualifié
# La plus simple — Fabric résout WH_Banking dans le workspace
# ============================================================
tables = {
    "client_kpis":             df_client_kpis,
    "monthly_kpis":            df_monthly,
    "suspicious_transactions": df_suspicious,
}

try:
    for table_name, df in tables.items():
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"WH_Banking.dbo.{table_name}")
        )
        print(f"  OK  WH_Banking.dbo.{table_name} — {df.count()} rows")
    print("Approche 1 reussie")
except Exception as e:
    print(f"Approche 1 echouee : {e}")
    print("-> Tester Approche 2 ci-dessous")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
