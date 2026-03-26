# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "90666a77-e276-47a0-b114-b8a8c5aa291e",
# META       "default_lakehouse_name": "RTL_LH_Silver",
# META       "default_lakehouse_workspace_id": "6063f389-ce67-4732-bafe-51945b2f2584",
# META       "known_lakehouses": [
# META         {
# META           "id": "90666a77-e276-47a0-b114-b8a8c5aa291e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# RTL_NB_03_Gold_Aggregation — RetailIntelligence
# Auteur : Meissa Ndione SAMBA | DP-700
# Source : RTL_LH_Silver.silver_*
# Cible  : RTL_LH_Gold.gold_* + RTL_DW_Gold.dbo.*

from pyspark.sql.functions import (col, count, sum as spark_sum, avg,
    max, min, round as spark_round, when, current_timestamp,
    date_format, countDistinct, rank, lit, lag, datediff)
from pyspark.sql.window import Window
import logging
logger = logging.getLogger("RTL_Gold")

LH_SILVER = "RTL_LH_Silver"
LH_GOLD   = "RTL_LH_Gold"

# Relecture Silver
df_mag  = spark.read.table("silver_magasins")
df_prod = spark.read.table("silver_produits")
df_tx   = spark.read.table("silver_transactions")
df_zon  = spark.read.table("silver_zones_chalandise")

print(f"Silver: {df_mag.count()} magasins | {df_prod.count()} produits | "
      f"{df_tx.count()} transactions | {df_zon.count()} zones")

# ── Gold 1 : Performance commerciale par magasin ──────────────
df_store_kpis = (
    df_tx.groupBy("store_id","annee","mois")
    .agg(
        spark_round(spark_sum("ca_chf"),2)      .alias("ca_chf"),
        spark_round(spark_sum("marge_chf"),2)   .alias("marge_chf"),
        spark_sum("quantite")                   .alias("nb_unites"),
        count("tx_id")                          .alias("nb_transactions"),
        countDistinct("product_id")             .alias("nb_references"),
        count(when(col("statut_stock")=="Rupture",1)).alias("nb_ruptures"),
        count(when(col("statut_stock")=="Promo",1)) .alias("nb_promos"),
    )
    .withColumn("panier_moyen_chf",
        spark_round(col("ca_chf")/col("nb_transactions"),2))
    .withColumn("taux_marge_pct",
        spark_round(col("marge_chf")/col("ca_chf")*100,2))
    .withColumn("taux_rupture_pct",
        spark_round(col("nb_ruptures")/col("nb_transactions")*100,2))
    .join(df_mag.select("store_id","nom","ville","format","concept",
                         "surface_m2","statut"), on="store_id", how="left")
    .withColumn("ca_par_m2", spark_round(col("ca_chf")/col("surface_m2"),2))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Gold 2 : Mix produits par magasin ─────────────────────────
df_product_mix = (
    df_tx.join(df_prod.select("product_id","categorie","famille",
                               "bio","local","premium"), on="product_id")
    .groupBy("store_id","annee","categorie","famille")
    .agg(
        spark_round(spark_sum("ca_chf"),2)    .alias("ca_chf"),
        spark_round(spark_sum("marge_chf"),2) .alias("marge_chf"),
        spark_sum("quantite")                 .alias("nb_unites"),
        countDistinct("product_id")           .alias("nb_references"),
        count(when(col("bio")==True,1))       .alias("nb_bio"),
        count(when(col("local")==True,1))     .alias("nb_local"),
    )
    .join(df_mag.select("store_id","nom","ville","surface_m2"),
          on="store_id", how="left")
    .withColumn("ca_par_m2",
        spark_round(col("ca_chf")/col("surface_m2"),2))
    .withColumn("taux_marge_pct",
        spark_round(col("marge_chf")/col("ca_chf")*100,2))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Gold 3 : Scoring géomarketing ────────────────────────────
w_ville = Window.partitionBy("ville")
df_geo = (
    df_zon.join(df_mag.select("store_id","nom","format",
                               "concept","surface_m2","statut",
                               "latitude","longitude"),
                on="store_id", how="left")
    .join(
        df_tx.groupBy("store_id").agg(
            spark_round(spark_sum("ca_chf"),2).alias("ca_total_chf"),
            spark_round(avg("ca_chf"),2)      .alias("ca_moyen_hebdo"),
        ), on="store_id", how="left"
    )
    .withColumn("taux_captation_pct",
        spark_round(col("ca_total_chf")/
                    (col("potentiel_ca_zone_mchf")*1000000)*100,2))
    .withColumn("score_potentiel_implantation",
        spark_round(
            (col("indice_pouvoir_achat")/100 * 0.30) +
            (col("indice_attractivite")/100 * 0.25) +
            (col("accessibilite_score")/10  * 0.20) +
            ((10-col("nb_concurrents_directs"))/10 * 0.25) * 100
        , 1))
    .withColumn("recommandation",
        when(col("score_potentiel_implantation") >= 75, "Implantation prioritaire")
        .when(col("score_potentiel_implantation") >= 55, "Potentiel modéré")
        .when(col("statut") == "A rénover",              "Rénovation recommandée")
        .otherwise("Surveiller"))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Gold 4 : Tendances temporelles ───────────────────────────
df_trends = (
    df_tx.groupBy("annee","mois","store_id")
    .agg(spark_round(spark_sum("ca_chf"),2).alias("ca_chf"))
    .join(df_mag.select("store_id","ville","format"), on="store_id")
    .groupBy("annee","mois","ville","format")
    .agg(spark_round(spark_sum("ca_chf"),2).alias("ca_chf"),
         count("store_id").alias("nb_magasins"))
    .withColumn("ca_moyen_par_magasin",
        spark_round(col("ca_chf")/col("nb_magasins"),2))
    .withColumn("_gold_ts", current_timestamp())
)

# ── Écriture dans RTL_LH_Gold ─────────────────────────────────


tables = {
    "store_performance_kpis": df_store_kpis,
    "product_mix_analysis":   df_product_mix,
    "geomarketing_scoring":   df_geo,
    "sales_trends":           df_trends,
}

print("\nÉcriture dans RTL_LH_Gold...")
for name, df in tables.items():
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"gold_{name}"))
    print(f"  OK  gold_{name:30} {df.count():>8} rows")



print("\n" + "="*60)
print("GOLD AGGREGATION — terminé")
print("="*60)
print("Prochaine étape : RTL_DW_Gold_load.sql")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
