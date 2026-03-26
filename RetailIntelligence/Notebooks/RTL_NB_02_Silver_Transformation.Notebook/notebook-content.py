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
# META           "id": "90666a77-e276-47a0-b114-b8a8c5aa291e"
# META         },
# META         {
# META           "id": "cf09a274-6f38-4c70-bc38-a231db5c83cd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# RTL_NB_02_Silver_Transformation — RetailIntelligence
# Auteur : Meissa Ndione SAMBA | DP-700
# Source : RTL_LH_Bronze.bronze_*
# Cible  : RTL_LH_Silver.silver_ via Pipelines

from pyspark.sql.functions import (col, trim, upper, to_date, when,
    current_timestamp, sha2, concat_ws, round as spark_round,
    lit, coalesce)
import logging
logger = logging.getLogger("RTL_Silver")

LH_BRONZE = "RTL_LH_Bronze"
LH_SILVER = "RTL_LH_Silver"

def transform(bronze, silver, dedup, mandatory=None, casts=None):
    df = spark.read.table(f"bronze_{bronze}")
    n0 = df.count()
    if mandatory:
        for c in mandatory: df = df.filter(col(c).isNotNull())
    df = df.dropDuplicates(dedup)
    if casts:
        for c, t in casts.items():
            if t == "date": df = df.withColumn(c, to_date(col(c)))
            else:           df = df.withColumn(c, col(c).cast(t))
    df = (df
        .withColumn("_silver_hash", sha2(concat_ws("|",*[col(k) for k in dedup]),256))
        .withColumn("_silver_ts",   current_timestamp()))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"silver_{silver}"))
    n1 = df.count()
    logger.info(f"Silver: {n0}→{n1} rows -> silver_{silver}")
    return n1

results = {}

results["magasins"] = transform(
    "magasins", "magasins", ["store_id"],
    mandatory=["store_id","ville"],
    casts={"surface_m2":"integer","nb_caisses":"integer",
           "population_zone":"integer","revenu_moyen_chf":"integer",
           "nb_concurrents":"integer","parking":"boolean",
           "date_ouverture":"date","latitude":"double","longitude":"double"}
)

results["produits"] = transform(
    "produits", "produits", ["product_id"],
    mandatory=["product_id","categorie"],
    casts={"prix_unitaire":"double","marge_pct":"double",
           "bio":"boolean","local":"boolean","premium":"boolean"}
)

results["transactions"] = transform(
    "transactions", "transactions", ["tx_id"],
    mandatory=["tx_id","store_id","product_id"],
    casts={"date":"date","semaine":"integer","mois":"integer",
           "annee":"integer","quantite":"integer",
           "ca_chf":"double","marge_chf":"double"}
)

results["zones_chalandise"] = transform(
    "zones_chalandise", "zones_chalandise", ["store_id"],
    mandatory=["store_id"],
    casts={"population_primaire":"integer","population_secondaire":"integer",
           "revenu_moyen_chf":"integer","pct_familles":"double",
           "pct_seniors":"double","pct_actifs":"double",
           "indice_pouvoir_achat":"double","nb_concurrents_directs":"integer",
           "part_de_marche_estimee_pct":"double",
           "potentiel_ca_zone_mchf":"double",
           "indice_attractivite":"double","flux_pietons_semaine":"integer",
           "accessibilite_score":"double"}
)

print("\n" + "="*60)
print("SILVER TRANSFORMATION — RTL_LH_Silver")
print("="*60)
for t, n in results.items():
    print(f"  OK  silver_{t:25} {n:>8} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
