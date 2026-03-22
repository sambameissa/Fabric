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
# META           "id": "15b396be-f270-487e-9077-81182b434eea"
# META         },
# META         {
# META           "id": "18f4c638-3287-4a68-a95b-093847255e60"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# GOV_NB_05_Documentation — fixed v2
# Auteur : Meissa Ndione SAMBA | DP-700 | MCT
# Fix : gestion des tables non accessibles + schema explicite

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from datetime import datetime
import logging
logger = logging.getLogger("GOV_Documentation")

LH_BRONZE = "GOV_LH_Bronze"
LH_SILVER = "GOV_LH_Silver"
LH_GOLD   = "GOV_LH_Gold"
DW_GOLD   = "GOV_DW_Gold"

print("Génération de la documentation automatique...")
print("=" * 60)

# ── 1. DICTIONNAIRE DE DONNÉES ───────────────────────────────
print("\n[1/3] Dictionnaire de données...")

# Le notebook 05 est attaché à GOV_LH_Silver par défaut
# On tente d abord le nom qualifié, puis le nom court si Silver est le défaut

tables_to_document = {
    # Bronze — lecture via nom qualifié
    "bronze_data_assets":   (LH_BRONZE, "BRONZE"),
    "bronze_pipeline_runs": (LH_BRONZE, "BRONZE"),
    "bronze_dq_checks":     (LH_BRONZE, "BRONZE"),
    # Silver — accessible directement (Lakehouse par défaut)
    "silver_data_assets":   (LH_SILVER, "SILVER"),
    "silver_pipeline_runs": (LH_SILVER, "SILVER"),
    "silver_dq_checks":     (LH_SILVER, "SILVER"),
    # Gold — lecture via nom qualifié
    "gold_data_catalog":    (LH_GOLD,   "GOLD"),
    "gold_pipeline_kpis":   (LH_GOLD,   "GOLD"),
    "gold_dq_dashboard":    (LH_GOLD,   "GOLD"),
    "gold_dq_report":       (LH_GOLD,   "GOLD"),
}

dict_rows = []

for table_name, (lakehouse, layer) in tables_to_document.items():
    df = None
    # Tentative 1 : nom qualifié
    try:
        df = spark.read.table(f"{lakehouse}.{table_name}")
    except Exception as e1:
        # Tentative 2 : nom court (si Lakehouse par défaut)
        try:
            df = spark.read.table(table_name)
        except Exception as e2:
            print(f"  SKIP {table_name}: {e2}")
            continue

    if df is not None:
        nb_rows = df.count()
        for field in df.schema.fields:
            dict_rows.append((
                f"{lakehouse}.{table_name}",
                table_name,
                layer,
                field.name,
                str(field.dataType),
                field.nullable,
                nb_rows,
                field.name.startswith("_"),
                "GOV_NB_05_Documentation",
                datetime.now().isoformat(),
            ))
        print(f"  OK  {lakehouse}.{table_name:30} {len(df.schema.fields)} cols | {nb_rows} rows")

print(f"\n  Total : {len(dict_rows)} entrées dans le dictionnaire")

# Schema explicite pour éviter l erreur CANNOT_INFER_EMPTY_SCHEMA
dict_schema = StructType([
    StructField("full_table_name", StringType(),  True),
    StructField("table_name",      StringType(),  True),
    StructField("layer",           StringType(),  True),
    StructField("column_name",     StringType(),  True),
    StructField("data_type",       StringType(),  True),
    StructField("nullable",        BooleanType(), True),
    StructField("nb_rows",         LongType(),    True),
    StructField("is_technical",    BooleanType(), True),
    StructField("documented_by",   StringType(),  True),
    StructField("doc_ts",          StringType(),  True),
])

if dict_rows:
    df_dict = spark.createDataFrame(dict_rows, schema=dict_schema)
    tgt_dict = f"{LH_GOLD}.gold_data_dictionary"
    (df_dict.write.format("delta").mode("overwrite")
     .option("overwriteSchema","true").saveAsTable(tgt_dict))
    print(f"  Sauvegardé -> {tgt_dict} ({df_dict.count()} entrées)")
else:
    print("  ATTENTION : aucune table documentée — vérifier que NB_01 à NB_04 ont été exécutés")

# ── 2. DATA LINEAGE ───────────────────────────────────────────
print("\n[2/3] Data Lineage...")

lineage_schema = StructType([
    StructField("source",       StringType(), True),
    StructField("target",       StringType(), True),
    StructField("operation",    StringType(), True),
    StructField("target_layer", StringType(), True),
    StructField("notebook",     StringType(), True),
    StructField("lineage_ts",   StringType(), True),
])

lineage_data = [
    ("Files/raw/data_assets",                    f"{LH_BRONZE}.bronze_data_assets",    "INGEST",    "BRONZE", "GOV_NB_01", datetime.now().isoformat()),
    ("Files/raw/pipeline_runs",                  f"{LH_BRONZE}.bronze_pipeline_runs",  "INGEST",    "BRONZE", "GOV_NB_01", datetime.now().isoformat()),
    ("Files/raw/dq_checks",                      f"{LH_BRONZE}.bronze_dq_checks",      "INGEST",    "BRONZE", "GOV_NB_01", datetime.now().isoformat()),
    (f"{LH_BRONZE}.bronze_data_assets",          f"{LH_SILVER}.silver_data_assets",    "TRANSFORM", "SILVER", "GOV_NB_02", datetime.now().isoformat()),
    (f"{LH_BRONZE}.bronze_pipeline_runs",        f"{LH_SILVER}.silver_pipeline_runs",  "TRANSFORM", "SILVER", "GOV_NB_02", datetime.now().isoformat()),
    (f"{LH_BRONZE}.bronze_dq_checks",            f"{LH_SILVER}.silver_dq_checks",      "TRANSFORM", "SILVER", "GOV_NB_02", datetime.now().isoformat()),
    (f"{LH_SILVER}.silver_data_assets",          f"{LH_GOLD}.gold_data_catalog",       "AGGREGATE", "GOLD",   "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_SILVER}.silver_dq_checks",            f"{LH_GOLD}.gold_data_catalog",       "ENRICH",    "GOLD",   "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_SILVER}.silver_pipeline_runs",        f"{LH_GOLD}.gold_pipeline_kpis",      "AGGREGATE", "GOLD",   "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_SILVER}.silver_dq_checks",            f"{LH_GOLD}.gold_dq_dashboard",       "AGGREGATE", "GOLD",   "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_GOLD}.gold_data_catalog",             f"{DW_GOLD}.dbo.data_catalog",        "COPY",      "DW",     "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_GOLD}.gold_pipeline_kpis",            f"{DW_GOLD}.dbo.pipeline_kpis",       "COPY",      "DW",     "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_GOLD}.gold_dq_dashboard",             f"{DW_GOLD}.dbo.dq_dashboard",        "COPY",      "DW",     "GOV_NB_03", datetime.now().isoformat()),
    (f"{LH_GOLD}.gold_data_catalog",             f"{DW_GOLD}.dbo.catalog_consolidated","COPY",      "DW",     "GOV_NB_05", datetime.now().isoformat()),
]

df_lineage = spark.createDataFrame(lineage_data, schema=lineage_schema)
tgt_lineage = f"{LH_GOLD}.gold_data_lineage"
(df_lineage.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(tgt_lineage))
print(f"  {tgt_lineage} : {df_lineage.count()} relations")
for row in lineage_data:
    print(f"    {row[0]:55} --> {row[1]}")

# ── 3. CATALOGUE CONSOLIDÉ ────────────────────────────────────
print("\n[3/3] Catalogue consolidé...")

try:
    # Lecture directe sans fonction
    spark.catalog.setCurrentDatabase(LH_GOLD)
    df_cat = spark.read.table("gold_data_catalog")

    df_catalog_cons = (
        df_cat.select(
            "asset_id","asset_name","asset_type","sector","layer",
            "status","classification","quality_score","avg_dq_score",
            "dq_status","nb_checks","last_dq_date","owner"
        )
        .withColumn("catalog_ts", lit(datetime.now().isoformat()))
        .withColumn("documented", lit(True))
    )

    # Écriture dans GOV_LH_Gold
    (df_catalog_cons.write.format("delta").mode("overwrite")
     .option("overwriteSchema","true")
     .saveAsTable("gold_catalog_consolidated"))

    spark.catalog.setCurrentDatabase(LH_SILVER)  # restore défaut

    print(f"  OK  {LH_GOLD}.gold_catalog_consolidated : {df_catalog_cons.count()} actifs")
    print(f"  -> Copier vers DW via SQL Editor : GOV_DW_Gold_load.sql")

except Exception as e:
    spark.catalog.setCurrentDatabase(LH_SILVER)  # restore même en cas d'erreur
    print(f"  SKIP : {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
