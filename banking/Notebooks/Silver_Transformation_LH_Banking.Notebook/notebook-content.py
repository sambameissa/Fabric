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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Banking Medallion — 02 Silver Transformation
# **Lakehouse** : `LH_Banking`  
# **Tables sources** : `bronze_clients_raw` · `bronze_transactions_raw`  
# **Tables cibles** : `silver_clients_clean` · `silver_transactions_clean`  

# MARKDOWN ********************

# ## 1. Configuration

# CELL ********************

from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, when,
    current_timestamp, sha2, concat_ws,
    regexp_replace, coalesce, lit
)
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LH_Banking_Silver")

print("Configuration OK — LH_Banking attaché par défaut")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Fonction de transformation Silver

# CELL ********************

def transform_silver(bronze_table: str, silver_table: str,
                     dedup_keys: list,
                     transforms: dict = None,
                     mandatory_cols: list = None) -> int:
    """
    Transforme les données Bronze vers Silver dans LH_Banking.
    Convention : bronze_{x} -> silver_{x}

    Opérations :
    1. Lecture depuis bronze_{bronze_table}
    2. Filtrage des nulls sur les clés obligatoires
    3. Déduplication sur dedup_keys
    4. Transformations métier (normalisation, typage)
    5. Ajout hash de lineage et timestamp
    6. Écriture dans silver_{silver_table}
    """
    src = f"bronze_{bronze_table}"
    tgt = f"silver_{silver_table}"
    logger.info(f"Transforming {src} -> {tgt}")

    df = spark.read.table(src)
    n0 = df.count()
    logger.info(f"  Source rows : {n0}")

    # Filtrage nulls obligatoires
    if mandatory_cols:
        for c in mandatory_cols:
            df = df.filter(col(c).isNotNull())
        logger.info(f"  After null filter : {df.count()}")

    # Déduplication
    df = df.dropDuplicates(dedup_keys)
    n1 = df.count()
    dups = n0 - n1
    if dups > 0:
        logger.warning(f"  {dups} duplicates removed")

    # Transformations custom
    if transforms:
        for col_name, transform_fn in transforms.items():
            df = transform_fn(df, col_name)

    # Hash de lineage (traçabilité bout-en-bout)
    df = (df
        .withColumn("_silver_row_hash",
            sha2(concat_ws("|", *[col(k) for k in dedup_keys]), 256))
        .withColumn("_silver_transformed_ts", current_timestamp())
    )

    # Écriture Delta
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tgt)
    )

    n2 = df.count()
    logger.info(f"  Silver OK : {n2} rows -> {tgt}")
    return n2

print("Fonction transform_silver prête")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Silver — Clients

# CELL ********************

n_clients = transform_silver(
    bronze_table   = "clients_raw",
    silver_table   = "clients_clean",
    dedup_keys     = ["client_id"],
    mandatory_cols = ["client_id"],
    transforms = {
        "nom":      lambda df, c: df.withColumn(c, trim(upper(col(c)))),
        "prenom":   lambda df, c: df.withColumn(c, trim(upper(col(c)))),
        "email":    lambda df, c: df.withColumn(c, lower(trim(col(c)))),
        "segment":  lambda df, c: df.withColumn(c, trim(col(c))),
        "statut":   lambda df, c: df.withColumn(c, upper(trim(col(c)))),
        "risk_score": lambda df, c: df.withColumn(c, col(c).cast("double")),
    }
)
print(f"silver_clients_clean : {n_clients} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Silver — Transactions

# CELL ********************

n_tx = transform_silver(
    bronze_table   = "transactions_raw",
    silver_table   = "transactions_clean",
    dedup_keys     = ["transaction_id"],
    mandatory_cols = ["transaction_id", "client_id"],
    transforms = {
        "montant":    lambda df, c: df.withColumn(c, col(c).cast("double")),
        "date":       lambda df, c: df.withColumn(c, to_date(col(c))),
        "client_id":  lambda df, c: df.withColumn(c, trim(col(c))),
        "type":       lambda df, c: df.withColumn(c, trim(col(c))),
        "canal":      lambda df, c: df.withColumn(c, trim(col(c))),
        "statut":     lambda df, c: df.withColumn(c, trim(col(c))),
        "is_suspicious": lambda df, c: df.withColumn(c, col(c).cast("integer")),
    }
)
print(f"silver_transactions_clean : {n_tx} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Bilan & contrôle

# CELL ********************

print("\n" + "="*55)
print("SILVER TRANSFORMATION — LH_Banking")
print("="*55)
print(f"  OK  silver_clients_clean      {n_clients:>8} rows")
print(f"  OK  silver_transactions_clean {n_tx:>8} rows")
print("="*55)

# Vérification jointure clients <-> transactions
df_c = spark.read.table("silver_clients_clean")
df_t = spark.read.table("silver_transactions_clean")

orphans = df_t.join(df_c, on="client_id", how="left_anti").count()
print(f"\nContrôle jointure :")
print(f"  Transactions sans client correspondant : {orphans}")
if orphans > 0:
    print("  -> Vérifier la cohérence des client_id entre les sources")

# Aperçu
print("\n-- silver_clients_clean (3 lignes) --")
df_c.select("client_id","nom","prenom","segment","city","risk_score","statut").show(3)

print("\n-- silver_transactions_clean (3 lignes) --")
df_t.select("transaction_id","client_id","date","montant","devise","type","statut","is_suspicious").show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
