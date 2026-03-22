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

# #  Banking Medallion — 01 Bronze Ingestion
# **Lakehouse** : `LH_Banking`  
# **Tables cibles** : `bronze_clients_raw` · `bronze_transactions_raw`  
# **Auteur** : Meissa Ndione SAMBA

# MARKDOWN ********************

# ## 1. Configuration

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LH_Banking_Bronze")

# Chemins relatifs — LH_Banking attaché par défaut
PATH_CLIENTS      = "Files/Raw/clients/clients.csv"
PATH_TRANSACTIONS = "Files/Raw/transactions/transactions.csv"

print("Configuration OK")
print(f"  Source clients      : {PATH_CLIENTS}")
print(f"  Source transactions : {PATH_TRANSACTIONS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Vérification des fichiers sources

# CELL ********************

# Vérification que les fichiers sont bien présents
from notebookutils import mssparkutils

for path in [PATH_CLIENTS, PATH_TRANSACTIONS]:
    try:
        files = mssparkutils.fs.ls(path)
        print(f"OK  {path}")
        for f in files:
            print(f"      {f.name}  ({round(f.size/1024, 1)} KB)")
    except Exception as e:
        print(f"ERR {path} — {e}")
        print("     -> Vérifier que le fichier CSV est bien uploadé dans Files/raw/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Fonction d'ingestion Bronze

# CELL ********************

def ingest_bronze(source_path: str, table_name: str,
                  source_system: str, file_format: str = "csv") -> int:
    """
    Ingère les données brutes vers la couche Bronze dans LH_Banking.
    Convention de nommage : bronze_{table_name}
    
    - Aucune transformation des données source
    - Ajout de colonnes techniques de traçabilité
    - Mode append : conserve l historique
    """
    full_table = f"bronze_{table_name}"
    logger.info(f"Ingesting {source_path} -> {full_table}")

    # Lecture
    reader = spark.read.format(file_format).option("header", "true").option("inferSchema", "true")
    df = reader.load(source_path)

    # Colonnes techniques Bronze
    df_bronze = (df
        .withColumn("_bronze_ingestion_ts",  current_timestamp())
        .withColumn("_bronze_source_system", lit(source_system))
        .withColumn("_bronze_source_file",   input_file_name())
        .withColumn("_bronze_batch_id",      lit(f"{source_system}_{table_name}"))
    )

    # Écriture Delta dans LH_Banking
    (df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_table)
    )

    count = df_bronze.count()
    logger.info(f"Bronze OK: {count} rows -> {full_table}")
    return count

print("Fonction ingest_bronze prête")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Exécution

# CELL ********************

results = {}

results["clients"] = ingest_bronze(
    source_path   = PATH_CLIENTS,
    table_name    = "clients_raw",
    source_system = "CRM"
)

results["transactions"] = ingest_bronze(
    source_path   = PATH_TRANSACTIONS,
    table_name    = "transactions_raw",
    source_system = "CORE_BANKING"
)

print("\n" + "="*55)
print("BRONZE INGESTION — LH_Banking")
print("="*55)
for table, count in results.items():
    print(f"  OK  bronze_{table:25} {count:>8} rows")
print("="*55)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Contrôle rapide

# CELL ********************

# Aperçu des tables Bronze créées
print("\n-- bronze_clients_raw --")
spark.read.table("bronze_clients_raw").printSchema()
spark.read.table("bronze_clients_raw").show(3, truncate=True)

print("\n-- bronze_transactions_raw --")
spark.read.table("bronze_transactions_raw").show(3, truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
