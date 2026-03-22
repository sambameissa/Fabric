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
# META           "id": "18f4c638-3287-4a68-a95b-093847255e60"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "c8382c14-825d-4844-979d-e1fa773049ec",
# META       "known_warehouses": [
# META         {
# META           "id": "c8382c14-825d-4844-979d-e1fa773049ec",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# GOV_NB_RESET — Reset complet pour démonstration
# ============================================================
# Auteur : Meissa Ndione SAMBA | DP-700
# Action : supprime toutes les tables Delta des 3 Lakehouses
#          pour repartir d un état vierge
# ============================================================

LH_BRONZE = "GOV_LH_Bronze"
LH_SILVER = "GOV_LH_Silver"
LH_GOLD   = "GOV_LH_Gold"

# ── Tables à supprimer par Lakehouse ─────────────────────────
TABLES = {
    LH_BRONZE: [
        "bronze_data_assets",
        "bronze_pipeline_runs",
        "bronze_dq_checks",
        "bronze_deployment_audit",
    ],
    LH_SILVER: [
        "silver_data_assets",
        "silver_pipeline_runs",
        "silver_dq_checks",
        "silver_audit_log",
    ],
    LH_GOLD: [
        "gold_data_catalog",
        "gold_pipeline_kpis",
        "gold_dq_dashboard",
        "gold_catalog_consolidated",
        "gold_data_dictionary",
        "gold_data_lineage",
        "gold_dq_report",
    ],
}

CONFIRM = True  # Passer à False pour simuler sans supprimer

print("=" * 60)
print("  GOV RESET — Nettoyage complet")
print(f"  Mode : {'RÉEL' if CONFIRM else 'SIMULATION'}")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Suppression des tables Delta ────────────────────────────
results = {"dropped": [], "not_found": [], "error": []}

for lakehouse, tables in TABLES.items():
    print(f"\n  {lakehouse}")
    print(f"  {'-'*40}")
    for table in tables:
        full_name = f"{lakehouse}.{table}"
        try:
            # Vérifier si la table existe
            spark.read.table(full_name).limit(1).count()
            if CONFIRM:
                spark.sql(f"DROP TABLE IF EXISTS {full_name}")
                print(f"    DROPPED  {table}")
                results["dropped"].append(full_name)
            else:
                print(f"    [SIM]    {table} — serait supprimée")
                results["dropped"].append(full_name)
        except Exception:
            print(f"    SKIP     {table} — non trouvée")
            results["not_found"].append(full_name)

print("\n" + "=" * 60)
print(f"  RESET TERMINÉ")
print(f"  Supprimées  : {len(results['dropped'])}")
print(f"  Non trouvées: {len(results['not_found'])}")
print("=" * 60)
print("\n  Prochaine étape :")
print("  Exécuter GOV_NB_DEMO pour relancer la démo complète")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Reset du Warehouse via SQL ───────────────────────────────
# À exécuter manuellement dans GOV_DW_Gold → SQL Editor
# après avoir lancé ce notebook

sql_reset = """
-- GOV_DW_Gold — SQL Editor
-- Coller et exécuter pour vider le Warehouse

TRUNCATE TABLE dbo.data_catalog;
TRUNCATE TABLE dbo.pipeline_kpis;
TRUNCATE TABLE dbo.dq_dashboard;
TRUNCATE TABLE dbo.catalog_consolidated;

-- Vérification
SELECT 'data_catalog'        AS table_name, COUNT(*) AS nb_rows FROM dbo.data_catalog
UNION ALL
SELECT 'pipeline_kpis',                     COUNT(*) FROM dbo.pipeline_kpis
UNION ALL
SELECT 'dq_dashboard',                      COUNT(*) FROM dbo.dq_dashboard
UNION ALL
SELECT 'catalog_consolidated',              COUNT(*) FROM dbo.catalog_consolidated;
"""

print("Script SQL à exécuter dans GOV_DW_Gold → SQL Editor :")
print("-" * 60)
print(sql_reset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
