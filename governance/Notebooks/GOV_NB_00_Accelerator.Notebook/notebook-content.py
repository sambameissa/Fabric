# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# GOV_NB_00_Accelerator — Fabric Governance Accelerator
# ============================================================
# Auteur  : Meissa Ndione SAMBA | DP-700 | PMP® | MCT
# Repo    : github.com/sambameissa/Fabric/governance
# Workspace : Governance_Workspace
#
# Ce notebook crée automatiquement TOUS les artefacts
# du projet Governance en une seule exécution.
# ============================================================

# ── CELL 1 : Paramètres ──────────────────────────────────────
PROJECT      = "GOV"
ENVIRONMENT  = "dev"
WORKSPACE_ID = "6a73c087-0a74-4700-9367-4cdb58e9eee4"       # Renseigner après création du workspace
DRY_RUN      = False     # True = simulation | False = déploiement réel

print("=" * 60)
print(f"  FABRIC GOVERNANCE ACCELERATOR")
print(f"  Project     : {PROJECT}")
print(f"  Environment : {ENVIRONMENT}")
print(f"  Workspace   : {WORKSPACE_ID or 'NON CONFIGURÉ'}")
print(f"  Mode        : {'SIMULATION' if DRY_RUN else 'DÉPLOIEMENT RÉEL'}")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CELL 2 : Plan de déploiement ────────────────────────────
ARTIFACTS = {
    "lakehouses": [
        {"name": "GOV_LH_Bronze", "desc": "Données brutes — fidélité source"},
        {"name": "GOV_LH_Silver", "desc": "Données nettoyées et normalisées"},
        {"name": "GOV_LH_Gold",   "desc": "Agrégats métier — prêt Power BI"},
    ],
    "warehouse": {
        "name": "GOV_DW_Gold",
        "desc": "Data Warehouse analytique"
    },
    "notebooks": [
        {"name": "GOV_NB_01_Bronze_Ingestion",       "desc": "Ingestion brute"},
        {"name": "GOV_NB_02_Silver_Transformation",  "desc": "Nettoyage & normalisation"},
        {"name": "GOV_NB_03_Gold_Aggregation",       "desc": "Agrégats & KPIs"},
        {"name": "GOV_NB_04_DataQuality",            "desc": "Contrôles qualité"},
        {"name": "GOV_NB_05_Documentation",          "desc": "Dictionnaire & lineage"},
    ],
    "pipeline": {"name": "GOV_PL_Orchestration", "desc": "Pipeline d'orchestration complet"},
}

print("\nDEPLOYMENT PLAN")
print("-" * 60)
print("\n  LAKEHOUSES")
for lh in ARTIFACTS["lakehouses"]:
    print(f"    -> {lh['name']:30} {lh['desc']}")
print(f"\n  WAREHOUSE")
print(f"    -> {ARTIFACTS['warehouse']['name']:30} {ARTIFACTS['warehouse']['desc']}")
print(f"\n  NOTEBOOKS ({len(ARTIFACTS['notebooks'])})")
for nb in ARTIFACTS["notebooks"]:
    print(f"    -> {nb['name']:40} {nb['desc']}")
print(f"\n  PIPELINE")
print(f"    -> {ARTIFACTS['pipeline']['name']:30} {ARTIFACTS['pipeline']['desc']}")
print("\n" + "-" * 60)
print(f"  Total : {3 + 1 + len(ARTIFACTS['notebooks']) + 1} artefacts à créer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CELL 3 : Déploiement ────────────────────────────────────
from datetime import datetime
results = {"created": [], "skipped": [], "failed": []}
t_start = datetime.now()

def deploy_artifact(name, artifact_type, desc=""):
    """Simule ou déploie un artefact Fabric via API REST."""
    if DRY_RUN:
        print(f"  [SIM]  {artifact_type:12} {name}")
        results["created"].append({"name": name, "type": artifact_type})
        return True

    # ── Mode déploiement réel ────────────────────────────────
    try:
        from notebookutils import mssparkutils
        token = mssparkutils.credentials.getToken(
            "https://api.fabric.microsoft.com"
        )
        import requests
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "displayName": name,
            "description": desc,
            "type": artifact_type
        }
        resp = requests.post(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items",
            headers=headers,
            json=payload,
            timeout=30
        )
        if resp.status_code in [200, 201]:
            print(f"  [OK]   {artifact_type:12} {name}")
            results["created"].append({"name": name, "type": artifact_type,
                                        "id": resp.json().get("id")})
            return True
        elif resp.status_code == 409:
            print(f"  [SKIP] {artifact_type:12} {name} — déjà existant")
            results["skipped"].append({"name": name, "type": artifact_type})
            return True
        else:
            raise Exception(f"HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"  [ERR]  {artifact_type:12} {name} — {e}")
        results["failed"].append({"name": name, "type": artifact_type, "error": str(e)})
        return False

print(f"\n{'SIMULATION' if DRY_RUN else 'DÉPLOIEMENT'} EN COURS...\n")

# Lakehouses
for lh in ARTIFACTS["lakehouses"]:
    deploy_artifact(lh["name"], "Lakehouse", lh["desc"])

# Warehouse
deploy_artifact(ARTIFACTS["warehouse"]["name"], "Warehouse",
                ARTIFACTS["warehouse"]["desc"])

# Notebooks
for nb in ARTIFACTS["notebooks"]:
    deploy_artifact(nb["name"], "Notebook", nb["desc"])

# Pipeline
deploy_artifact(ARTIFACTS["pipeline"]["name"], "DataPipeline",
                ARTIFACTS["pipeline"]["desc"])

# Bilan
duration = (datetime.now() - t_start).total_seconds()
print(f"\n{'=' * 60}")
print(f"  DEPLOYMENT REPORT — {PROJECT}")
print(f"  Duration    : {duration:.1f}s")
print(f"  Created     : {len(results['created'])}")
print(f"  Skipped     : {len(results['skipped'])}")
print(f"  Failed      : {len(results['failed'])}")
print(f"  Success rate: {round(100*len(results['created'])/(len(results['created'])+len(results['failed'])+0.001),1)}%")
print(f"{'=' * 60}")
if results["failed"]:
    print("\n  FAILURES:")
    for f in results["failed"]:
        print(f"    {f['name']} — {f['error']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CELL 4 : Audit log ──────────────────────────────────────
# Sauvegarde le résultat du déploiement dans une table Delta
# (uniquement si GOV_LH_Bronze est déjà créé et attaché)
import json
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

try:
    rows = [Row(
        project     = PROJECT,
        environment = ENVIRONMENT,
        artifact    = r["name"],
        type        = r["type"],
        status      = "CREATED",
        deployed_ts = str(current_timestamp())
    ) for r in results["created"]]

    if rows:
        df_audit = spark.createDataFrame(rows)
        (df_audit.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable("bronze_deployment_audit")
        )
        print(f"Audit log saved -> bronze_deployment_audit ({len(rows)} rows)")
except Exception as e:
    print(f"Audit log skipped (normal en DRY_RUN ou avant création du LH) : {e}")

print("\nProchaines étapes :")
print("  1. Passer DRY_RUN = False et renseigner WORKSPACE_ID")
print("  2. Ré-exécuter pour le déploiement réel")
print("  3. Attacher GOV_LH_Bronze aux notebooks générés")
print("  4. Exécuter NB_01 → NB_02 → NB_03 → NB_04 → NB_05")
print("  5. Connecter GOV_RPT_Monitoring à GOV_DW_Gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
