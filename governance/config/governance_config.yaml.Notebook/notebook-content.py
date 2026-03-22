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
# Fabric Governance Accelerator — Configuration
# ============================================================
# Auteur : Meissa Ndione SAMBA | DP-700 | PMP®
# Workspace : Governance_Workspace (dédié Governance)
# ============================================================

project:
  name: "GOV"
  description: "Fabric Governance Accelerator — template générique multi-secteurs"
  workspace: "Governance_Workspace"
  environment: "dev"

artifacts:
  lakehouses:
    - name: "GOV_LH_Bronze"
      layer: "Bronze"
      description: "Données brutes — fidélité source totale"
    - name: "GOV_LH_Silver"
      layer: "Silver"
      description: "Données nettoyées, typées, normalisées"
    - name: "GOV_LH_Gold"
      layer: "Gold"
      description: "Agrégats métier prêts pour Power BI"
  warehouse:
    name: "GOV_DW_Gold"
    description: "Data Warehouse analytique — couche Gold SQL"
  notebooks:
    - id: "NB_00"
      name: "GOV_NB_00_Accelerator"
      role: "Notebook principal — crée tous les artefacts"
    - id: "NB_01"
      name: "GOV_NB_01_Bronze_Ingestion"
      role: "Ingestion brute vers GOV_LH_Bronze"
    - id: "NB_02"
      name: "GOV_NB_02_Silver_Transformation"
      role: "Nettoyage et normalisation vers GOV_LH_Silver"
    - id: "NB_03"
      name: "GOV_NB_03_Gold_Aggregation"
      role: "Agrégats KPIs vers GOV_DW_Gold"
    - id: "NB_04"
      name: "GOV_NB_04_DataQuality"
      role: "Contrôles qualité — rapport DQ score"
    - id: "NB_05"
      name: "GOV_NB_05_Documentation"
      role: "Génération automatique dictionnaire, lineage, catalogue"
  pipeline:
    name: "GOV_PL_Orchestration"
    schedule: "0 6 * * *"  # 06h00 chaque jour
  report:
    name: "GOV_RPT_Monitoring"
    pages:
      - "Tableau de bord Governance"
      - "Qualité des données"
      - "Historique pipelines"
      - "Catalogue des actifs"

naming_conventions:
  prefix: "GOV"
  layers:
    bronze: "LH_Bronze"
    silver: "LH_Silver"
    gold_lh: "LH_Gold"
    gold_dw: "DW_Gold"
  tables:
    bronze: "bronze_{name}"
    silver: "silver_{name}"
    gold: "gold_{name}"
    warehouse: "dbo.{name}"

governance_rules:
  data_classification:
    - PUBLIC
    - INTERNAL
    - CONFIDENTIAL
    - RESTRICTED
  quality_thresholds:
    max_null_pct: 5.0
    max_duplicate_pct: 0.1
    min_dq_score: 0.85
  audit_table: "silver_audit_log"
  lineage_table: "silver_data_lineage"
  catalog_table: "gold_data_catalog"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
