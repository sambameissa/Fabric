# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # ⚙️ Fabric Governance Accelerator
# ### Documentation de démonstration
# **Auteur** : Meissa Ndione SAMBA | DP-700 | PMP® | MCT  
# **Workspace** : Governance_Workspace  
# **Repo** : [github.com/sambameissa/Fabric/governance](https://github.com/sambameissa/Fabric)
# 
# ---
# 
# > Ce notebook est le **guide de démonstration** du projet Governance.  
# > Il ne contient aucun code à exécuter — c'est le README du workspace.


# MARKDOWN ********************

# ## 🎯 Objectif du projet
# 
# Ce projet démontre la mise en œuvre d'une **plateforme data gouvernée** sur Microsoft Fabric, end-to-end :
# 
# - **Ingestion** de données brutes (Bronze)
# - **Transformation** et nettoyage (Silver)
# - **Agrégation** et KPIs métier (Gold)
# - **Contrôles qualité** automatisés avec scoring
# - **Documentation automatique** : dictionnaire, lineage, catalogue
# - **Reporting** Power BI connecté au Data Warehouse
# 
# Le tout sur un dataset fictif générique (actifs data, pipelines, contrôles DQ) — reproductible à l'infini grâce au script de reset.


# MARKDOWN ********************

# ## 🏛️ Architecture
# 
# ```
# Fichiers CSV (data/sample/)
#         │
#         ▼
# ┌─────────────────────────────────────────────────────────────┐
# │                    Governance_Workspace                      │
# │                                                             │
# │  GOV_LH_Bronze          GOV_LH_Silver          GOV_LH_Gold  │
# │  ─────────────          ─────────────          ───────────  │
# │  bronze_data_assets  →  silver_data_assets  →  gold_data_catalog        │
# │  bronze_pipeline_runs→  silver_pipeline_runs→  gold_pipeline_kpis       │
# │  bronze_dq_checks    →  silver_dq_checks    →  gold_dq_dashboard        │
# │                                                gold_dq_report           │
# │                                                gold_data_dictionary      │
# │                                                gold_data_lineage         │
# │                                                gold_catalog_consolidated │
# │                                                             │
# │                              GOV_DW_Gold (Warehouse SQL)    │
# │                              ─────────────────────────────  │
# │                              dbo.data_catalog               │
# │                              dbo.pipeline_kpis              │
# │                              dbo.dq_dashboard               │
# │                              dbo.catalog_consolidated        │
# │                                                             │
# │                              GOV_RPT_Monitoring (Power BI)  │
# └─────────────────────────────────────────────────────────────┘
# ```


# MARKDOWN ********************

# ## 📦 Artefacts du workspace
# 
# | Artefact | Type | Rôle |
# |---|---|---|
# | `GOV_LH_Bronze` | Lakehouse | Données brutes — fidélité source totale |
# | `GOV_LH_Silver` | Lakehouse | Données nettoyées, typées, hashées |
# | `GOV_LH_Gold` | Lakehouse | Agrégats métier + dictionnaire + lineage |
# | `GOV_DW_Gold` | Warehouse | Couche analytique SQL — connectée Power BI |
# | `GOV_NB_00_Accelerator` | Notebook | Création automatique de tous les artefacts |
# | `GOV_NB_01_Bronze_Ingestion` | Notebook | CSV → bronze_* |
# | `GOV_NB_02_Silver_Transformation` | Notebook | bronze_* → silver_* |
# | `GOV_NB_03_Gold_Aggregation` | Notebook | silver_* → gold_* |
# | `GOV_NB_04_DataQuality` | Notebook | Contrôles DQ + score /100 |
# | `GOV_NB_05_Documentation` | Notebook | Dictionnaire + lineage + catalogue auto |
# | `GOV_NB_RESET` | Notebook | Remise à zéro pour démo |
# | `GOV_NB_DEMO` | Notebook | Pipeline complet en 1 exécution |
# | `GOV_NB_DOC` | Notebook | **Ce fichier** — guide de démo |
# | `GOV_PL_Orchestration` | Pipeline | Orchestration NB_01 → 05 |


# MARKDOWN ********************

# ## 📊 Données fictives
# 
# 3 tables génériques — multi-secteurs (Finance · Telecom · Health · Retail · Public)
# 
# | Fichier | Lignes | Description |
# |---|---|---|
# | `data_assets.csv` | 200 | Catalogue d'actifs data (tables, rapports, pipelines...) |
# | `pipeline_runs.csv` | 500 | Historique d'exécutions de pipelines |
# | `dq_checks.csv` | 1 000 | Résultats de contrôles qualité |
# 
# **Emplacement dans Fabric** :
# ```
# GOV_LH_Bronze → Files → raw → data_assets/
#                                pipeline_runs/
#                                dq_checks/
# ```


# MARKDOWN ********************

# ## 🎬 Script de démonstration
# 
# ### Avant la démo (~2 min)
# 
# ```
# 1. GOV_NB_RESET → Run All
#    → Toutes les tables Delta supprimées
# 
# 2. GOV_DW_Gold → SQL Editor → exécuter :
#    TRUNCATE TABLE dbo.data_catalog;
#    TRUNCATE TABLE dbo.pipeline_kpis;
#    TRUNCATE TABLE dbo.dq_dashboard;
#    TRUNCATE TABLE dbo.catalog_consolidated;
# ```
# 
# ### Pendant la démo (~5 min)
# 
# **Étape 1 — Montrer l'architecture** *(ce notebook, section Architecture)*
# 
# **Étape 2 — Lancer le pipeline complet**
# ```
# GOV_NB_DEMO → Run All
# ```
# Pendant l'exécution, commenter chaque étape :
# - *"Étape 1 : on ingère les fichiers bruts — aucune transformation"*
# - *"Étape 2 : on nettoie, on type, on hash pour le lineage"*
# - *"Étape 3 : on construit les KPIs métier — Gold prêt pour Power BI"*
# - *"Étape 4 : contrôles qualité automatiques — score sur 100"*
# - *"Étape 5 : dictionnaire et lineage générés sans intervention manuelle"*
# 
# **Étape 3 — Charger le Warehouse**
# ```
# GOV_DW_Gold → SQL Editor → GOV_DW_Gold_load.sql
# ```
# *"Les données Gold sont maintenant dans le Warehouse SQL — Power BI peut s'y connecter"*
# 
# **Étape 4 — Ouvrir Power BI**
# ```
# GOV_RPT_Monitoring → Actualiser → Montrer les 4 pages
# ```
# 
# ### Questions fréquentes en démo
# 
# **Q : Combien de temps pour mettre ça en place ?**  
# R : Ce workspace a été déployé en moins d'une heure, Lakehouse et Warehouse compris.
# 
# **Q : Est-ce que ça marche avec nos propres données ?**  
# R : Oui — il suffit de remplacer les CSV fictifs par vos sources réelles (ERP, API, bases de données) et d'adapter les transformations Silver.
# 
# **Q : Et si on ajoute une nouvelle source ?**  
# R : On ajoute un `ingest_bronze()` dans NB_01, un `transform_silver()` dans NB_02, et on l'intègre dans les agrégats Gold. Le dictionnaire et le lineage se mettent à jour automatiquement au prochain run de NB_05.


# MARKDOWN ********************

# ## 📐 Conventions de nommage
# 
# ```
# Lakehouses   : {PROJET}_LH_{Couche}     → GOV_LH_Bronze
# Warehouses   : {PROJET}_DW_{Couche}     → GOV_DW_Gold
# Notebooks    : {PROJET}_NB_{N}_{Role}   → GOV_NB_03_Gold_Aggregation
# Pipelines    : {PROJET}_PL_{Role}       → GOV_PL_Orchestration
# Rapports     : {PROJET}_RPT_{Role}      → GOV_RPT_Monitoring
# 
# Tables Bronze : bronze_{nom}            → bronze_data_assets
# Tables Silver : silver_{nom}            → silver_data_assets
# Tables Gold   : gold_{nom}              → gold_data_catalog
# Tables DW     : dbo.{nom}               → dbo.data_catalog
# ```
# 
# **Colonnes techniques**
# 
# | Colonne | Couche | Description |
# |---|---|---|
# | `_bronze_ts` | Bronze | Timestamp d'ingestion |
# | `_source_system` | Bronze | Système source |
# | `_source_file` | Bronze | Fichier source |
# | `_silver_hash` | Silver | Hash SHA256 des clés métier |
# | `_silver_ts` | Silver | Timestamp de transformation |
# | `_gold_ts` | Gold | Timestamp de construction |


# MARKDOWN ********************

# ## 💡 Bonnes pratiques Fabric — leçons apprises
# 
# | Problème | Solution retenue |
# |---|---|
# | `saveAsTable` écrit dans le LH par défaut | Noms qualifiés `LH_Gold.gold_table` ou `setCurrentDatabase()` |
# | Warehouse inaccessible depuis PySpark | Écrire dans LH_Gold, puis CTAS depuis SQL Editor |
# | HTTP 202 traité comme erreur API | 202 = succès asynchrone (normal pour Warehouse) |
# | Session indépendante par notebook | Toujours relire les tables en début de notebook |
# | Schema drift LH → DW | `DROP + SELECT * INTO` plutôt que `CREATE + INSERT` |
# | `CANNOT_INFER_EMPTY_SCHEMA` | Toujours définir un schéma explicite `StructType` |
# | Tables non trouvées cross-LH | Attacher les LH au notebook + `setCurrentDatabase()` |


# MARKDOWN ********************

# ## 🔗 Ressources
# 
# **Repo GitHub** : [github.com/sambameissa/Fabric](https://github.com/sambameissa/Fabric)
# 
# ```
# Fabric/
# ├── banking/          ← Projet Banking (Medallion + Power BI)
# └── governance/       ← Ce projet
#     ├── notebooks/    ← Tous les notebooks versionnés
#     ├── config/       ← governance_config.yaml
#     ├── data/sample/  ← Données fictives CSV
#     └── README.md
# ```
# 
# **Auteur** : Meissa Ndione SAMBA  
# DP-700 · DP-600 · PL-300 · PMP® · MCT  
# [linkedin.com/in/sambameissa](https://linkedin.com/in/sambameissa)

