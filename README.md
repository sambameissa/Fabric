# Microsoft Fabric — Projets Data

> **Meissa Ndione SAMBA** · Data Engineer & BI Solutions Architect · DP-700 · PMP® · MCT  
> [LinkedIn](https://linkedin.com/in/sambameissa) · [GitHub](https://github.com/sambameissa)

Repo dédié à des projets end-to-end sur **Microsoft Fabric** : architecture Medallion, pipelines PySpark, Data Warehouse, gouvernance et reporting Power BI. Chaque projet est construit sur des données fictives et documenté pour être réutilisable comme template.

---

## 📁 Projets

### 🏦 [banking/](./banking) — BankingDataPlatform
> Architecture Medallion complète sur données bancaires fictives

**Stack** : `Microsoft Fabric` `PySpark` `Delta Lake` `SQL Warehouse` `Power BI`

**Ce que couvre ce projet :**
- Pipeline Medallion Bronze → Silver → Gold
- Lakehouse unique (`LH_Banking`) pour les couches Bronze et Silver
- Data Warehouse (`WH_Banking`) pour la couche Gold
- Détection de transactions suspectes (scoring multi-règles)
- Rapport Power BI 4 pages *(en cours)*

**Artefacts Fabric :**

| Artefact | Type | Rôle |
|---|---|---|
| `LH_Banking` | Lakehouse | Stockage Bronze & Silver (tables Delta) |
| `WH_Banking` | Warehouse | Agrégats Gold (SQL analytique) |
| `NB_Bronze_01_Ingestion` | Notebook | Ingestion CSV → bronze_* |
| `NB_Silver_02_Transformation` | Notebook | Nettoyage → silver_* |
| `NB_Gold_03_Aggregation` | Notebook | KPIs → WH_Banking.dbo.* |
| `NB_Gold_04_DataQuality` | Notebook | Contrôles qualité Gold |

**Architecture :**

```
Files/raw/
  ├── clients/          (500 clients fictifs — Suisse)
  └── transactions/     (11 306 transactions CHF)
          │
          ▼
  LH_Banking
  ├── bronze_clients_raw
  ├── bronze_transactions_raw
  ├── silver_clients_clean
  └── silver_transactions_clean
          │
          ▼
  WH_Banking
  ├── dbo.client_kpis              ← Vue 360° client
  ├── dbo.monthly_kpis             ← Agrégats mensuels
  └── dbo.suspicious_transactions  ← Alertes fraude
          │
          ▼
  Power BI Report (4 pages) ← en cours
```

---

### ⚙️ [governance/](./governance) — GOV Governance Accelerator
> Déploiement automatisé et gouverné d'une plateforme data Microsoft Fabric

**Stack** : `Microsoft Fabric` `PySpark` `Delta Lake` `SQL Warehouse` `Fabric REST API` `Stored Procedures`

**Ce que couvre ce projet :**
- Déploiement automatique de tous les artefacts Fabric via API REST (NB_00)
- Pipeline Medallion Bronze → Silver → Gold avec naming conventions strictes
- Framework Data Quality automatisé avec DQ score /100
- Génération automatique du dictionnaire de données, lineage et catalogue
- Pipeline d'orchestration complet avec procédures stockées DW
- Script de reset pour démonstrations reproductibles

**Artefacts Fabric (13) :**

| Artefact | Type | Rôle |
|---|---|---|
| `GOV_LH_Bronze` | Lakehouse | Données brutes — fidélité source |
| `GOV_LH_Silver` | Lakehouse | Données nettoyées, typées, hashées |
| `GOV_LH_Gold` | Lakehouse | Agrégats + dictionnaire + lineage |
| `GOV_DW_Gold` | Warehouse | Couche analytique SQL |
| `GOV_NB_00_Accelerator` | Notebook | Déploiement automatique via API REST |
| `GOV_NB_01_Bronze_Ingestion` | Notebook | CSV → bronze_* |
| `GOV_NB_02_Silver_Transformation` | Notebook | bronze_* → silver_* |
| `GOV_NB_03_Gold_Aggregation` | Notebook | silver_* → gold_* |
| `GOV_NB_04_DataQuality` | Notebook | Contrôles DQ + score /100 |
| `GOV_NB_05_Documentation` | Notebook | Dictionnaire + lineage + catalogue auto |
| `GOV_NB_RESET` | Notebook | Remise à zéro complète pour démo |
| `GOV_NB_DOC` | Notebook | Guide de démonstration Markdown |
| `GOV_PL_Orchestration` | Pipeline | Orchestration complète Reset → Load |

**Architecture :**

```
Files/raw/
  ├── data_assets/      (200 actifs data fictifs — multi-secteurs)
  ├── pipeline_runs/    (500 exécutions de pipelines)
  └── dq_checks/        (1 000 contrôles qualité)
          │
          ▼
  GOV_LH_Bronze         GOV_LH_Silver         GOV_LH_Gold
  ─────────────    →    ─────────────    →    ───────────
  bronze_*              silver_*              gold_data_catalog
                                              gold_pipeline_kpis
                                              gold_dq_dashboard
                                              gold_data_dictionary  ← auto
                                              gold_data_lineage      ← auto
                                              gold_dq_report
                                              gold_catalog_consolidated
          │
          ▼
  GOV_DW_Gold (via usp_Load_FromGold)
  ├── dbo.data_catalog
  ├── dbo.pipeline_kpis
  ├── dbo.dq_dashboard
  └── dbo.catalog_consolidated
          │
          ▼
  GOV_RPT_Monitoring (Power BI) ← à venir
```

**Pipeline GOV_PL_Orchestration :**

```
[SQL] usp_Reset_AllTables
    → [NB] GOV_NB_RESET
    → [NB] GOV_NB_01_Bronze_Ingestion
    → [NB] GOV_NB_02_Silver_Transformation
    → [NB] GOV_NB_03_Gold_Aggregation
    → [NB] GOV_NB_04_DataQuality
    → [NB] GOV_NB_05_Documentation
    → [SQL] usp_Load_FromGold
```

**Procédures stockées :**
- `usp_Reset_AllTables` — vide les 4 tables du DW
- `usp_Load_FromGold` — recharge via `DROP + SELECT * INTO` (anti-schema drift)

---

## 🗺️ Roadmap — Projets à venir

| Projet | Secteur | Focus |
|---|---|---|
| `telecom/` | Télécom | Détection de fraude · Eventstream · KQL Real-Time Intelligence |
| `hospital/` | Santé | Data Quality framework · codes CIM-10 · séjours patients |

---

## 🛠️ Stack commune

| Couche | Technologie |
|---|---|
| Plateforme | Microsoft Fabric (Lakehouse · Warehouse · Notebooks · Pipelines) |
| Traitement | PySpark · Python · Delta Lake |
| Analytique | SQL · DAX · Power BI |
| Gouvernance | Naming conventions · Data Quality · Audit log Delta · Lineage auto |
| CI/CD | Git integration Fabric → GitHub |

---

## 📐 Conventions de nommage

```
Lakehouses  : {PROJET}_LH_{Couche}               → GOV_LH_Bronze
Warehouses  : {PROJET}_DW_{Couche}               → GOV_DW_Gold
Notebooks   : {PROJET}_NB_{N:02d}_{Description}  → GOV_NB_03_Gold_Aggregation
Pipelines   : {PROJET}_PL_{Description}          → GOV_PL_Orchestration
Rapports    : {PROJET}_RPT_{Description}         → GOV_RPT_Monitoring

Tables Bronze : bronze_{nom}     → bronze_data_assets
Tables Silver : silver_{nom}     → silver_data_assets
Tables Gold   : gold_{nom}       → gold_data_catalog
Tables DW     : dbo.{nom}        → dbo.data_catalog
```

---

## 💡 Bonnes pratiques Fabric consolidées

| Problème | Solution |
|---|---|
| `saveAsTable` écrit dans le LH par défaut | `spark.catalog.setCurrentDatabase()` + nom qualifié |
| Warehouse inaccessible depuis PySpark | Écrire dans LH_Gold → `SELECT * INTO` via procédure stockée |
| HTTP 202 traité comme erreur API | 202 = succès asynchrone (normal pour Warehouse) |
| Session indépendante par notebook | Relire les tables en début de chaque notebook |
| Schema drift LH → DW | `DROP + SELECT * INTO` systématique |
| `CANNOT_INFER_EMPTY_SCHEMA` | Schéma explicite `StructType` obligatoire |
| Cross-LH sans Lakehouse par défaut | Attacher les LH + `setCurrentDatabase()` |

---

## 🚀 Comment utiliser un projet

```bash
# 1. Cloner le repo
git clone https://github.com/sambameissa/Fabric

# 2. Dans ton workspace Fabric
#    → Connecter le workspace au repo via Git integration
#    → Les notebooks apparaissent automatiquement

# 3. Uploader les données fictives dans Files/raw/ du Lakehouse Bronze
# 4. Exécuter le pipeline d'orchestration
```

> 📌 Tous les projets utilisent des **données 100% fictives** générées synthétiquement.  
> Aucune donnée réelle ou confidentielle n'est publiée dans ce repo.

---

**Meissa Ndione SAMBA**  
Data Engineer & BI Solutions Architect · DP-700 · DP-600 · PL-300 · PMP® · MCT  
[sambameissa@hotmail.com](mailto:sambameissa@hotmail.com) · [LinkedIn](https://linkedin.com/in/sambameissa)
