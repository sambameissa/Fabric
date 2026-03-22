# Microsoft Fabric — Projets Data

> **Meissa Ndione SAMBA** · Data & BI Engineer
> [LinkedIn](https://linkedin.com/in/sambameissa) · [GitHub](https://github.com/sambameissa)

Repo dédié à des projets end-to-end sur **Microsoft Fabric** : architecture Medallion, pipelines PySpark, Data Warehouse, gouvernance et reporting Power BI. Chaque projet est construit sur des données fictives et documenté pour être réutilisable comme template.

---

## 📁 Projets

###  [banking/](./banking) — BankingDataPlatform
> Architecture Medallion complète sur données bancaires fictives

**Stack** : `Microsoft Fabric` `PySpark` `Delta Lake` `SQL Warehouse` `Power BI`

**Ce que couvre ce projet :**
- Pipeline Medallion Bronze → Silver → Gold
- Lakehouse unique (`LH_Banking`) pour les couches Bronze et Silver
- Data Warehouse (`WH_Banking`) pour la couche Gold
- Détection de transactions suspectes (scoring multi-règles)
- Rapport Power BI 4 pages (Synthèse · Clients 360° · Transactions · Fraude)

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
  Power BI Report (4 pages)
  ├── Synthèse exécutive
  ├── Vue 360° Clients
  ├── Analyse Transactions
  └── Risque & Fraude
```

**Données fictives :**
- 500 clients (segments : Premium · Standard · Basic · Private Banking)
- 11 306 transactions (CHF/EUR/USD · canaux Mobile/Web/Agence/ATM)
- Villes : Lausanne · Genève · Zurich · Berne · Bâle · Sion · Fribourg · Neuchâtel

---

## 🗺️ Roadmap — Projets à venir

| Projet | Secteur | Focus |
|---|---|---|
| `telecom/` | Télécom | Détection de fraude temps réel · Eventstream · KQL |
| `hospital/` | Santé | Data Quality framework · codes CIM-10 · séjours patients |
| `governance/` | Transversal | Accelerator de déploiement · création automatique d'artefacts via API Fabric |

---

## 🛠️ Stack commune

| Couche | Technologie |
|---|---|
| Plateforme | Microsoft Fabric (Lakehouse · Warehouse · Notebooks · Pipelines) |
| Traitement | PySpark · Python · Delta Lake |
| Analytique | SQL · DAX · Power BI |
| Gouvernance | Naming conventions · Data Quality · Audit log Delta |
| CI/CD | Git integration Fabric → GitHub |

---

## 📐 Conventions de nommage

```
Lakehouses  : LH_{Projet}
Warehouses  : WH_{Projet}
Notebooks   : {Projet}_NB_{Couche}_{Etape:02d}_{Description}
Pipelines   : {Projet}_PL_{Description}
Rapports    : {Projet}_RPT_{Description}

Tables Bronze : bronze_{nom}
Tables Silver : silver_{nom}
Tables Gold   : dbo.{nom}  (dans le Warehouse)
```

---

## 🚀 Comment utiliser un projet

```bash
# 1. Cloner le repo
git clone https://github.com/sambameissa/Fabric

# 2. Dans ton workspace Fabric
#    → Importer les notebooks depuis le dossier du projet
#    → Attacher le Lakehouse par défaut
#    → Uploader les données fictives dans Files/raw/

# 3. Exécuter dans l'ordre
#    NB_01 → NB_02 → NB_03 → NB_04
```

> 📌 Tous les projets utilisent des **données 100% fictives** générées synthétiquement.  
> Aucune donnée réelle ou confidentielle n'est publiée dans ce repo.


