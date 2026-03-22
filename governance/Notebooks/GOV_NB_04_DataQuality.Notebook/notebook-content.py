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
# META           "id": "18f4c638-3287-4a68-a95b-093847255e60"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# GOV_NB_04_DataQuality — Governance Workspace
# Auteur : Meissa Ndione SAMBA | DP-700
# Source : GOV_LH_Silver.silver_*
# Cible  : GOV_LH_Gold.gold_dq_report
# Noms qualifiés — indépendant du Lakehouse par défaut

from pyspark.sql.functions import (col, count, when, stddev, mean,
    abs as spark_abs, current_timestamp, lit)
from dataclasses import dataclass
from typing import List, Any
import logging
logger = logging.getLogger("GOV_DQ")

# ── Lakehouses ───────────────────────────────────────────────
LH_SILVER = "GOV_LH_Silver"
LH_GOLD   = "GOV_LH_Gold"

# ── Relecture Silver ─────────────────────────────────────────
df_a = spark.read.table(f"{LH_SILVER}.silver_data_assets")
df_r = spark.read.table(f"{LH_SILVER}.silver_pipeline_runs")
df_d = spark.read.table(f"{LH_SILVER}.silver_dq_checks")

print(f"Silver loaded: {df_a.count()} assets | {df_r.count()} runs | {df_d.count()} checks")

# ── Framework DQ ─────────────────────────────────────────────
@dataclass
class DQResult:
    table: str; rule: str; column: str
    status: str; value: Any; threshold: Any; details: str

class GOVDQRunner:
    def __init__(self):
        self.results: List[DQResult] = []
        self.score = 100.0

    def nulls(self, df, table, cols, max_pct=5.0):
        n = df.count()
        for c in cols:
            null_n = df.filter(col(c).isNull()).count()
            pct    = round(100.0 * null_n / n, 2)
            ok     = pct <= max_pct
            if not ok: self.score -= 8
            self.results.append(DQResult(table, "null_check", c,
                "PASSED" if ok else "FAILED", pct, max_pct,
                f"{null_n}/{n} nulls ({pct}%)"))

    def duplicates(self, df, table, keys):
        total = df.count(); dist = df.dropDuplicates(keys).count()
        dups  = total - dist
        ok    = dups == 0
        if not ok: self.score -= 15
        self.results.append(DQResult(table, "dup_check", str(keys),
            "PASSED" if ok else "FAILED", dups, 0, f"{dups} duplicates"))

    def range_check(self, df, table, col_name, min_v=None, max_v=None):
        out = df
        if min_v is not None: out = out.filter(col(col_name) < min_v)
        if max_v is not None: out = out.filter(col(col_name) > max_v)
        n  = out.count()
        ok = n == 0
        if not ok: self.score -= 5
        self.results.append(DQResult(table, "range_check", col_name,
            "PASSED" if ok else "WARNING", n, 0,
            f"{n} values out of [{min_v},{max_v}]"))

    def referential(self, df_child, df_parent, child_key, parent_key, label):
        orphans = df_child.join(
            df_parent, df_child[child_key] == df_parent[parent_key], "left_anti"
        ).count()
        ok = orphans == 0
        if not ok: self.score -= 10
        self.results.append(DQResult(label, "referential",
            f"{child_key}->{parent_key}",
            "PASSED" if ok else "FAILED", orphans, 0,
            f"{orphans} orphan records"))

    def report(self, spark_session):
        icons = {"PASSED":"OK  ","WARNING":"WARN","FAILED":"FAIL"}
        print("\n" + "="*65)
        print(f"  DATA QUALITY REPORT — Governance Workspace")
        print(f"  DQ Score global : {round(self.score,1)} / 100")
        print("="*65)
        for r in self.results:
            print(f"  {icons[r.status]}  {r.table:25} {r.rule:20} {r.details}")
        f = sum(1 for r in self.results if r.status=="FAILED")
        w = sum(1 for r in self.results if r.status=="WARNING")
        print(f"\n  Summary: {len(self.results)} checks | {f} FAILED | {w} WARNINGS | {len(self.results)-f-w} PASSED")
        print("="*65)

        # Sauvegarde dans GOV_LH_Gold
        from pyspark.sql import Row
        rows = [Row(
            table=r.table, rule=r.rule, column=r.column,
            status=r.status, details=r.details,
            dq_score=round(self.score, 1),
            check_ts=str(current_timestamp())
        ) for r in self.results]
        tgt = f"{LH_GOLD}.gold_dq_report"
        (spark_session.createDataFrame(rows)
            .write.format("delta")
            .mode("overwrite")
            .option("mergeSchema","true")
            .saveAsTable(tgt))
        print(f"\n  Rapport DQ sauvegardé -> {tgt}")

# ── Exécution ────────────────────────────────────────────────
dq = GOVDQRunner()

dq.nulls(df_a, "data_assets",   ["asset_id","asset_name","layer","status"])
dq.nulls(df_r, "pipeline_runs", ["run_id","pipeline_name","status"])
dq.nulls(df_d, "dq_checks",     ["check_id","asset_id","check_type"])
dq.duplicates(df_a, "data_assets",   ["asset_id"])
dq.duplicates(df_r, "pipeline_runs", ["run_id"])
dq.duplicates(df_d, "dq_checks",     ["check_id"])
dq.range_check(df_a, "data_assets",   "quality_score", min_v=0.0, max_v=1.0)
dq.range_check(df_r, "pipeline_runs", "duration_sec",  min_v=0)
dq.range_check(df_d, "dq_checks",     "dq_score",      min_v=0.0, max_v=1.0)
dq.referential(df_d, df_a, "asset_id", "asset_id", "dq_checks->data_assets")

dq.report(spark)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
