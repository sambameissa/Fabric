# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "90666a77-e276-47a0-b114-b8a8c5aa291e",
# META       "default_lakehouse_name": "RTL_LH_Silver",
# META       "default_lakehouse_workspace_id": "6063f389-ce67-4732-bafe-51945b2f2584",
# META       "known_lakehouses": [
# META         {
# META           "id": "90666a77-e276-47a0-b114-b8a8c5aa291e"
# META         },
# META         {
# META           "id": "acfa9a8d-9495-482c-bf7f-3ee0cda662b7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# RTL_NB_04_DataQuality — RetailIntelligence
# Auteur : Meissa Ndione SAMBA | DP-700
# Contrôles qualité retail : cohérence géo, prix, transactions

from pyspark.sql.functions import (col, count, when, avg, stddev,
    mean, abs as spark_abs, current_timestamp, lit)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dataclasses import dataclass
from typing import List, Any
import logging

LH_SILVER = "RTL_LH_Silver"
LH_GOLD   = "RTL_LH_Gold"

@dataclass
class DQResult:
    table: str; rule: str; column: str
    status: str; value: Any; details: str

class RetailDQRunner:
    def __init__(self):
        self.results: List[DQResult] = []
        self.score = 100.0

    def nulls(self, df, table, cols, max_pct=5.0):
        n = df.count()
        for c in cols:
            null_n = df.filter(col(c).isNull()).count()
            pct    = round(100.0*null_n/n, 2)
            ok     = pct <= max_pct
            if not ok: self.score -= 8
            self.results.append(DQResult(table,"null_check",c,
                "PASSED" if ok else "FAILED",pct,
                f"{null_n}/{n} nulls ({pct}%)"))

    def duplicates(self, df, table, keys):
        total = df.count(); dist = df.dropDuplicates(keys).count()
        dups  = total - dist
        ok    = dups == 0
        if not ok: self.score -= 15
        self.results.append(DQResult(table,"dup_check",str(keys),
            "PASSED" if ok else "FAILED",dups,f"{dups} duplicates"))

    def range_check(self, df, table, col_name, min_v=None, max_v=None):
        out = df
        if min_v is not None: out = out.filter(col(col_name) < min_v)
        if max_v is not None: out = out.filter(col(col_name) > max_v)
        n  = out.count()
        ok = n == 0
        if not ok: self.score -= 5
        self.results.append(DQResult(table,"range_check",col_name,
            "PASSED" if ok else "WARNING",n,
            f"{n} values out of [{min_v},{max_v}]"))

    def referential(self, df_child, df_parent, key, label):
        orphans = df_child.join(df_parent,on=key,how="left_anti").count()
        ok = orphans == 0
        if not ok: self.score -= 10
        self.results.append(DQResult(label,"referential",key,
            "PASSED" if ok else "FAILED",orphans,
            f"{orphans} orphan records"))

    def report(self, spark_session):
        icons = {"PASSED":"OK  ","WARNING":"WARN","FAILED":"FAIL"}
        print("\n" + "="*65)
        print(f"  RETAIL DATA QUALITY REPORT")
        print(f"  DQ Score : {round(self.score,1)} / 100")
        print("="*65)
        for r in self.results:
            print(f"  {icons[r.status]}  {r.table:25} {r.rule:20} {r.details}")
        f = sum(1 for r in self.results if r.status=="FAILED")
        w = sum(1 for r in self.results if r.status=="WARNING")
        print(f"\n  {len(self.results)} checks | {f} FAILED | {w} WARN | {len(self.results)-f-w} PASSED")
        # Save
        schema = StructType([
            StructField("table",  StringType(), True),
            StructField("rule",   StringType(), True),
            StructField("column", StringType(), True),
            StructField("status", StringType(), True),
            StructField("details",StringType(), True),
            StructField("dq_score",DoubleType(),True),
        ])
        rows = [(r.table,r.rule,r.column,r.status,r.details,
                 round(self.score,1)) for r in self.results]

        (spark_session.createDataFrame(rows,schema=schema)
            .write.format("delta").mode("overwrite")
            .saveAsTable("gold_dq_report"))
        print(f"  Sauvegardé -> {LH_GOLD}.gold_dq_report")

dq = RetailDQRunner()
df_m = spark.read.table("silver_magasins")
df_p = spark.read.table("silver_produits")
df_t = spark.read.table("silver_transactions")
df_z = spark.read.table("silver_zones_chalandise")

dq.nulls(df_m, "magasins",     ["store_id","ville","surface_m2"])
dq.nulls(df_p, "produits",     ["product_id","categorie","prix_unitaire"])
dq.nulls(df_t, "transactions", ["tx_id","store_id","product_id","ca_chf"])
dq.duplicates(df_m, "magasins",     ["store_id"])
dq.duplicates(df_p, "produits",     ["product_id"])
dq.duplicates(df_t, "transactions", ["tx_id"])
dq.range_check(df_m, "magasins",     "surface_m2",      min_v=50)
dq.range_check(df_p, "produits",     "prix_unitaire",   min_v=0.01)
dq.range_check(df_p, "produits",     "marge_pct",       min_v=0, max_v=1)
dq.range_check(df_t, "transactions", "ca_chf",          min_v=0)
dq.range_check(df_z, "zones",        "indice_attractivite", min_v=0, max_v=100)
dq.referential(df_t, df_m, "store_id",   "transactions->magasins")
dq.referential(df_t, df_p, "product_id", "transactions->produits")
dq.referential(df_z, df_m, "store_id",   "zones->magasins")

dq.report(spark)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
