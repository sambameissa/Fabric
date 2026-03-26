CREATE TABLE [dbo].[gold_store_performance_kpis] (

	[store_id] varchar(8000) NULL, 
	[annee] int NULL, 
	[mois] int NULL, 
	[ca_chf] float NULL, 
	[marge_chf] float NULL, 
	[nb_unites] bigint NULL, 
	[nb_transactions] bigint NULL, 
	[nb_references] bigint NULL, 
	[nb_ruptures] bigint NULL, 
	[nb_promos] bigint NULL, 
	[panier_moyen_chf] float NULL, 
	[taux_marge_pct] float NULL, 
	[taux_rupture_pct] float NULL, 
	[nom] varchar(8000) NULL, 
	[ville] varchar(8000) NULL, 
	[format] varchar(8000) NULL, 
	[concept] varchar(8000) NULL, 
	[surface_m2] int NULL, 
	[statut] varchar(8000) NULL, 
	[ca_par_m2] float NULL, 
	[_gold_ts] datetime2(6) NULL
);