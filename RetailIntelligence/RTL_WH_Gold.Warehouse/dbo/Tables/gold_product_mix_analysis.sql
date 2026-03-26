CREATE TABLE [dbo].[gold_product_mix_analysis] (

	[store_id] varchar(8000) NULL, 
	[annee] int NULL, 
	[categorie] varchar(8000) NULL, 
	[famille] varchar(8000) NULL, 
	[ca_chf] float NULL, 
	[marge_chf] float NULL, 
	[nb_unites] bigint NULL, 
	[nb_references] bigint NULL, 
	[nb_bio] bigint NULL, 
	[nb_local] bigint NULL, 
	[nom] varchar(8000) NULL, 
	[ville] varchar(8000) NULL, 
	[surface_m2] int NULL, 
	[ca_par_m2] float NULL, 
	[taux_marge_pct] float NULL, 
	[_gold_ts] datetime2(6) NULL
);