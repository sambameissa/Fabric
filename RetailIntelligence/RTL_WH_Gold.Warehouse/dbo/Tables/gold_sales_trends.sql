CREATE TABLE [dbo].[gold_sales_trends] (

	[annee] int NULL, 
	[mois] int NULL, 
	[ville] varchar(8000) NULL, 
	[format] varchar(8000) NULL, 
	[ca_chf] float NULL, 
	[nb_magasins] bigint NULL, 
	[ca_moyen_par_magasin] float NULL, 
	[_gold_ts] datetime2(6) NULL
);