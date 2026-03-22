CREATE TABLE [dbo].[monthly_kpis] (

	[mois] varchar(7) NULL, 
	[type] varchar(50) NULL, 
	[canal] varchar(50) NULL, 
	[devise] varchar(3) NULL, 
	[nb_transactions] int NULL, 
	[volume_chf] float NULL, 
	[avg_montant_chf] float NULL, 
	[nb_refuses] int NULL, 
	[nb_suspects] int NULL, 
	[taux_refus_pct] float NULL, 
	[_gold_built_ts] datetime2(6) NULL
);