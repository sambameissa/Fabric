CREATE TABLE [dbo].[client_kpis] (

	[client_id] varchar(10) NOT NULL, 
	[nom] varchar(100) NULL, 
	[prenom] varchar(100) NULL, 
	[segment] varchar(50) NULL, 
	[city] varchar(50) NULL, 
	[risk_score] float NULL, 
	[statut] varchar(20) NULL, 
	[nb_transactions] int NULL, 
	[total_montant_chf] float NULL, 
	[avg_montant_chf] float NULL, 
	[max_transaction_chf] float NULL, 
	[last_tx_date] date NULL, 
	[days_since_last_tx] int NULL, 
	[nb_suspicious_tx] int NULL, 
	[nb_types_utilises] int NULL, 
	[statut_activite] varchar(20) NULL, 
	[risque_fraude] varchar(20) NULL, 
	[_gold_built_ts] datetime2(6) NULL
);