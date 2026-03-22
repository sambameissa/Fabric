CREATE TABLE [dbo].[dq_dashboard] (

	[check_type] varchar(8000) NULL, 
	[status] varchar(8000) NULL, 
	[mois] varchar(8000) NULL, 
	[nb_checks] bigint NULL, 
	[avg_score] float NULL, 
	[avg_value] float NULL, 
	[total_rows_failed] bigint NULL, 
	[_gold_ts] datetime2(6) NULL
);