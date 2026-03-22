CREATE TABLE [dbo].[pipeline_kpis] (

	[pipeline_name] varchar(8000) NULL, 
	[layer] varchar(8000) NULL, 
	[nb_runs] bigint NULL, 
	[nb_success] bigint NULL, 
	[nb_failed] bigint NULL, 
	[avg_duration_sec] float NULL, 
	[avg_rows_processed] float NULL, 
	[last_run_ts] datetime2(6) NULL, 
	[success_rate] float NULL, 
	[pipeline_health] varchar(8000) NULL, 
	[_gold_ts] datetime2(6) NULL
);