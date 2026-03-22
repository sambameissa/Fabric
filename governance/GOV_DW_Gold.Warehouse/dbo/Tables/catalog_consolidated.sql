CREATE TABLE [dbo].[catalog_consolidated] (

	[asset_id] varchar(8000) NULL, 
	[asset_name] varchar(8000) NULL, 
	[asset_type] varchar(8000) NULL, 
	[sector] varchar(8000) NULL, 
	[layer] varchar(8000) NULL, 
	[status] varchar(8000) NULL, 
	[classification] varchar(8000) NULL, 
	[quality_score] float NULL, 
	[avg_dq_score] float NULL, 
	[dq_status] varchar(8000) NULL, 
	[nb_checks] bigint NULL, 
	[last_dq_date] date NULL, 
	[owner] varchar(8000) NULL, 
	[catalog_ts] varchar(8000) NULL, 
	[documented] bit NULL
);