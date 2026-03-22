-- GOV_DW_Gold → SQL Editor
-- Remplacer la procédure usp_Load_FromGold

CREATE PROCEDURE dbo.usp_Load_FromGold
AS
BEGIN
    -- DROP + CTAS pour éviter tout problème de schéma
    DROP TABLE IF EXISTS dbo.data_catalog;
    SELECT * INTO dbo.data_catalog
    FROM GOV_LH_Gold.dbo.gold_data_catalog;

    DROP TABLE IF EXISTS dbo.pipeline_kpis;
    SELECT * INTO dbo.pipeline_kpis
    FROM GOV_LH_Gold.dbo.gold_pipeline_kpis;

    DROP TABLE IF EXISTS dbo.dq_dashboard;
    SELECT * INTO dbo.dq_dashboard
    FROM GOV_LH_Gold.dbo.gold_dq_dashboard;

    DROP TABLE IF EXISTS dbo.catalog_consolidated;
    SELECT * INTO dbo.catalog_consolidated
    FROM GOV_LH_Gold.dbo.gold_catalog_consolidated;

    -- Confirmation
    SELECT 'data_catalog'        AS table_name, COUNT(*) AS nb_rows FROM dbo.data_catalog
    UNION ALL
    SELECT 'pipeline_kpis',                     COUNT(*) FROM dbo.pipeline_kpis
    UNION ALL
    SELECT 'dq_dashboard',                      COUNT(*) FROM dbo.dq_dashboard
    UNION ALL
    SELECT 'catalog_consolidated',              COUNT(*) FROM dbo.catalog_consolidated;
END;