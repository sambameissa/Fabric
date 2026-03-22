-- GOV_DW_Gold → SQL Editor
-- Créer la procédure de reset

CREATE PROCEDURE dbo.usp_Reset_AllTables
AS
BEGIN
    -- Vider toutes les tables Gold
    TRUNCATE TABLE dbo.data_catalog;
    TRUNCATE TABLE dbo.pipeline_kpis;
    TRUNCATE TABLE dbo.dq_dashboard;
    TRUNCATE TABLE dbo.catalog_consolidated;

    -- Confirmation
    SELECT
        'data_catalog'        AS table_name, COUNT(*) AS nb_rows FROM dbo.data_catalog
    UNION ALL
    SELECT 'pipeline_kpis',                  COUNT(*) FROM dbo.pipeline_kpis
    UNION ALL
    SELECT 'dq_dashboard',                   COUNT(*) FROM dbo.dq_dashboard
    UNION ALL
    SELECT 'catalog_consolidated',           COUNT(*) FROM dbo.catalog_consolidated;
END;