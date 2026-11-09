-- Read-only analyst access to the Gold layer only (Unity Catalog).
-- Run as the catalog/schema owner. Catalog/schema below are the dev bundle target; adjust for prod.

-- 1. Let the analyst see the catalog and schema (no data access by itself).
GRANT USE CATALOG ON CATALOG rearc TO `analyst`;
GRANT USE SCHEMA ON SCHEMA rearc.dev_subratagayanhitk_rearc_quest TO `analyst`;

-- 2. SELECT on the three Gold tables only. A schema-level SELECT would also expose bronze, silver and
--    active_files, so it is revoked in favour of table-level grants.
REVOKE SELECT ON SCHEMA rearc.dev_subratagayanhitk_rearc_quest FROM `analyst`;
GRANT SELECT ON TABLE rearc.dev_subratagayanhitk_rearc_quest.gold_population_stats TO `analyst`;
GRANT SELECT ON TABLE rearc.dev_subratagayanhitk_rearc_quest.gold_best_year_by_series TO `analyst`;
GRANT SELECT ON TABLE rearc.dev_subratagayanhitk_rearc_quest.gold_series_population TO `analyst`;

-- 3. Remove broad default grants on the catalog: every account user (analyst included) had
--    ALL PRIVILEGES and MANAGE, which would let them modify or drop any table.
REVOKE ALL PRIVILEGES ON CATALOG rearc FROM `account users`;
REVOKE MANAGE ON CATALOG rearc FROM `account users`;
REVOKE EXTERNAL USE SCHEMA ON CATALOG rearc FROM `account users`;

-- 4. Verify.
SHOW GRANTS ON CATALOG rearc;
SHOW GRANTS ON SCHEMA rearc.dev_subratagayanhitk_rearc_quest;
SHOW GRANTS ON TABLE rearc.dev_subratagayanhitk_rearc_quest.gold_best_year_by_series;
SHOW GRANTS ON TABLE rearc.dev_subratagayanhitk_rearc_quest.silver_bls_observations;
