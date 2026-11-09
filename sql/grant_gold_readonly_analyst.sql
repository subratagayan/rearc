-- Read-only analyst access to the Gold layer only (Unity Catalog).
-- Run as the catalog/schema owner. Schemas below are the dev bundle target; adjust for prod.

-- 1. Let the analyst reach the catalog and the gold schema (no data access by itself).
GRANT USE CATALOG ON CATALOG rearc TO `analyst`;
GRANT USE SCHEMA ON SCHEMA rearc.dev_subratagayanhitk_gold TO `analyst`;

-- 2. SELECT on the gold schema. Each layer has its own schema, so this exposes gold only and also
--    covers gold tables added later. Bronze and silver get no grants.
GRANT SELECT ON SCHEMA rearc.dev_subratagayanhitk_gold TO `analyst`;

-- 3. Remove broad default grants on the catalog: every account user (analyst included) had
--    ALL PRIVILEGES and MANAGE, which would let them modify or drop any table.
REVOKE ALL PRIVILEGES ON CATALOG rearc FROM `account users`;
REVOKE MANAGE ON CATALOG rearc FROM `account users`;
REVOKE EXTERNAL USE SCHEMA ON CATALOG rearc FROM `account users`;

-- 4. Verify.
SHOW GRANTS ON CATALOG rearc;
SHOW GRANTS ON SCHEMA rearc.dev_subratagayanhitk_gold;
SHOW GRANTS ON SCHEMA rearc.dev_subratagayanhitk_silver;
SHOW GRANTS ON SCHEMA rearc.dev_subratagayanhitk_bronze;
