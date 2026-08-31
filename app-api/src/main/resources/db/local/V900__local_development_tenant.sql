-- =============================================================================
-- LOCAL ONLY. This file is on the Flyway path only under the `local` profile
-- (see spring.flyway.locations in app-api/src/main/resources/application.yml).
--
-- Why it exists: with notification.api.security.permit-all=true every request is
-- attributed to the fixed development tenant declared in ApiSecurityProperties,
-- and every write scopes on notif.tenant.id. V1__baseline.sql seeds reference
-- data (delivery_status, retry_policy) but no tenants, and it is right not to --
-- a tenant is customer data, not schema. So without this row `./mvnw
-- spring-boot:run` plus a curl answers 401 for a token the platform itself
-- issued, which reads as a bug in the API rather than as an empty database.
--
-- Version 900 rather than 2 so it can never collide with a real migration added
-- to platform-persistence, and so its position in the applied history makes
-- obvious that it is not part of the baseline.
--
-- public_id matches ApiSecurityProperties.DEFAULT_LOCAL_TENANT. Changing either
-- without the other breaks the local demo with a 401 and no other clue.
-- =============================================================================

INSERT INTO notif.tenant (public_id, slug, display_name, status, daily_send_quota, rate_limit_rps)
VALUES ('00000000-0000-7000-8000-000000000001',
        'local-development',
        'Local Development',
        'ACTIVE',
        1000000,
        100)
ON CONFLICT (public_id) DO NOTHING;
