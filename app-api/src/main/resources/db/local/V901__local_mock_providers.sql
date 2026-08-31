-- =============================================================================
-- LOCAL ONLY. On the Flyway path only under the `local` profile, for the same
-- reason as V900: which vendors a deployment routes to is operator-managed
-- configuration, not schema. A real environment registers Twilio, SES and FCM
-- here; this file registers the five mocks that ship with the repository.
--
-- Why it exists: `notif.provider` was empty, and nothing said so.
--
-- The provider adapters are Spring beans, so ProviderRegistry found all five
-- and /v1/providers/health cheerfully reported them CLOSED and healthy. But
-- delivery_attempt.provider_id is a smallint FK into this table, so the first
-- thing the worker does after selecting a provider is resolve its code to that
-- id -- and ProviderIds throws when the row is absent:
--
--   IllegalStateException: no notif.provider row for code 'mock-email-secondary';
--   the provider bean exists but the configuration migration that registers it
--   has not run
--
-- Every dispatch therefore failed at step 5 of AbstractChannelWorker, before any
-- provider call, and the error handler routed it to notification.dlq. The
-- symptom was a platform that accepted 202s, advanced rows to QUEUED, and
-- delivered nothing, with an empty delivery_attempt table and every circuit
-- eternally CLOSED -- because a circuit breaker only records calls that happen.
--
-- ProviderIds' Javadoc names this exact failure ("a deployment that skipped a
-- config migration") and chose to throw rather than default. That was the right
-- call: defaulting would have written attempts against a provider id belonging
-- to a different vendor and quietly corrupted every cost and health rollup.
-- The check worked. The migration it presupposed did not exist.
--
-- Columns mirror ProviderCapabilities on each adapter rather than a plausible
-- guess. Two sources of truth for "does this vendor batch" is how a batch-size
-- of 50 gets sent to an API that accepts one.
-- =============================================================================

INSERT INTO notif.provider (code, display_name, channel, vendor,
                            supports_batching, max_batch_size,
                            supports_idempotency_key, supports_webhook, is_active)
VALUES
    -- MockEmailProvider.capabilities() -> (true, SES_BULK_DESTINATION_LIMIT = 50, false, true, true).
    -- Models SES SendBulkEmail: 50 destinations per call, no client-supplied
    -- idempotency key, event-destination webhooks for bounce and complaint.
    ('mock-email-primary',   'Mock Email (primary)',   'EMAIL', 'mock-ses',
     true,  50, false, true, true),
    ('mock-email-secondary', 'Mock Email (secondary)', 'EMAIL', 'mock-sendgrid',
     true,  50, false, true, true),

    -- MockSmsProvider.capabilities() -> (false, 1, false, true, false).
    -- One message per request and no client reference field, which is precisely
    -- why SMS is at-most-once in this platform: an indeterminate send cannot be
    -- reconciled against anything afterwards.
    ('mock-sms-primary',     'Mock SMS (primary)',     'SMS',   'mock-twilio',
     false,  1, false, true, true),
    ('mock-sms-secondary',   'Mock SMS (secondary)',   'SMS',   'mock-sns',
     false,  1, false, true, true),

    -- MockPushProvider.capabilities() -> (false, 1, false, true, false).
    ('mock-push-primary',    'Mock Push (primary)',    'PUSH',  'mock-fcm',
     false,  1, false, true, true)
ON CONFLICT (code) DO NOTHING;
