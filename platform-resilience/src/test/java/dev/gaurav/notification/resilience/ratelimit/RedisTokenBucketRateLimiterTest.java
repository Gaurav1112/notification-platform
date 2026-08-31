package dev.gaurav.notification.resilience.ratelimit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The Lua arithmetic is exercised against a real Redis in the integration suite. What is asserted
 * here is the policy around it, which is where the damaging mistakes are.
 */
class RedisTokenBucketRateLimiterTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final RedisTokenBucketRateLimiter limiter =
            new RedisTokenBucketRateLimiter(redis, RateLimitQuota.perSecond(100));

    @Test
    @DisplayName("a Redis outage must not stop a tenant's one-time passcodes")
    void failsOpenWhenRedisIsDown() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("no route to redis"));

        assertThat(limiter.tryAcquire("provider:twilio:sms"))
                .as("fail open on quota: overshooting a rate limit costs 429s, rejecting paying "
                        + "traffic because a cache is down is an outage we caused")
                .isTrue();
        assertThat(limiter.failOpenCount())
                .as("the fallback must be visible on a dashboard, not silent")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a script that did not run is treated as an outage, not as a denial")
    void nullResultFailsOpen() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(null);

        assertThat(limiter.tryAcquire("tenant:42")).isTrue();
        assertThat(limiter.failOpenCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("an exhausted bucket refuses the call, and does not fail open")
    void deniesWhenTheBucketIsEmpty() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(0L);

        assertThat(limiter.tryAcquire("tenant:42")).isFalse();
        assertThat(limiter.failOpenCount()).isZero();
    }

    @Test
    @DisplayName("a 50-message batch spends 50 permits, not one")
    void batchesConsumeOnePermitPerMessage() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(1L);

        limiter.tryAcquire("provider:ses:email", 50);

        // rate, capacity, now, requested, ttl — "requested" is the one that matters here:
        // charging a batch a single permit is how a 100/s quota becomes 5,000/s.
        verify(redis).execute(any(RedisScript.class), eq(List.of("rl:provider:ses:email")),
                eq("100.0"), eq("100"), anyString(), eq("50"), anyString());
    }

    @Test
    @DisplayName("a request larger than the bucket can ever hold is refused instead of retried forever")
    void oversizedRequestIsRefusedWithoutTouchingRedis() {
        assertThat(limiter.tryAcquire("tenant:42", 5_000)).isFalse();

        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        assertThat(limiter.failOpenCount())
                .as("this is a genuine denial, not a Redis fallback, and must not pollute the gauge")
                .isZero();
    }

    @Test
    @DisplayName("zero permits would be a free call and is a caller bug, not a quota decision")
    void rejectsNonPositivePermits() {
        assertThatIllegalArgumentException().isThrownBy(() -> limiter.tryAcquire("tenant:42", 0));
    }

    @Test
    @DisplayName("a quota with no refill rate would deadlock every caller and is refused")
    void rejectsAnUnfillableQuota() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RateLimitQuota(0, 10));
        assertThatIllegalArgumentException().isThrownBy(() -> new RateLimitQuota(10, 0));
    }
}
