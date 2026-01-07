package com.vft.cdp.auth.infra;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class RedisApiKeyCache {

    private static final String PREFIX = "cdp:auth:apikey:";

    private final RedisTemplate<String, Object> redisTemplate;
    private final ApiKeyCacheProperties props;

    @SuppressWarnings("unchecked")
    public ApiKeyAuthContext get(String apiKey) {
        String key = PREFIX + apiKey;
        Map<Object, Object> map = redisTemplate.opsForHash().entries(key);
        if (!map.isEmpty()) {
            String tenantId = (String) map.get("tenantId");
            String appId = (String) map.get("appId");
            String keyId = (String) map.get("keyId");
            return ApiKeyAuthContext.builder()
                    .tenantId(tenantId)
                    .appId(appId)
                    .keyId(keyId)
                    .build();
        }
        return null;
    }

    public void put(String apiKey, ApiKeyAuthContext ctx) {
        String key = PREFIX + apiKey;
        redisTemplate.opsForHash().put(key, "tenantId", ctx.getTenantId());
        redisTemplate.opsForHash().put(key, "appId", ctx.getAppId());
        redisTemplate.opsForHash().put(key, "keyId", ctx.getKeyId());
        redisTemplate.expire(key, Duration.ofSeconds(props.getTtlSeconds()));
    }

    public void evict(String apiKey) {
        redisTemplate.delete(PREFIX + apiKey);
    }
}
