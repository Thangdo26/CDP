package com.vft.cdp.auth.application;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.auth.domain.ApiKeyRecord;
import com.vft.cdp.auth.domain.ApiKeyStatus;
import com.vft.cdp.auth.infra.ApiKeyRepository;
import com.vft.cdp.auth.infra.RedisApiKeyCache;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ApiKeyAuthService {

    private final ApiKeyRepository repository;
    private final RedisApiKeyCache cache;
    private final PasswordEncoder passwordEncoder;

    public ApiKeyAuthContext authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new ApiKeyAuthException("Missing API Key");
        }

        // 1. Cache
        ApiKeyAuthContext cached = cache.get(apiKey);
        if (cached != null) {
            return cached;
        }

        // 2. Parse keyId: dạng "prefix_keyId_suffix"
        String[] parts = apiKey.split("_", 3);
        if (parts.length < 3) {
            throw new ApiKeyAuthException("Invalid API Key format");
        }
        String keyId = parts[1];

        // 3. Load từ MySQL
        Optional<ApiKeyRecord> opt = repository.findByKeyId(keyId);
        ApiKeyRecord recordKey = opt.orElseThrow(
                () -> new ApiKeyAuthException("API Key not found")
        );

        if (recordKey.getStatus() != ApiKeyStatus.ACTIVE) {
            throw new ApiKeyAuthException("API Key not active");
        }

        // 4. Validate: ưu tiên hash, fallback rawKey (dev)
        if (recordKey.getKeyHash() != null && !recordKey.getKeyHash().isBlank()) {
            if (!passwordEncoder.matches(apiKey, recordKey.getKeyHash())) {
                throw new ApiKeyAuthException("Invalid API Key");
            }
        } else if (recordKey.getRawKey() != null && !recordKey.getRawKey().isBlank()) {
            if (!apiKey.equals(recordKey.getRawKey())) {
                throw new ApiKeyAuthException("Invalid API Key");
            }
        } else {
            throw new ApiKeyAuthException("API Key credentials not configured");
        }

        // 5. Build context + cache
        ApiKeyAuthContext ctx = ApiKeyAuthContext.builder()
                .tenantId(recordKey.getTenantId())
                .appId(recordKey.getAppId())
                .keyId(recordKey.getKeyId())
                .build();

        cache.put(apiKey, ctx);
        return ctx;
    }

    public static class ApiKeyAuthException extends RuntimeException {
        public ApiKeyAuthException(String message) {
            super(message);
        }
    }
}
