package com.vft.cdp.profile.infra.cache;

import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.util.Optional;


@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileCacheService {

    private final CacheManager caffeineCacheManager;  // L1
    private final CacheManager redisCacheManager;     // L2

    private static final String CACHE_NAME = "profiles-hot";

    /**
     * Build cache key
     */
    public String buildKey(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    /**
     * Get profile from cache (multi-level)
     */
    public Optional<ProfileModel> get(String tenantId, String appId, String userId) {
        String key = buildKey(tenantId, appId, userId);

        // 1. Try L1 (Caffeine)
        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            ProfileModel profile = l1Cache.get(key, ProfileModel.class);
            if (profile != null) {
                log.debug("✅ L1 Cache HIT: {}", key);
                return Optional.of(profile);
            }
        }

        // 2. Try L2 (Redis)
        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            ProfileModel profile = l2Cache.get(key, ProfileModel.class);
            if (profile != null) {
                log.debug("✅ L2 Cache HIT: {}", key);

                // Populate L1
                if (l1Cache != null) {
                    l1Cache.put(key, profile);
                    log.debug("📝 Populated L1 cache: {}", key);
                }

                return Optional.of(profile);
            }
        }

        log.debug("❌ Cache MISS (both L1 & L2): {}", key);
        return Optional.empty();
    }

    /**
     * Put profile to cache (write-through both levels)
     */
    public void put(String tenantId, String appId, String userId, ProfileModel profile) {
        String key = buildKey(tenantId, appId, userId);

        // Write to L1
        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.put(key, profile);
            log.debug("📝 L1 Cache PUT: {}", key);
        }

        // Write to L2
        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.put(key, profile);
            log.debug("📝 L2 Cache PUT: {}", key);
        }
    }

    /**
     * Evict profile from both cache levels
     */
    public void evict(String tenantId, String appId, String userId) {
        String key = buildKey(tenantId, appId, userId);

        // Evict from L1
        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.evict(key);
            log.debug("🗑️  L1 Cache EVICT: {}", key);
        }

        // Evict from L2
        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.evict(key);
            log.debug("🗑️  L2 Cache EVICT: {}", key);
        }
    }

    /**
     * Clear all profile caches (admin operation)
     */
    public void clear() {
        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.clear();
            log.warn("🗑️  L1 Cache CLEARED");
        }

        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.clear();
            log.warn("🗑️  L2 Cache CLEARED");
        }
    }
}