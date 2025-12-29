package com.vft.cdp.profile.infra.cache;

import com.vft.cdp.profile.application.dto.MasterProfileDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE CACHE SERVICE - MULTI-LEVEL
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * STRATEGY:
 * - L1: Caffeine (local, fast, 15 min TTL, 5K entries)
 * - L2: Redis (distributed, 60 min TTL)
 * - L3: Elasticsearch (source of truth)
 *
 * CACHE KEY: master:{masterId}
 *
 * INVALIDATION:
 * - When master profile updated (merge, re-merge)
 * - Manual eviction via admin API
 *
 * WHY CACHE MASTER PROFILES?
 * - Master profiles change less frequently than individual profiles
 * - Often accessed for analytics, reporting
 * - Merge operations are expensive
 * - Read-heavy workload
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MasterProfileCacheService {

    private final CacheManager caffeineCacheManager;  // L1
    private final CacheManager redisCacheManager;     // L2

    private static final String CACHE_NAME = "master-profiles";

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CACHE KEY BUILDER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Build cache key from master profile ID
     *
     * Format: master:{masterId}
     */
    public String buildKey(String masterId) {
        return "master:" + masterId;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GET - MULTI-LEVEL CACHE LOOKUP
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get master profile from cache (multi-level)
     *
     * FLOW:
     * 1. Try L1 (Caffeine) → <1ms
     * 2. If miss, try L2 (Redis) → 2-5ms
     * 3. If miss, return empty → caller loads from ES
     *
     * @param masterId the master profile ID
     * @return Optional of MasterProfileDTO if cached
     */
    public Optional<MasterProfileDTO> get(String masterId) {
        String key = buildKey(masterId);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 1: Try L1 Cache (Caffeine)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            MasterProfileDTO dto = l1Cache.get(key, MasterProfileDTO.class);
            if (dto != null) {
                log.debug("✅ L1 Cache HIT: {}", key);
                return Optional.of(dto);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 2: Try L2 Cache (Redis)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            MasterProfileDTO dto = l2Cache.get(key, MasterProfileDTO.class);
            if (dto != null) {
                log.debug("✅ L2 Cache HIT: {}", key);

                // Populate L1 cache for future requests
                if (l1Cache != null) {
                    l1Cache.put(key, dto);
                    log.debug("📝 Populated L1 cache: {}", key);
                }

                return Optional.of(dto);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 3: Cache Miss - Return Empty
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        log.debug("❌ Cache MISS (both L1 & L2): {}", key);
        return Optional.empty();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PUT - WRITE-THROUGH TO BOTH LEVELS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Put master profile to cache (write-through both levels)
     *
     * STRATEGY: Write to both L1 and L2 immediately
     *
     * @param masterId the master profile ID
     * @param dto the master profile DTO to cache
     */
    public void put(String masterId, MasterProfileDTO dto) {
        if (dto == null) {
            log.warn("⚠️  Attempted to cache null MasterProfileDTO: {}", masterId);
            return;
        }

        String key = buildKey(masterId);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Write to L1 (Caffeine)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.put(key, dto);
            log.debug("📝 L1 Cache PUT: {} (profile_id={})", key, dto.getProfileId());
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Write to L2 (Redis)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.put(key, dto);
            log.debug("📝 L2 Cache PUT: {} (profile_id={})", key, dto.getProfileId());
        }

        log.info("✅ Master profile cached: masterId={}, mergedCount={}",
                masterId, dto.getMergedIds() != null ? dto.getMergedIds().size() : 0);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // EVICT - CACHE INVALIDATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Evict master profile from both cache levels
     *
     * WHEN TO CALL:
     * - After manual merge
     * - After auto merge
     * - When profile updates trigger re-merge
     * - Admin cache clear operation
     *
     * @param masterId the master profile ID to evict
     */
    public void evict(String masterId) {
        String key = buildKey(masterId);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Evict from L1 (Caffeine)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.evict(key);
            log.debug("🗑️  L1 Cache EVICT: {}", key);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Evict from L2 (Redis)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.evict(key);
            log.debug("🗑️  L2 Cache EVICT: {}", key);
        }

        log.info("🧹 Master profile evicted from cache: masterId={}", masterId);
    }

    /**
     * Evict multiple master profiles
     *
     * USAGE: Batch invalidation after auto-merge
     */
    public void evictMultiple(java.util.List<String> masterIds) {
        if (masterIds == null || masterIds.isEmpty()) {
            return;
        }

        log.info("🧹 Batch evicting {} master profiles", masterIds.size());

        for (String masterId : masterIds) {
            evict(masterId);
        }

        log.info("✅ Batch eviction completed");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CLEAR - ADMIN OPERATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Clear all master profile caches
     *
     * DANGER: Use only for admin operations or testing
     */
    public void clearAll() {
        log.warn("🚨 CLEARING ALL MASTER PROFILE CACHES");

        Cache l1Cache = caffeineCacheManager.getCache(CACHE_NAME);
        if (l1Cache != null) {
            l1Cache.clear();
            log.warn("🗑️  L1 Cache CLEARED (master-profiles)");
        }

        Cache l2Cache = redisCacheManager.getCache(CACHE_NAME);
        if (l2Cache != null) {
            l2Cache.clear();
            log.warn("🗑️  L2 Cache CLEARED (master-profiles)");
        }

        log.warn("✅ All master profile caches cleared");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATISTICS (for monitoring)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get cache statistics (if available)
     */
    public void logCacheStats() {
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("📊 Master Profile Cache Statistics");
        log.info("  L1 (Caffeine): {} entries", getCacheSize(caffeineCacheManager));
        log.info("  L2 (Redis): {} entries", getCacheSize(redisCacheManager));
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    private long getCacheSize(CacheManager cacheManager) {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        if (cache == null) {
            return 0;
        }

        // Note: Size calculation depends on cache implementation
        // Caffeine provides statistics, Redis may not
        return -1; // Unknown
    }
}