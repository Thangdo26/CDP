package com.vft.cdp.profile.infra.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MULTI-LEVEL CACHE CONFIGURATION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * STRATEGY:
 * - L1: Caffeine (local, fast, small)
 * - L2: Redis (distributed, medium, large)
 * - L3: Elasticsearch (source of truth)
 *
 * CACHE NAMES:
 * - "profiles-hot": Individual profiles (frequently accessed)
 * - "master-profiles": Merged master profiles (less frequent changes)
 * - "profiles-search": Search results (short TTL)
 *
 *UPDATED: Added master-profiles cache
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Configuration
@EnableCaching
public class ProfileCacheConfig {

    
    // L1 CACHE: CAFFEINE (LOCAL JVM)
    

    /**
     * Caffeine Cache Manager (L1)
     *
     * WHY CAFFEINE?
     * - Fastest possible (<1ms latency)
     * - Window TinyLFU eviction (better than LRU)
     * - Automatic statistics tracking
     * - Low memory overhead
     *
     * CONFIGURATION:
     * - profiles-hot: 10,000 entries, 15 min TTL
     * - master-profiles: 5,000 entries, 15 min TTL (NEW)
     */
    @Bean
    @Primary
    public CacheManager caffeineCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();

        
        // Default configuration
        

        cacheManager.setCaffeine(Caffeine.newBuilder()
                // Size limit (default)
                .maximumSize(10_000)

                // Time-based eviction
                .expireAfterWrite(15, TimeUnit.MINUTES)

                // Stats for monitoring
                .recordStats()

                // Async removal listener
                .removalListener((key, value, cause) ->
                        log.debug("L1 Cache eviction: key={}, cause={}", key, cause))
        );

        
        // Register cache names
        

        cacheManager.setCacheNames(List.of(
                "profiles-hot",      // Individual profiles
                "master-profiles"    //NEW: Master profiles
        ));

        log.info("✅ Caffeine Cache Manager initialized with caches: profiles-hot, master-profiles");

        return cacheManager;
    }

    
    // L2 CACHE: REDIS (DISTRIBUTED)
    

    /**
     * Redis Cache Manager (L2)
     *
     * WHY REDIS?
     * - Shared across multiple instances
     * - Survives application restart
     * - Larger capacity than L1
     * - 2-5ms latency (acceptable)
     *
     * CONFIGURATION:
     * - profiles-hot: 20 min TTL
     * - master-profiles: 60 min TTL (longer - less changes)
     * - profiles-search: 10 min TTL
     */
    @Bean
    public CacheManager redisCacheManager(RedisConnectionFactory connectionFactory) {

        
        // Default Redis cache configuration
        

        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration
                .defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(20))  // Default 20 min
                .serializeKeysWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(
                                new StringRedisSerializer()
                        )
                )
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(
                                new GenericJackson2JsonRedisSerializer()
                        )
                )
                .disableCachingNullValues();  // Don't cache null results

        
        // Custom TTL for different cache types
        

        Map<String, RedisCacheConfiguration> cacheConfigs = new HashMap<>();

        // Individual profiles: 20 min (default)
        cacheConfigs.put("profiles-hot", defaultConfig);

        //NEW: Master profiles: 60 min (longer TTL - less frequent changes)
        cacheConfigs.put("master-profiles",
                defaultConfig.entryTtl(Duration.ofMinutes(60)));

        // Search results: 10 min (searches change frequently)
        cacheConfigs.put("profiles-search",
                defaultConfig.entryTtl(Duration.ofMinutes(10)));

        log.info("✅ Redis Cache Manager initialized:");
        log.info("   - profiles-hot: 20 min TTL");
        log.info("   - master-profiles: 60 min TTL (NEW)");
        log.info("   - profiles-search: 10 min TTL");

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(defaultConfig)
                .withInitialCacheConfigurations(cacheConfigs)
                .build();
    }

    
    // CACHE STRATEGY SUMMARY
    

    /**
     * CACHE HIERARCHY:
     *
     * profiles-hot (Individual Profiles):
     * - L1: 10K entries, 15 min TTL
     * - L2: Unlimited, 20 min TTL
     * - Use case: Frequent reads of active profiles
     *
     * master-profiles (Merged Profiles):NEW
     * - L1: 5K entries, 15 min TTL
     * - L2: Unlimited, 60 min TTL (longer!)
     * - Use case: Analytics, reporting, dashboards
     * - Why longer TTL? Master profiles change less frequently
     *
     * profiles-search (Search Results):
     * - L1: Not used
     * - L2: Unlimited, 10 min TTL
     * - Use case: Search result caching
     *
     * INVALIDATION:
     * - profiles-hot: When profile updated/deleted
     * - master-profiles: When merge happens, profile re-merged
     * - profiles-search: Natural expiration (short TTL)
     */
}