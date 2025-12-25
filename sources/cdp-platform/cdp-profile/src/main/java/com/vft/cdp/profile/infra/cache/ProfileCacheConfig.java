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

/*
 * Multi-Level Cache Configuration
 *
 * STRATEGY:
 * - L1: Caffeine (local, fast, small)
 * - L2: Redis (distributed, medium, large)
 * - L3: Elasticsearch (source of truth)
 *
 * CACHE NAMES:
 * - "profiles-hot": High-traffic profiles (L1 + L2)
 * - "profiles-search": Search results (L2 only)
 */
@Slf4j
@Configuration
@EnableCaching
public class ProfileCacheConfig {

    /*
     * L1 Cache: Caffeine (Local JVM)
     *
     * WHY CAFFEINE?
     * - Fastest possible (in-memory, same JVM)
     * - <1ms latency
     * - Window TinyLFU eviction algorithm (better than LRU)
     * - Automatic stats tracking
     *
     * CONFIGURATION:
     * - Max size: 10,000 profiles
     * - TTL: 5 minutes (after write)
     * - Eviction: Size-based + Time-based
     */
    @Bean
    @Primary
    public CacheManager caffeineCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();

        cacheManager.setCaffeine(Caffeine.newBuilder()
                // Size limit
                .maximumSize(10_000)

                // Time-based eviction
                .expireAfterWrite(15, TimeUnit.MINUTES)

                // Stats for monitoring
                .recordStats()

                // Async removal listener for logging
                .removalListener((key, value, cause) -> log.debug("L1 Cache eviction: key={}, cause={}", key, cause))
        );

        cacheManager.setCacheNames(List.of("profiles-hot"));

        return cacheManager;
    }

    /*
     * L2 Cache: Redis (Distributed)
     *
     * WHY REDIS?
     * - Shared across multiple instances
     * - Survives application restart
     * - Larger capacity than L1
     * - 2-5ms latency (acceptable)
     *
     * CONFIGURATION:
     * - TTL: 30 minutes (longer than L1)
     * - Serialization: JSON (human-readable, debugging)
     */
    @Bean
    public CacheManager redisCacheManager(RedisConnectionFactory connectionFactory) {

        // JSON serialization config
        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration
                .defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(20))  // 20 min TTL
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

        // Hot profiles: 30 min
        cacheConfigs.put("profiles-hot", defaultConfig);

        // Search results: 10 min (search changes more frequently)
        cacheConfigs.put("profiles-search",
                defaultConfig.entryTtl(Duration.ofMinutes(10)));

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(defaultConfig)
                .withInitialCacheConfigurations(cacheConfigs)
                .build();
    }
}