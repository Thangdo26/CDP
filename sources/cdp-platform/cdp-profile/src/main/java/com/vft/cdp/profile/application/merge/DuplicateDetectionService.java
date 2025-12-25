// cdp-profile/src/main/java/com/vft/cdp/profile/application/merge/DuplicateDetectionService.java
package com.vft.cdp.profile.application.merge;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.infra.es.ProfileDocument;
import com.vft.cdp.profile.infra.es.ProfileMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Duplicate Detection Service
 *
 * STRATEGIES:
 * 1. Same idcard (highest confidence)
 * 2. Same phone + dob (high confidence)
 * 3. Same email + full_name (medium confidence)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final ElasticsearchOperations esOps;

    /**
     * Find duplicate profiles in a tenant
     *
     * @param tenantId Tenant to search in
     * @return Map<MatchKey, List<ProfileId>> - Grouped duplicates
     */
    public Map<String, List<EnrichedProfile>> findDuplicates(String tenantId) {

        log.info("🔍 Finding duplicates for tenant: {}", tenantId);

        Map<String, List<EnrichedProfile>> duplicateGroups = new HashMap<>();

        // Strategy 1: Group by idcard
        Map<String, List<EnrichedProfile>> byIdcard = findByIdcard(tenantId);
        duplicateGroups.putAll(byIdcard);

        // Strategy 2: Group by phone + dob
        Map<String, List<EnrichedProfile>> byPhoneDob = findByPhoneAndDob(tenantId);
        mergeGroups(duplicateGroups, byPhoneDob);

        // Strategy 3: Group by email + full_name
        Map<String, List<EnrichedProfile>> byEmailName = findByEmailAndName(tenantId);
        mergeGroups(duplicateGroups, byEmailName);

        // Filter: Only keep groups with 2+ profiles
        Map<String, List<EnrichedProfile>> result = duplicateGroups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        log.info("✅ Found {} duplicate groups", result.size());

        return result;
    }

    /**
     * Find duplicates by idcard
     */
    private Map<String, List<EnrichedProfile>> findByIdcard(String tenantId) {

        // Query: tenant_id AND idcard exists
        List<Query> mustQueries = new ArrayList<>();

        mustQueries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(tenantId)
        )));

        mustQueries.add(Query.of(q -> q.exists(e -> e
                .field("traits.idcard")
        )));

        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withMaxResults(10000)  // Adjust based on data size
                .build();

        SearchHits<ProfileDocument> hits = esOps.search(nativeQuery, ProfileDocument.class);

        // Group by idcard
        Map<String, List<EnrichedProfile>> groups = hits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .filter(p -> p.getTraits() != null && p.getTraits().getIdcard() != null)
                .collect(Collectors.groupingBy(
                        p -> "idcard:" + p.getTraits().getIdcard()
                ));

        log.info("Found {} groups by idcard", groups.size());

        return groups;
    }

    /**
     * Find duplicates by phone + dob
     */
    private Map<String, List<EnrichedProfile>> findByPhoneAndDob(String tenantId) {

        List<Query> mustQueries = new ArrayList<>();

        mustQueries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(tenantId)
        )));

        mustQueries.add(Query.of(q -> q.exists(e -> e
                .field("traits.phone")
        )));

        mustQueries.add(Query.of(q -> q.exists(e -> e
                .field("traits.dob")
        )));

        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withMaxResults(10000)
                .build();

        SearchHits<ProfileDocument> hits = esOps.search(nativeQuery, ProfileDocument.class);

        Map<String, List<EnrichedProfile>> groups = hits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getDob() != null)
                .collect(Collectors.groupingBy(
                        p -> "phone_dob:" + p.getTraits().getPhone() + "_" + p.getTraits().getDob()
                ));

        log.info("Found {} groups by phone+dob", groups.size());

        return groups;
    }

    /**
     * Find duplicates by strategy
     */
    public Map<String, List<EnrichedProfile>> findDuplicatesByStrategy(
            String tenantId,
            String strategy) {

        log.info("🔍 Finding duplicates: tenant={}, strategy={}", tenantId, strategy);

        Map<String, List<EnrichedProfile>> result = new HashMap<>();

        switch (strategy.toLowerCase()) {
            case "idcard_only":
                result.putAll(findByIdcard(tenantId));
                break;

            case "phone_dob":
                result.putAll(findByPhoneAndDob(tenantId));
                break;

            case "email_name":
                result.putAll(findByEmailAndName(tenantId));
                break;

            case "all":
            default:
                // Use all strategies
                result.putAll(findByIdcard(tenantId));
                mergeGroups(result, findByPhoneAndDob(tenantId));
                mergeGroups(result, findByEmailAndName(tenantId));
                break;
        }

        // Filter: Only keep groups with 2+ profiles
        return result.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Find duplicates by email + full_name
     */
    private Map<String, List<EnrichedProfile>> findByEmailAndName(String tenantId) {

        List<Query> mustQueries = new ArrayList<>();

        mustQueries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(tenantId)
        )));

        mustQueries.add(Query.of(q -> q.exists(e -> e
                .field("traits.email")
        )));

        mustQueries.add(Query.of(q -> q.exists(e -> e
                .field("traits.full_name")
        )));

        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withMaxResults(10000)
                .build();

        SearchHits<ProfileDocument> hits = esOps.search(nativeQuery, ProfileDocument.class);

        Map<String, List<EnrichedProfile>> groups = hits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getEmail() != null
                        && p.getTraits().getFullName() != null)
                .collect(Collectors.groupingBy(
                        p -> "email_name:" + p.getTraits().getEmail() + "_" + p.getTraits().getFullName()
                ));

        log.info("Found {} groups by email+name", groups.size());

        return groups;
    }

    /**
     * Merge duplicate groups (avoid double counting)
     */
    private void mergeGroups(
            Map<String, List<EnrichedProfile>> target,
            Map<String, List<EnrichedProfile>> source) {

        for (Map.Entry<String, List<EnrichedProfile>> entry : source.entrySet()) {
            target.merge(entry.getKey(), entry.getValue(), (existing, incoming) -> {
                // Merge and deduplicate
                Set<String> existingIds = existing.stream()
                        .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                        .collect(Collectors.toSet());

                List<EnrichedProfile> merged = new ArrayList<>(existing);

                for (EnrichedProfile profile : incoming) {
                    String id = ProfileMapper.buildId(
                            profile.getTenantId(),
                            profile.getAppId(),
                            profile.getUserId()
                    );

                    if (!existingIds.contains(id)) {
                        merged.add(profile);
                        existingIds.add(id);
                    }
                }

                return merged;
            });
        }
    }
}