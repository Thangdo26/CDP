package com.vft.cdp.profile.infra.es;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.vft.cdp.profile.domain.model.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/*
 * Elasticsearch Profile Repository Implementation
 *
 * RESPONSIBILITIES:
 * - Implement ProfileRepository interface
 * - Handle ES queries (find, save, search)
 * - Convert between Domain ↔ ES Document
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final SpringDataProfileRepository springDataRepo;
    private final ElasticsearchOperations esOps;

    // ========== FIND BY ID ==========
    /*
     * Find profile by composite key: tenant_id + app_id + user_id
     *
     * BUSINESS LOGIC:
     * - tenant_id: Công ty (VC, VAM)
     * - app_id: Ứng dụng (VTMN, MOMO)
     * - user_id: External user ID
     *
     * @return Optional<EnrichedProfile> - Present if found, empty otherwise
     */
    @Override
    public Optional<EnrichedProfile> find(String tenantId, String appId, String userId) {
        String id = ProfileMapper.buildId(tenantId, appId, userId);

        return springDataRepo.findById(id)
                .map(doc -> {
                    log.debug("✅ Profile found: {}", id);
                    return ProfileMapper.toDomain(doc);
                })
                .or(() -> {
                    log.debug("❌ Profile not found: {}", id);
                    return Optional.empty();
                });
    }

    // ========== SAVE ==========

    /**
     * Save or update profile in Elasticsearch
     *
     * @param profile EnrichedProfile to save
     * @return Saved profile with ES-generated fields
     */
    @Override
    public EnrichedProfile save(EnrichedProfile profile) {
        ProfileDocument doc = ProfileMapper.toDocument(profile);
        ProfileDocument saved = springDataRepo.save(doc);

        log.info("✅ Profile saved successfully: id={}", saved.getId());

        return ProfileMapper.toDomain(saved);
    }

    // Pagination safety limits
    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_DEEP_PAGINATION_OFFSET = 10000;  // ES default
    /**
     * OPTIMIZED Search
     *
     * OPTIMIZATIONS:
     * 1. Term queries for exact match fields (email, phone, idcard)
     * 2. Match queries WITHOUT fuzzy for performance
     * 3. Fuzzy only if explicitly requested
     * 4. Prefix queries for autocomplete use cases
     */
    @Override
    public Page<EnrichedProfile> search(SearchProfileRequest request) {

        // ✅ OPTIMIZATION 1: Validate pagination limits
        validatePaginationLimits(request);

        // ✅ OPTIMIZATION 2: Build optimized queries
        List<Query> mustQueries = buildOptimizedQueries(request);

        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        Pageable pageable = buildPageable(request);

        // ✅ OPTIMIZATION 3: Add query hints for ES
        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withPageable(pageable)
                .withTrackTotalHits(true)  // Accurate total count
                .withMaxResults(MAX_PAGE_SIZE)
                .build();

        SearchHits<ProfileDocument> searchHits = esOps.search(nativeQuery, ProfileDocument.class);

        List<EnrichedProfile> profiles = searchHits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .toList();

        log.info("✅ Search completed: {} results ({}ms)",
                profiles.size(), searchHits.getTotalHits());

        return PageableExecutionUtils.getPage(
                profiles,
                pageable,
                searchHits::getTotalHits
        );
    }

    /**
     * OPTIMIZATION: Validate pagination to prevent deep pagination issues
     */
    private void validatePaginationLimits(SearchProfileRequest request) {
        // Check page size
        if (request.getPageSize() > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Page size cannot exceed %d", MAX_PAGE_SIZE)
            );
        }

        // Check deep pagination
        int offset = request.getPage() * request.getPageSize();
        if (offset > MAX_DEEP_PAGINATION_OFFSET) {
            throw new IllegalArgumentException(
                    String.format(
                            "Deep pagination not supported. Use search-after for offset > %d",
                            MAX_DEEP_PAGINATION_OFFSET
                    )
            );
        }
    }

    /**
     * OPTIMIZED Query Builder
     *
     * STRATEGY:
     * 1. Use TERM queries for keyword fields (fastest)
     * 2. Use MATCH queries for text fields (no fuzzy by default)
     * 3. Use PREFIX queries for autocomplete
     * 4. Only use FUZZY if explicitly requested
     */
    private List<Query> buildOptimizedQueries(SearchProfileRequest request) {
        List<Query> mustQueries = new ArrayList<>();

        // Required: tenant_id (always term query)
        mustQueries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(request.getTenantId())
        )));

        // Optional filters
        addOptionalFilters(mustQueries, request);

        // Traits search (optimized)
        if (request.getTraits() != null) {
            addOptimizedTraitsQueries(mustQueries, request.getTraits());
        }

        return mustQueries;
    }

    /**
     * OPTIMIZED Traits Queries
     */
    private void addOptimizedTraitsQueries(
            List<Query> mustQueries,
            SearchProfileRequest.TraitsSearch traits) {

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // EXACT MATCH FIELDS (use TERM - fastest)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Email (keyword field)
        if (traits.getEmail() != null && !traits.getEmail().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.email")
                    .value(traits.getEmail().toLowerCase())  // Normalize
            )));
        }

        // Phone (keyword field)
        if (traits.getPhone() != null && !traits.getPhone().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.phone")
                    .value(traits.getPhone())
            )));
        }

        // ID card (keyword field)
        if (traits.getIdcard() != null && !traits.getIdcard().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.idcard")
                    .value(traits.getIdcard())
            )));
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // FULL-TEXT SEARCH FIELDS (optimized match)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Full name - MATCH without fuzzy (faster)
        if (traits.getFullName() != null && !traits.getFullName().isBlank()) {

            String searchTerm = traits.getFullName();

            // ✅ OPTIMIZATION: Use different query types based on input

            // Case 1: Short input (1-2 words) → Use prefix (autocomplete)
            if (searchTerm.split("\\s+").length <= 2) {
                mustQueries.add(Query.of(q -> q.matchPhrasePrefix(m -> m
                        .field("traits.full_name")
                        .query(searchTerm)
                        .maxExpansions(50)  // Limit expansions
                )));

                // Case 2: Exact phrase search (quoted)
            } else if (searchTerm.contains("\"")) {
                String cleanTerm = searchTerm.replace("\"", "");
                mustQueries.add(Query.of(q -> q.matchPhrase(m -> m
                        .field("traits.full_name")
                        .query(cleanTerm)
                )));

                // Case 3: Normal search → Match (NO fuzzy)
            } else {
                mustQueries.add(Query.of(q -> q.match(m -> m
                                .field("traits.full_name")
                                .query(searchTerm)
                        // ❌ NO FUZZY - 10x faster
                        // .fuzziness("AUTO")
                )));
            }
        }

        // Address - Prefix query for partial match
        if (traits.getAddress() != null && !traits.getAddress().isBlank()) {
            mustQueries.add(Query.of(q -> q.matchPhrasePrefix(m -> m
                    .field("traits.address")
                    .query(traits.getAddress())
                    .maxExpansions(50)
            )));
        }
    }

    private void addOptionalFilters(List<Query> mustQueries, SearchProfileRequest request) {
        // user_id
        if (request.getUserId() != null && !request.getUserId().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("user_id")
                    .value(request.getUserId())
            )));
        }

        // app_id
        if (request.getAppId() != null && !request.getAppId().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("app_id")
                    .value(request.getAppId())
            )));
        }

        // type
        if (request.getType() != null && !request.getType().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("type")
                    .value(request.getType())
            )));
        }
    }

    private Pageable buildPageable(SearchProfileRequest request) {
        String sortBy = request.getSortBy() != null ? request.getSortBy() : "updated_at";
        String sortOrder = request.getSortOrder() != null ? request.getSortOrder() : "desc";

        Sort sort = sortOrder.equalsIgnoreCase("asc")
                ? Sort.by(sortBy).ascending()
                : Sort.by(sortBy).descending();

        return PageRequest.of(
                request.getPage(),
                Math.min(request.getPageSize(), MAX_PAGE_SIZE),  // Enforce max
                sort
        );
    }

    @Override
    public void delete(String tenantId, String appId, String userId) {
        String id = ProfileMapper.buildId(tenantId, appId, userId);

        if (!springDataRepo.existsById(id)) {
            log.warn("❌ Profile not found to delete: {}|{}|{}", tenantId, appId, userId);
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND,
                    String.format("Profile not found: %s|%s|%s", tenantId, appId, userId)
            );
        }

        springDataRepo.deleteById(id);

        log.info("✅ Profile deleted from ES: id={}", id);
    }
}