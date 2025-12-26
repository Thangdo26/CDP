package com.vft.cdp.profile.infra.es;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.application.repository.ProfileRepository;
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

@Slf4j
@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final SpringDataProfileRepository springDataRepo;
    private final ElasticsearchOperations esOps;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CONFIGURATION CONSTANTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_DEEP_PAGINATION_OFFSET = 10000;

    // Fuzzy matching settings
    private static final String FUZZINESS_AUTO = "AUTO";  // AUTO adjusts based on term length
    private static final int PREFIX_LENGTH = 2;            // First 2 chars must match exactly
    private static final int MAX_EXPANSIONS = 50;          // Limit fuzzy term expansions

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CRUD OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

    @Override
    public EnrichedProfile save(EnrichedProfile profile) {
        ProfileDocument doc = ProfileMapper.toDocument(profile);
        ProfileDocument saved = springDataRepo.save(doc);
        log.info("✅ Profile saved successfully: id={}", saved.getId());
        return ProfileMapper.toDomain(saved);
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ENHANCED SEARCH
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * SEARCH PROFILES - ENHANCED VERSION
     *
     * Supports:
     * - tenant_id (required)
     * - user_id, app_id, type (exact match)
     * - traits:
     *   → EXACT: phone, idcard, old_idcard, gender, dob
     *   → FUZZY: full_name, first_name, last_name, email, address
     *   → PARTIAL: religion
     * - metadata (dynamic fields)
     */
    @Override
    public Page<EnrichedProfile> search(SearchProfileRequest request) {

        // 1. Validate pagination
        validatePaginationLimits(request);

        // 2. Build query
        List<Query> mustQueries = buildSearchQueries(request);

        // 3. Log query for debugging
        log.info("📊 Total must queries: {}", mustQueries.size());
        mustQueries.forEach(q -> log.debug("  - Query: {}", q));

        // 4. Build bool query
        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        // 5. Build pageable
        Pageable pageable = buildPageable(request);

        // 6. Execute query
        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withPageable(pageable)
                .withTrackTotalHits(true)
                .withMaxResults(MAX_PAGE_SIZE)
                .build();

        long startTime = System.currentTimeMillis();
        SearchHits<ProfileDocument> searchHits = esOps.search(nativeQuery, ProfileDocument.class);
        long duration = System.currentTimeMillis() - startTime;

        // 7. Convert results
        List<EnrichedProfile> profiles = searchHits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .toList();

        log.info("✅ Search completed: {} results in {}ms (total hits: {})",
                profiles.size(), duration, searchHits.getTotalHits());

        return PageableExecutionUtils.getPage(
                profiles,
                pageable,
                searchHits::getTotalHits
        );
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY BUILDERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * BUILD SEARCH QUERIES - ENHANCED
     */
    private List<Query> buildSearchQueries(SearchProfileRequest request) {
        List<Query> queries = new ArrayList<>();

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. REQUIRED: tenant_id
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        queries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(request.getTenantId())
        )));

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. OPTIONAL TOP-LEVEL FIELDS (Exact match)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        addTermQueryIfPresent(queries, "user_id", request.getUserId());
        addTermQueryIfPresent(queries, "app_id", request.getAppId());
        addTermQueryIfPresent(queries, "type", request.getType());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. TRAITS FIELDS (Mixed strategies)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if (request.getTraits() != null) {
            addTraitsQueries(queries, request.getTraits());
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4. METADATA FIELDS
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if (request.getMetadata() != null && !request.getMetadata().isEmpty()) {
            addMetadataQueries(queries, request.getMetadata());
        }

        return queries;
    }

    /**
     * ADD TRAITS QUERIES - FIXED TO MATCH ES MAPPING
     *
     * Mapping Analysis:
     * - text fields: full_name, address (support fuzzy/match)
     * - keyword fields: email, phone, idcard, old_idcard, gender, dob,
     *                   first_name, last_name, religion (exact match only)
     */
    private void addTraitsQueries(List<Query> queries, SearchProfileRequest.TraitsSearch traits) {

        log.debug("🔍 Adding traits queries...");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // KEYWORD FIELDS - EXACT MATCH ONLY
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Phone (keyword - exact)
        addTermQueryIfPresent(queries, "traits.phone", traits.getPhone());

        // Email (keyword - exact, case-insensitive)
        addTermQueryIfPresentLowercase(queries, "traits.email", traits.getEmail());

        // ID Card / CCCD (keyword - exact)
        addTermQueryIfPresent(queries, "traits.idcard", traits.getIdcard());

        // Old ID Card / CMND (keyword - exact)
        addTermQueryIfPresent(queries, "traits.old_idcard", traits.getOldIdcard());

        // Gender (keyword - exact, case-insensitive)
        addTermQueryIfPresentLowercase(queries, "traits.gender", traits.getGender());

        // DOB (keyword - exact)
        addTermQueryIfPresent(queries, "traits.dob", traits.getDob());

        // Religion (keyword - exact, case-insensitive)
        addTermQueryIfPresentLowercase(queries, "traits.religion", traits.getReligion());

        // First Name (keyword - wildcard for partial, case-insensitive)
        addWildcardQueryIfPresent(queries, "traits.first_name", traits.getFirstName());

        // Last Name (keyword - wildcard for partial, case-insensitive)
        addWildcardQueryIfPresent(queries, "traits.last_name", traits.getLastName());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // TEXT FIELDS - FUZZY MATCH SUPPORTED
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Full Name (text - fuzzy + case-insensitive + partial)
        // Example: "Nguyen" → matches "Nguyễn", "Ngyuen", "nguyen"
        addFuzzyMatchQueryIfPresent(queries, "traits.full_name", traits.getFullName());

        // Address (text - fuzzy + case-insensitive + partial)
        // Example: "Ha Noi" → matches "Hà Nội", "Hanoi", "hà nội"
        addFuzzyMatchQueryIfPresent(queries, "traits.address", traits.getAddress());
    }

    /**
     * ADD METADATA QUERIES (Dynamic fields)
     */
    private void addMetadataQueries(List<Query> queries, java.util.Map<String, Object> metadata) {
        metadata.forEach((key, value) -> {
            if (value != null) {
                String fieldPath = "metadata." + key;
                queries.add(Query.of(q -> q.term(t -> t
                        .field(fieldPath)
                        .value(String.valueOf(value))
                )));
            }
        });
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY HELPER METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * HELPER: Add term query if value is present (exact match, case-sensitive)
     * Use for: phone, idcard, old_idcard, dob, user_id, app_id, type
     */
    private void addTermQueryIfPresent(List<Query> queries, String field, String value) {
        if (value != null && !value.isBlank()) {
            log.debug("  ✅ [EXACT] {} = {}", field, value);
            queries.add(Query.of(q -> q.term(t -> t
                    .field(field)
                    .value(value)
            )));
        }
    }

    /**
     * HELPER: Add term query with lowercase normalization
     * Use for: email, gender, religion (keyword fields that need case-insensitive)
     *
     * ES stores these as lowercase, so we normalize input to match
     */
    private void addTermQueryIfPresentLowercase(List<Query> queries, String field, String value) {
        if (value != null && !value.isBlank()) {
            String normalized = value.toLowerCase().trim();
            log.debug("  ✅ [EXACT-LC] {} = {} (normalized)", field, normalized);
            queries.add(Query.of(q -> q.term(t -> t
                    .field(field)
                    .value(normalized)
            )));
        }
    }

    /**
     * HELPER: Add wildcard query for partial matching on keyword fields
     * Use for: first_name, last_name (keyword fields that need partial search)
     *
     * Example: "Dinh" → matches "Do Dinh", "Dinh", "dinh"
     */
    private void addWildcardQueryIfPresent(List<Query> queries, String field, String value) {
        if (value != null && !value.isBlank()) {
            String normalized = value.toLowerCase().trim();
            log.debug("  ✅ [WILDCARD] {} ~ *{}* (case-insensitive)", field, normalized);

            queries.add(Query.of(q -> q.wildcard(w -> w
                    .field(field)
                    .value("*" + normalized + "*")
                    .caseInsensitive(true)
            )));
        }
    }

    /**
     * HELPER: Add fuzzy match query (partial + fuzzy, case-insensitive)
     * Use for: full_name, address (TEXT fields only)
     *
     * IMPORTANT: For Vietnamese text, this also removes accents manually
     * since ES doesn't have asciifolding configured in the current mapping.
     *
     * Fuzzy parameters:
     * - fuzziness: AUTO (0 for 1-2 chars, 1 for 3-5 chars, 2 for 6+ chars)
     * - prefix_length: 1 (reduced to handle Vietnamese better)
     * - max_expansions: 100 (increased for better accent matching)
     *
     * Example:
     * - "Nguyen" → matches "Nguyễn" (accent), "Ngyuen" (typo)
     * - "Ha Noi" → matches "Hà Nội" (accent), "Hanoi" (no space)
     */
    private void addFuzzyMatchQueryIfPresent(List<Query> queries, String field, String value) {
        if (value != null && !value.isBlank()) {
            String normalized = removeVietnameseAccents(value.toLowerCase().trim());
            log.debug("  ✅ [FUZZY] {} ~ {} (accent-removed, fuzziness=AUTO)", field, normalized);

            // Use bool query with SHOULD to match both with/without accents
            BoolQuery boolQuery = BoolQuery.of(b -> b
                    // Option 1: Match with accent-removed query
                    .should(Query.of(q -> q.match(m -> m
                            .field(field)
                            .query(normalized)
                            .fuzziness(FUZZINESS_AUTO)
                            .prefixLength(1)  // Reduced for better Vietnamese matching
                            .maxExpansions(100)  // Increased for accent variations
                    )))
                    // Option 2: Match original value (in case ES has it without accents)
                    .should(Query.of(q -> q.match(m -> m
                            .field(field)
                            .query(value.toLowerCase().trim())
                            .fuzziness(FUZZINESS_AUTO)
                            .prefixLength(1)
                            .maxExpansions(100)
                    )))
                    .minimumShouldMatch("1")
            );

            queries.add(Query.of(q -> q.bool(boolQuery)));
        }
    }

    /**
     * HELPER: Remove Vietnamese accents from text
     *
     * This is a workaround since ES doesn't have asciifolding configured.
     * For production, should configure proper Vietnamese analyzer in ES.
     */
    private String removeVietnameseAccents(String text) {
        if (text == null || text.isBlank()) {
            return text;
        }

        // Vietnamese character mappings
        text = text.replaceAll("[àáạảãâầấậẩẫăằắặẳẵ]", "a");
        text = text.replaceAll("[èéẹẻẽêềếệểễ]", "e");
        text = text.replaceAll("[ìíịỉĩ]", "i");
        text = text.replaceAll("[òóọỏõôồốộổỗơờớợởỡ]", "o");
        text = text.replaceAll("[ùúụủũưừứựửữ]", "u");
        text = text.replaceAll("[ỳýỵỷỹ]", "y");
        text = text.replaceAll("đ", "d");

        text = text.replaceAll("[ÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴ]", "A");
        text = text.replaceAll("[ÈÉẸẺẼÊỀẾỆỂỄ]", "E");
        text = text.replaceAll("[ÌÍỊỈĨ]", "I");
        text = text.replaceAll("[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]", "O");
        text = text.replaceAll("[ÙÚỤỦŨƯỪỨỰỬỮ]", "U");
        text = text.replaceAll("[ỲÝỴỶỸ]", "Y");
        text = text.replaceAll("Đ", "D");

        return text;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALIDATION & PAGINATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * VALIDATE PAGINATION LIMITS
     */
    private void validatePaginationLimits(SearchProfileRequest request) {
        if (request.getPageSize() > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Page size cannot exceed %d", MAX_PAGE_SIZE)
            );
        }

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
     * BUILD PAGEABLE
     */
    private Pageable buildPageable(SearchProfileRequest request) {
        String sortBy = request.getSortBy() != null ? request.getSortBy() : "updated_at";
        String sortOrder = request.getSortOrder() != null ? request.getSortOrder() : "desc";

        Sort sort = sortOrder.equalsIgnoreCase("asc")
                ? Sort.by(sortBy).ascending()
                : Sort.by(sortBy).descending();

        return PageRequest.of(
                request.getPage(),
                Math.min(request.getPageSize(), MAX_PAGE_SIZE),
                sort
        );
    }
}