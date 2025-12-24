package com.vft.cdp.profile.infra.es;

import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.vft.cdp.common.profile.EnrichedProfile;
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

    // ========== SEARCH ==========
    /*
     * Search profiles by criteria using Native Query
     *
     * WHY NATIVE QUERY?
     * - Criteria API doesn't support full-text search on fields with spaces
     * - Native Query supports match, term, fuzzy queries
     * - More flexible for complex queries
     *
     * QUERY TYPES:
     * - term: Exact match (email, phone, idcard)
     * - match: Full-text search (full_name, address)
     *
     * @param request Search criteria
     * @return Page of matching profiles
     */
    @Override
    public Page<EnrichedProfile> search(SearchProfileRequest request) {
        log.info("🔎 Searching profiles: tenant={}, criteria={}",
                request.getTenantId(), request);

        // Build list of must queries
        List<Query> mustQueries = buildMustQueries(request);

        // Create bool query (all conditions must match - AND logic)
        BoolQuery boolQuery = BoolQuery.of(b -> {
            mustQueries.forEach(b::must);
            return b;
        });

        // Build pageable with sorting
        Pageable pageable = buildPageable(request);

        // Execute native query
        NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> q.bool(boolQuery))
                .withPageable(pageable)
                .build();

        SearchHits<ProfileDocument> searchHits = esOps.search(nativeQuery, ProfileDocument.class);

        // Convert ES documents to domain objects
        List<EnrichedProfile> profiles = searchHits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .toList();

        log.info("✅ Found {} profiles (total: {})",
                profiles.size(), searchHits.getTotalHits());

        // Return Spring Data Page
        return PageableExecutionUtils.getPage(
                profiles,
                pageable,
                searchHits::getTotalHits
        );
    }

    // ========== PRIVATE QUERY BUILDERS ==========
    /*
     * Build list of must queries (AND logic)
     *
     * QUERY BUILDING STRATEGY:
     * 1. Start with required tenant_id
     * 2. Add optional filters (user_id, app_id, type)
     * 3. Add traits filters if provided
     *
     * @param request Search criteria
     * @return List of Query objects
     */
    private List<Query> buildMustQueries(SearchProfileRequest request) {
        List<Query> mustQueries = new ArrayList<>();

        // ========== REQUIRED: tenant_id ==========
        mustQueries.add(Query.of(q -> q.term(t -> t
                .field("tenant_id")
                .value(request.getTenantId())
        )));

        // ========== OPTIONAL FILTERS ==========

        // user_id (exact match)
        if (request.getUserId() != null && !request.getUserId().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("user_id")
                    .value(request.getUserId())
            )));
        }

        // app_id (exact match)
        if (request.getAppId() != null && !request.getAppId().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("app_id")
                    .value(request.getAppId())
            )));
        }

        // type (exact match)
        if (request.getType() != null && !request.getType().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("type")
                    .value(request.getType())
            )));
        }

        // ========== TRAITS FILTERS ==========
        if (request.getTraits() != null) {
            addTraitsQueries(mustQueries, request.getTraits());
        }

        return mustQueries;
    }

    /*
     * Add traits search queries
     *
     * FIELD TYPES & QUERY STRATEGY:
     *
     * KEYWORD FIELDS (exact match → term query):
     * - email, phone, idcard, old_idcard
     * - gender, dob, religion
     *
     * TEXT FIELDS (full-text search → match query):
     * - full_name, first_name, last_name
     * - address
     *
     * MATCH QUERY BENEFITS:
     * - Tokenization: "Nguyen Van A" → ["nguyen", "van", "a"]
     * - Relevance scoring
     * - Fuzzy matching (typo tolerance)
     *
     * @param mustQueries List to add queries to
     * @param traits Search criteria for traits
     */
    private void addTraitsQueries(List<Query> mustQueries,
                                  SearchProfileRequest.TraitsSearch traits) {

        // ========== EXACT MATCH FIELDS (keyword) ==========

        // Email
        if (traits.getEmail() != null && !traits.getEmail().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.email")
                    .value(traits.getEmail())
            )));
        }

        // Phone
        if (traits.getPhone() != null && !traits.getPhone().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.phone")
                    .value(traits.getPhone())
            )));
        }

        // ID card (CCCD)
        if (traits.getIdcard() != null && !traits.getIdcard().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.idcard")
                    .value(traits.getIdcard())
            )));
        }

        // Old ID card (CMND)
        if (traits.getOldIdcard() != null && !traits.getOldIdcard().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.old_idcard")
                    .value(traits.getOldIdcard())
            )));
        }

        // Gender
        if (traits.getGender() != null && !traits.getGender().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.gender")
                    .value(traits.getGender())
            )));
        }

        // Date of birth
        if (traits.getDob() != null && !traits.getDob().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.dob")
                    .value(traits.getDob())
            )));
        }

        // Religion
        if (traits.getReligion() != null && !traits.getReligion().isBlank()) {
            mustQueries.add(Query.of(q -> q.term(t -> t
                    .field("traits.religion")
                    .value(traits.getReligion())
            )));
        }

        // ========== FULL-TEXT SEARCH FIELDS (text) ==========

        // Full name (MATCH query - supports spaces & fuzzy)
        // Examples that will match:
        // - "Nguyen Van A" (exact)
        // - "Nguyen" (partial)
        // - "Van A" (partial)
        // - "Nguen Van A" (typo with fuzzy)
        if (traits.getFullName() != null && !traits.getFullName().isBlank()) {
            mustQueries.add(Query.of(q -> q.match(m -> m
                    .field("traits.full_name")
                    .query(traits.getFullName())
                    .fuzziness("AUTO")  // Tolerates 1-2 char typos
            )));
        }

        // First name
        if (traits.getFirstName() != null && !traits.getFirstName().isBlank()) {
            mustQueries.add(Query.of(q -> q.match(m -> m
                    .field("traits.first_name")
                    .query(traits.getFirstName())
            )));
        }

        // Last name
        if (traits.getLastName() != null && !traits.getLastName().isBlank()) {
            mustQueries.add(Query.of(q -> q.match(m -> m
                    .field("traits.last_name")
                    .query(traits.getLastName())
            )));
        }

        // Address (MATCH query - supports partial address)
        // Examples: "Ha Noi", "Quan 1", "District 7"
        if (traits.getAddress() != null && !traits.getAddress().isBlank()) {
            mustQueries.add(Query.of(q -> q.match(m -> m
                    .field("traits.address")
                    .query(traits.getAddress())
            )));
        }
    }

    /*
     * Build pageable with sorting
     *
     * DEFAULT SORT: updated_at DESC (newest first)
     *
     * SUPPORTED SORT FIELDS:
     * - user_id
     * - created_at
     * - updated_at
     * - first_seen_at
     * - last_seen_at
     *
     * @param request Search request with pagination params
     * @return Pageable object
     */
    private Pageable buildPageable(SearchProfileRequest request) {
        String sortBy = request.getSortBy() != null ? request.getSortBy() : "updated_at";
        String sortOrder = request.getSortOrder() != null ? request.getSortOrder() : "desc";

        Sort sort = sortOrder.equalsIgnoreCase("asc")
                ? Sort.by(sortBy).ascending()
                : Sort.by(sortBy).descending();

        return PageRequest.of(
                request.getPage(),
                request.getPageSize(),
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