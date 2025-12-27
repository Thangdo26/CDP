package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.repository.MasterProfileRepository;
import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.infra.es.SpringDataMasterProfileRepository;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.MasterProfileMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ELASTICSEARCH MASTER PROFILE REPOSITORY IMPLEMENTATION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ FIXED: Changed findByMergedIdsContaining → findByMergedProfileIdsContaining
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsMasterProfileRepositoryImpl implements MasterProfileRepository {

    private final SpringDataMasterProfileRepository springDataRepo;
    private final ElasticsearchOperations esOps;

    @Override
    public MasterProfile save(MasterProfile masterProfile) {
        log.debug("Saving master profile: {}", masterProfile.getProfileId());
        MasterProfileDocument doc = MasterProfileMapper.toDocument(masterProfile);
        MasterProfileDocument saved = springDataRepo.save(doc);
        return MasterProfileMapper.toDomain(saved);
    }

    @Override
    public Optional<MasterProfile> findById(String masterId) {
        log.debug("Finding master profile by ID: {}", masterId);
        return springDataRepo.findByMasterId(masterId)
                .map(MasterProfileMapper::toDomain);
    }

    @Override
    public Optional<MasterProfile> findByMergedProfileId(String profileId) {
        log.debug("Finding master profile containing merged profile: {}", profileId);

        // ✅ FIXED: findByMergedProfileIdsContaining (matches MasterProfileDocument.mergedProfileIds field)
        List<MasterProfileDocument> docs = springDataRepo.findByMergedProfileIdsContaining(profileId);

        if (docs.isEmpty()) {
            return Optional.empty();
        }

        if (docs.size() > 1) {
            log.warn("Multiple master profiles found for profile {}, returning first", profileId);
        }

        return Optional.of(MasterProfileMapper.toDomain(docs.get(0)));
    }

    @Override
    public List<MasterProfile> findByTenantId(String tenantId) {
        log.debug("Finding master profiles by tenant: {}", tenantId);
        return springDataRepo.findByTenantId(tenantId).stream()
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());
    }

    @Override
    public List<MasterProfile> findByTenantIdAndAppId(String tenantId, String appId) {
        log.debug("Finding master profiles by tenant {} and app {}", tenantId, appId);
        return springDataRepo.findByTenantIdAndAppId(tenantId, appId).stream()
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());
    }

    @Override
    public void delete(String masterId) {
        log.debug("Deleting master profile: {}", masterId);
        springDataRepo.findByMasterId(masterId)
                .ifPresent(springDataRepo::delete);
    }

    @Override
    public boolean exists(String masterId) {
        log.debug("Checking if master profile exists: {}", masterId);
        return springDataRepo.findByMasterId(masterId).isPresent();
    }
}