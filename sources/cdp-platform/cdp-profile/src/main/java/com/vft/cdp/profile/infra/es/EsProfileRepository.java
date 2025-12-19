package com.vft.cdp.profile.infra.es;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.domain.model.Profile;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final SpringDataProfileRepository springDataRepo; // Auto-implemented by Spring

    @Override
    public Optional<EnrichedProfile> find(String tenantId, String profileId) {
        String id = ProfileMapper.buildId(tenantId, profileId);
        return springDataRepo.findById(id)  // ← Spring Data method
                .map(ProfileMapper::toDomain);
    }

    @Override
    public Profile save(Profile profile) {
        return null;
    }

    @Override
    public EnrichedProfile save(EnrichedProfile profile) {
        ProfileDocument doc = ProfileMapper.toDocument(profile);
        ProfileDocument saved = springDataRepo.save(doc); // ← Spring Data method
        return ProfileMapper.toDomain(saved);
    }
}