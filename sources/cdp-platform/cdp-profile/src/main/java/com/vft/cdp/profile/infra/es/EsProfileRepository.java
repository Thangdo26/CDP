package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.domain.model.Profile;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final SpringDataProfileEsRepository springDataRepo;

    @Override
    public Optional<Profile> find(String tenantId, String profileId) {
        return springDataRepo
                .findByTenantIdAndProfileId(tenantId, profileId)
                .map(ProfileMapper::toDomain);
    }

    @Override
    public Profile save(Profile profile) {
        ProfileDocument doc = ProfileMapper.toDocument(profile);
        ProfileDocument saved = springDataRepo.save(doc);
        return ProfileMapper.toDomain(saved);
    }
}
