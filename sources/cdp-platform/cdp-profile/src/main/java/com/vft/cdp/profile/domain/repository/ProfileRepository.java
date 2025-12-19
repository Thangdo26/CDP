package com.vft.cdp.profile.domain.repository;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.domain.model.Profile;

import java.util.Optional;

public interface ProfileRepository {

    Optional<EnrichedProfile> find(String tenantId, String profileId);

    Profile save(Profile profile);

    EnrichedProfile save(EnrichedProfile profile);
}
