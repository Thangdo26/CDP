package com.vft.cdp.profile.application.dto;

import com.vft.cdp.profile.domain.model.Profile;

import java.time.Instant;
import java.util.Map;

public record ProfileDto(
        String tenantId,
        String profileId,
        Map<String, Object> identifiers,
        Map<String, Object> traits,
        String status,
        Instant createdAt,
        Instant updatedAt
) {
    public static ProfileDto fromDomain(Profile profile) {
        return new ProfileDto(
                profile.getTenantId(),
                profile.getProfileId(),
                profile.getIdentifiers(),
                profile.getTraits(),
                profile.getStatus(),
                profile.getCreatedAt(),
                profile.getUpdatedAt()
        );
    }
}
