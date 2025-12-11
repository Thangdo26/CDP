package com.vft.cdp.profile.api.response;

import com.vft.cdp.profile.application.dto.ProfileDto;

import java.time.Instant;
import java.util.Map;

public record ProfileResponse(
        String tenantId,
        String profileId,
        Map<String, Object> identifiers,
        Map<String, Object> traits,
        String status,
        Instant createdAt,
        Instant updatedAt
) {
    public static ProfileResponse fromDto(ProfileDto dto) {
        return new ProfileResponse(
                dto.tenantId(),
                dto.profileId(),
                dto.identifiers(),
                dto.traits(),
                dto.status(),
                dto.createdAt(),
                dto.updatedAt()
        );
    }
}
