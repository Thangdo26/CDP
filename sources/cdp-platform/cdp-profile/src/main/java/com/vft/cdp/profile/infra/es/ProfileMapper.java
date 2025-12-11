package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.domain.model.Profile;

public final class ProfileMapper {

    private ProfileMapper() {}

    public static ProfileDocument toDocument(Profile profile) {
        ProfileDocument doc = new ProfileDocument();
        doc.setId(buildId(profile.getTenantId(), profile.getProfileId()));
        doc.setTenantId(profile.getTenantId());
        doc.setProfileId(profile.getProfileId());
        doc.setIdentifiers(profile.getIdentifiers());
        doc.setTraits(profile.getTraits());
        doc.setStatus(profile.getStatus());
        doc.setCreatedAt(profile.getCreatedAt());
        doc.setUpdatedAt(profile.getUpdatedAt());
        return doc;
    }

    public static Profile toDomain(ProfileDocument doc) {
        if (doc == null) {
            return null;
        }
        return new Profile(
                doc.getTenantId(),
                doc.getProfileId(),
                doc.getIdentifiers(),
                doc.getTraits(),
                doc.getStatus(),
                doc.getCreatedAt(),
                doc.getUpdatedAt()
        );
    }

    private static String buildId(String tenantId, String profileId) {
        return tenantId + "|" + profileId;
    }
}
