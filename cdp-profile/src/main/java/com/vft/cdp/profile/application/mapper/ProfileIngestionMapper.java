package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;

public final class ProfileIngestionMapper {

    private ProfileIngestionMapper() {
        // Utility class
    }

    // =========================
    // MAIN MAPPER
    // =========================
    public static RawProfile toRawProfile(
            String tenantId,
            String appId,
            ProfileIngestionRequest req
    ) {
        return RawProfile.builder()
                .tenantId(tenantId)
                .appId(appId)
                .type(req.getType())
                .userId(req.getUserId())
                .traits(toTraits(req.getTraits()))
                .platforms(toPlatforms(req.getPlatforms()))
                .campaign(toCampaign(req.getCampaign()))
                .metadata(req.getMetadata())
                .build();
    }

    // =========================
    // SUB-MAPPERS
    // =========================

    public static RawProfile.Traits toTraits(ProfileIngestionRequest.Traits reqTraits) {
        if (reqTraits == null) return null;

        return RawProfile.Traits.builder()
                .fullName(reqTraits.getFullName())
                .firstName(reqTraits.getFirstName())
                .lastName(reqTraits.getLastName())
                .idcard(reqTraits.getIdcard())
                .oldIdcard(reqTraits.getOldIdcard())
                .phone(reqTraits.getPhone())
                .email(reqTraits.getEmail())
                .gender(reqTraits.getGender())
                .dob(reqTraits.getDob())
                .address(reqTraits.getAddress())
                .religion(reqTraits.getReligion())
                .build();
    }

    public static RawProfile.Platforms toPlatforms(ProfileIngestionRequest.Platforms reqPlatforms) {
        if (reqPlatforms == null) return null;

        return RawProfile.Platforms.builder()
                .os(reqPlatforms.getOs())
                .device(reqPlatforms.getDevice())
                .browser(reqPlatforms.getBrowser())
                .appVersion(reqPlatforms.getAppVersion())
                .build();
    }

    public static RawProfile.Campaign toCampaign(ProfileIngestionRequest.Campaign reqCampaign) {
        if (reqCampaign == null) return null;

        return RawProfile.Campaign.builder()
                .utmSource(reqCampaign.getUtmSource())
                .utmCampaign(reqCampaign.getUtmCampaign())
                .utmMedium(reqCampaign.getUtmMedium())
                .utmContent(reqCampaign.getUtmContent())
                .utmTerm(reqCampaign.getUtmTerm())
                .utmCustom(reqCampaign.getUtmCustom())
                .build();
    }
}
