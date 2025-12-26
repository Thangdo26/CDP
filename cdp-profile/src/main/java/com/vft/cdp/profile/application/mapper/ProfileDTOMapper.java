package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.model.ProfileModel;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DTO MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Maps ProfileModel (interface) to ProfileDTO.
 *
 * KEY DESIGN:
 * - Works with ProfileModel INTERFACE, not concrete implementation
 * - Doesn't care if model is Profile entity or ProfileModelImpl
 * - Pure application layer logic
 *
 * BENEFIT:
 * - Application layer doesn't depend on Domain or Infrastructure
 * - Can change database without changing this mapper
 * - Easy to test with mock ProfileModel
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileDTOMapper {

    private ProfileDTOMapper() {
        throw new AssertionError("Utility class");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MODEL → DTO (for output)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ProfileModel to ProfileDTO
     *
     * Works with ANY implementation of ProfileModel:
     * - Profile (domain entity)
     * - ProfileModelImpl (infrastructure adapter)
     * - Mock implementation (for testing)
     */
    public static ProfileDTO toDTO(ProfileModel model) {
        if (model == null) {
            return null;
        }

        return ProfileDTO.builder()
                // Identity
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())

                // Status
                .status(model.getStatus())
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())

                // Data
                .traits(mapTraits(model.getTraits()))
                .platforms(mapPlatforms(model.getPlatforms()))
                .campaign(mapCampaign(model.getCampaign()))
                .metadata(model.getMetadata())

                // Timestamps
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED OBJECT MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static ProfileDTO.TraitsDTO mapTraits(ProfileModel.TraitsModel traits) {
        if (traits == null) {
            return null;
        }

        return ProfileDTO.TraitsDTO.builder()
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .phone(traits.getPhone())
                .email(traits.getEmail())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .religion(traits.getReligion())
                .build();
    }

    private static ProfileDTO.PlatformsDTO mapPlatforms(ProfileModel.PlatformsModel platforms) {
        if (platforms == null) {
            return null;
        }

        return ProfileDTO.PlatformsDTO.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static ProfileDTO.CampaignDTO mapCampaign(ProfileModel.CampaignModel campaign) {
        if (campaign == null) {
            return null;
        }

        return ProfileDTO.CampaignDTO.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }
}