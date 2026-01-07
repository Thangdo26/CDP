package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.model.ProfileModel;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DTO MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ProfileModel (interface) → ProfileDTO
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileDTOMapper {

    private ProfileDTOMapper() {
        throw new AssertionError("Utility class");
    }

    public static ProfileDTO toDTO(ProfileModel model) {
        if (model == null) return null;

        return ProfileDTO.builder()
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(model.getStatus())
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())
                .users(mapUsers(model.getUsers()))
                .traits(mapTraits(model.getTraits()))
                .platforms(mapPlatforms(model.getPlatforms()))
                .campaign(mapCampaign(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    private static List<ProfileDTO.UserIdentityDTO> mapUsers(
            List<? extends ProfileModel.UserIdentityModel> users) {
        if (users == null) {
            return null;
        }

        return users.stream()
                .map(u -> ProfileDTO.UserIdentityDTO.builder()
                        .appId(u.getAppId())
                        .userId(u.getUserId())
                        .build())
                .collect(Collectors.toList());
    }

    private static ProfileDTO.TraitsDTO mapTraits(ProfileModel.TraitsModel traits) {
        if (traits == null) return null;

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
        if (platforms == null) return null;

        return ProfileDTO.PlatformsDTO.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static ProfileDTO.CampaignDTO mapCampaign(ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;

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