package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import com.vft.cdp.profile.api.request.UpdateProfileRequest;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.command.UpdateProfileCommand;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * COMMAND MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Converts API Request objects to Application Command objects.
 *
 * Responsibility: API Layer → Application Layer
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class CommandMapper {

    private CommandMapper() {
        throw new AssertionError("Utility class");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CREATE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ProfileIngestionRequest + Auth Context to CreateProfileCommand
     */
    public static CreateProfileCommand toCreateCommand(
            ProfileIngestionRequest request,
            ApiKeyAuthContext authContext) {

        return CreateProfileCommand.builder()
                // From auth context
                .tenantId(authContext.getTenantId())
                .appId(authContext.getAppId())

                // From request
                .userId(request.getUserId())
                .type(request.getType())
                .traits(mapTraitsToCommand(request.getTraits()))
                .platforms(mapPlatformsToCommand(request.getPlatforms()))
                .campaign(mapCampaignToCommand(request.getCampaign()))
                .metadata(request.getMetadata())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UPDATE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert UpdateProfileRequest to UpdateProfileCommand
     */
    public static UpdateProfileCommand toUpdateCommand(
            String tenantId,
            String appId,
            String userId,
            UpdateProfileRequest request) {

        return UpdateProfileCommand.builder()
                // Identity
                .tenantId(tenantId)
                .appId(appId)
                .userId(userId)

                // Data
                .type(request.getType())
                .traits(mapTraitsToCommand(request.getTraits()))
                .platforms(mapPlatformsToCommand(request.getPlatforms()))
                .campaign(mapCampaignToCommand(request.getCampaign()))
                .metadata(request.getMetadata())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED OBJECT MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static CreateProfileCommand.TraitsCommand mapTraitsToCommand(
            ProfileIngestionRequest.Traits traits) {

        if (traits == null) {
            return null;
        }

        return CreateProfileCommand.TraitsCommand.builder()
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

    private static CreateProfileCommand.TraitsCommand mapTraitsToCommand(
            UpdateProfileRequest.TraitsUpdate traits) {

        if (traits == null) {
            return null;
        }

        return CreateProfileCommand.TraitsCommand.builder()
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

    private static CreateProfileCommand.PlatformsCommand mapPlatformsToCommand(
            ProfileIngestionRequest.Platforms platforms) {

        if (platforms == null) {
            return null;
        }

        return CreateProfileCommand.PlatformsCommand.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static CreateProfileCommand.PlatformsCommand mapPlatformsToCommand(
            UpdateProfileRequest.PlatformsUpdate platforms) {

        if (platforms == null) {
            return null;
        }

        return CreateProfileCommand.PlatformsCommand.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static CreateProfileCommand.CampaignCommand mapCampaignToCommand(
            ProfileIngestionRequest.Campaign campaign) {

        if (campaign == null) {
            return null;
        }

        return CreateProfileCommand.CampaignCommand.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private static CreateProfileCommand.CampaignCommand mapCampaignToCommand(
            UpdateProfileRequest.CampaignUpdate campaign) {

        if (campaign == null) {
            return null;
        }

        return CreateProfileCommand.CampaignCommand.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }
}