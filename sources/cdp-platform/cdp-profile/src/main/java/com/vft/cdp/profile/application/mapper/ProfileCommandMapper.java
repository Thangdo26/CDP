package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import org.springframework.stereotype.Component;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE COMMAND MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Converts RawProfile → CreateProfileCommand
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Component
public class ProfileCommandMapper {

    public CreateProfileCommand toCommand(RawProfile raw) {
        if (raw == null) return null;

        return CreateProfileCommand.builder()
                .tenantId(raw.getTenantId())
                .appId(raw.getAppId())
                .userId(raw.getUserId())
                .type(raw.getType())
                .traits(mapTraits(raw.getTraits()))
                .platforms(mapPlatforms(raw.getPlatforms()))
                .campaign(mapCampaign(raw.getCampaign()))
                .metadata(raw.getMetadata())
                .build();
    }

    private CreateProfileCommand.TraitsCommand mapTraits(RawProfile.Traits traits) {
        if (traits == null) return null;

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

    private CreateProfileCommand.PlatformsCommand mapPlatforms(RawProfile.Platforms platforms) {
        if (platforms == null) return null;

        return CreateProfileCommand.PlatformsCommand.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private CreateProfileCommand.CampaignCommand mapCampaign(RawProfile.Campaign campaign) {
        if (campaign == null) return null;

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