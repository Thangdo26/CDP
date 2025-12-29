package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE INBOUND PROCESSOR
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Consumes RawProfile from Kafka → Save to Elasticsearch
 *
 * FLOW:
 * Kafka (RawProfile) → Convert to Command → ProfileService → ES
 *
 * ✅ No EnrichedProfile - convert directly to domain
 * ✅ Use ProfileService for business logic
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileInboundProcessor {

    private final ProfileService profileService;

    @KafkaListener(
            topics = "${cdp.kafka.topics.profiles-raw:cdp.profiles.raw}",
            groupId = "${cdp.kafka.consumer.profile-inbound.group-id:cdp-profile-inbound}"
    )
    public void handleRawProfile(@Payload RawProfile rawProfile) {

        if (rawProfile == null) {
            log.warn("⚠️  Received null RawProfile");
            return;
        }


        try {
            // 1. Convert RawProfile → CreateProfileCommand
            CreateProfileCommand command = convertToCommand(rawProfile);

            // 2. Use ProfileService to create/update
            ProfileDTO saved = profileService.createProfile(command);

        } catch (Exception ex) {
            log.error("❌ Failed to process RawProfile: tenant={}, app={}, user={}",
                    rawProfile.getTenantId(),
                    rawProfile.getAppId(),
                    rawProfile.getUserId(),
                    ex);
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CONVERTER: RawProfile → CreateProfileCommand
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private CreateProfileCommand convertToCommand(RawProfile raw) {
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