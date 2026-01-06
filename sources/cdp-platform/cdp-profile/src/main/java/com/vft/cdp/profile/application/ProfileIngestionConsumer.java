package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.mapper.ProfileCommandMapper;
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
 * No EnrichedProfile - convert directly to domain
 * Use ProfileService for business logic
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileIngestionConsumer {

    private final ProfileService profileService;
    private final ProfileCommandMapper commandMapper;

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
            // Use mapper
            CreateProfileCommand command = commandMapper.toCommand(rawProfile);
            ProfileDTO saved = profileService.createProfile(command);

            log.info("Profile saved to ES: {}|{}|{}",
                    saved.getTenantId(),
                    saved.getAppId(),
                    saved.getUserId());

        } catch (Exception ex) {
            log.error("❌ Failed to process RawProfile: tenant={}, app={}, user={}",
                    rawProfile.getTenantId(),
                    rawProfile.getAppId(),
                    rawProfile.getUserId(),
                    ex);
        }
    }
}