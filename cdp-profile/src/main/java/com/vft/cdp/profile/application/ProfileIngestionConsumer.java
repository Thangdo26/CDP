package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.mapper.ProfileCommandMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileIngestionConsumer {

    private final ProfileTrackService trackService;
    private final ProfileCommandMapper commandMapper;

    @KafkaListener(
            topics = "${cdp.kafka.topics.profiles-raw:cdp.profiles.raw}",
            groupId = "${cdp.kafka.consumer.profile-inbound.group-id:cdp-profile-inbound}"
    )
    public void handleRawProfile(@Payload RawProfile rawProfile) {

        if (rawProfile == null) {
            log.warn("⚠️ Received null RawProfile");
            return;
        }

        try {
            log.info("📥 Processing RawProfile: tenant={}, app={}, user={}, idcard={}",
                    rawProfile.getTenantId(),
                    rawProfile.getAppId(),
                    rawProfile.getUserId(),
                    rawProfile.getTraits() != null ? rawProfile.getTraits().getIdcard() : null);

            // Convert to command
            CreateProfileCommand command = commandMapper.toCommand(rawProfile);
            ProfileTrackService.ProcessResult result = trackService.processTrack(command);

            log.info("✅ Profile processed: action={}, profileId={}, mappingCreated={}",
                    result.getAction(),
                    result.getProfileId(),
                    result.isMappingCreated());

        } catch (Exception ex) {
            log.error("❌ Failed to process RawProfile: tenant={}, app={}, user={}",
                    rawProfile.getTenantId(),
                    rawProfile.getAppId(),
                    rawProfile.getUserId(),
                    ex);
        }
    }
}