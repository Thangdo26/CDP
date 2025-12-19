package com.vft.cdp.profile.infra.kafka;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.application.ProfileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProfileEventConsumer {

    private final ProfileService profileService;

    @KafkaListener(
            topics = "${cdp.kafka.topics.profiles-enriched:cdp.profiles.enriched}",
            groupId = "${cdp.kafka.consumer.profile.group-id:cdp-profile}"
    )
    public void consume(EnrichedProfile profile) {

        log.info("=".repeat(60));
        log.info("🎯 PROFILE CONSUMER STARTED");
        log.info("TenantId: {}, UserId: {}",
                profile.getTenantId(),
                profile.getUserId());

        try {
            profileService.saveEnrichedProfile(profile);
            log.info("✅ SUCCESS - Profile saved: {}", profile.getUserId());

        } catch (Exception ex) {
            log.error("❌ ERROR - Failed to process profile", ex);
        }

        log.info("=".repeat(60));
    }
}