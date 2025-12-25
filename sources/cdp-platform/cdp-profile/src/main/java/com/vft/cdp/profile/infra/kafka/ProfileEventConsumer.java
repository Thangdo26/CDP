package com.vft.cdp.profile.infra.kafka;

import com.vft.cdp.profile.domain.model.EnrichedProfile;
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

        try {
            profileService.saveEnrichedProfile(profile);
            log.info("[ProfileConsumer]✅ SUCCESS - Profile saved successfully to ElasticSearch: {}", profile.getUserId());

        } catch (Exception ex) {
            log.error("❌ ERROR - Failed to process profile", ex);
        }
    }
}