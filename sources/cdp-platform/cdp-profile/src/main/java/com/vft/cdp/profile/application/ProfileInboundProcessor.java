package com.vft.cdp.profile.application;

import com.vft.cdp.common.exception.EventValidationException;
import com.vft.cdp.profile.domain.model.EnrichedProfile;
import com.vft.cdp.profile.domain.model.RawProfile;
import com.vft.cdp.profile.domain.ProfileEnricher;
import com.vft.cdp.profile.domain.ProfileValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileInboundProcessor {

    private static final String ENRICHED_TOPIC = "cdp.profiles.enriched";

    private final ProfileValidator validator;
    private final ProfileEnricher enricher;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = "${cdp.kafka.topics.profiles-raw:cdp.profiles.raw}",
            groupId = "${cdp.kafka.consumer.profile-inbound.group-id:cdp-profile-inbound}"
    )
    public void handleRawProfile(@Payload RawProfile profile) {
        if (profile == null) {
            log.warn("[profile-inbound] Received null RawProfile");
            return;
        }

        try {
            // 1. Validate
            validator.validate(profile);

            // 2. Enrich
            EnrichedProfile enriched = enricher.enrich(profile);

            // 3. Publish to enriched topic
            String key = enriched.getPartitionKey();
            kafkaTemplate.send(ENRICHED_TOPIC, key, enriched)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error(
                                    "[profile-inbound] ❌ Failed to publish EnrichedProfile | topic={} | key={}",
                                    ENRICHED_TOPIC,
                                    key,
                                    ex
                            );
                        } else {
                            log.info(
                                    "[profile-inbound] ✅ Published EnrichedProfile successfully"
                            );
                        }
                    });

        } catch (EventValidationException ex) {
            log.warn("[profile-inbound] ⚠️  Invalid profile tenant={}, user={}, reason={}",
                    profile.getTenantId(), profile.getUserId(), ex.getMessage());
        } catch (Exception ex) {
            log.error("[profile-inbound] ❌ Unexpected error", ex);
        }
    }
}