package com.vft.cdp.ingestion.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.event.IngestionEvent;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.ingestion.application.dto.ProfileIngestionRequest;
import com.vft.cdp.ingestion.application.dto.TrackRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionService {

    private static final String TOPIC_INGESTION_RAW = "cdp.ingestion.raw";
    private static final String TOPIC_PROFILES_RAW = "cdp.profiles.raw";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // ========================================
    // EVENT INGESTION (Original method - kept as is)
    // ========================================

    /**
     * Ingest tracking event
     * Used for behavioral tracking, page views, clicks, etc.
     */
    public String ingestEvent(ApiKeyAuthContext authContext, TrackRequest req) {

        // 1. Build IngestionEvent từ request + auth context
        Instant eventTime = req.getEventTime() != null
                ? req.getEventTime().toInstant()
                : Instant.now();

        IngestionEvent event = IngestionEvent.builder()
                .tenantId(authContext.getTenantId())
                .appId(authContext.getAppId())
                .type(req.getType())
                .eventName(req.getEvent())
                .userId(req.getUserId())
                .anonymousId(req.getAnonymousId())
                .eventTime(eventTime)
                .properties(req.getProperties())
                .traits(req.getTraits())
                .context(req.getContext())
                .build();

        // Optional: dùng userId / anonymousId làm key để đảm bảo ordering theo user
        String kafkaKey = event.getUserId() != null
                ? event.getUserId()
                : event.getAnonymousId();

        // 3. Push vào Kafka
        kafkaTemplate.send(TOPIC_INGESTION_RAW, kafkaKey, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send event to Kafka, topic={}, key={}",
                                TOPIC_INGESTION_RAW, kafkaKey, ex);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Sent event to Kafka topic={}, key={}, partition={}, offset={}",
                                TOPIC_INGESTION_RAW,
                                kafkaKey,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        // 4. Tạm trả về một requestId sinh ngẫu nhiên (sau này có thể dùng traceId)
        return UUID.randomUUID().toString();
    }

    // ========================================
    // PROFILE INGESTION (New method - new schema)
    // ========================================

    /**
     * Ingest profile data (NEW SCHEMA)
     * tenant_id and app_id are injected from auth context (NOT from request body)
     */
    public String ingestProfile(ApiKeyAuthContext authContext, ProfileIngestionRequest req) {

        // 1. Build RawProfile - INJECT tenant_id and app_id from auth context
        RawProfile profile = RawProfile.builder()
                // FROM AUTH CONTEXT (header)
                .tenantId(authContext.getTenantId())  // ← From API key
                .appId(authContext.getAppId())        // ← From API key

                // FROM REQUEST BODY
                .type(req.getType())
                .userId(req.getUserId())
                .traits(mapTraits(req.getTraits()))
                .platforms(mapPlatforms(req.getPlatforms()))
                .campaign(mapCampaign(req.getCampaign()))
                .metadata(req.getMetadata())
                .build();

        // 2. Kafka key = userId for ordering guarantee
        String kafkaKey = profile.getUserId();

        // 3. Publish to Kafka
        kafkaTemplate.send(TOPIC_PROFILES_RAW, kafkaKey, profile)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("❌ Failed to send profile to Kafka, topic={}, key={}",
                                TOPIC_PROFILES_RAW, kafkaKey, ex);
                    } else {
                        log.info("✅ Sent profile to Kafka topic={}, key={}, partition={}, offset={}",
                                TOPIC_PROFILES_RAW,
                                kafkaKey,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                        log.info("📋 Profile: tenant_id={}, app_id={}, user_id={}, type={}",
                                profile.getTenantId(),
                                profile.getAppId(),
                                profile.getUserId(),
                                profile.getType());
                    }
                });

        // 4. Return request ID
        return UUID.randomUUID().toString();
    }

    // ========================================
    // PRIVATE MAPPING METHODS (for profile ingestion)
    // ========================================

    /**
     * Map request traits to RawProfile traits
     */
    private RawProfile.Traits mapTraits(ProfileIngestionRequest.Traits reqTraits) {
        if (reqTraits == null) {
            return null;
        }

        return RawProfile.Traits.builder()
                .fullName(reqTraits.getFullName())
                .firstName(reqTraits.getFirstName())
                .lastName(reqTraits.getLastName())
                .idcard(reqTraits.getIdcard())
                .oldIdcard(reqTraits.getOldIdcard())
                .phone(reqTraits.getPhone())
                .email(reqTraits.getEmail())
                .gender(reqTraits.getGender())
                .dob(reqTraits.getDob())
                .address(reqTraits.getAddress())
                .religion(reqTraits.getReligion())
                .build();
    }

    /**
     * Map request platforms to RawProfile platforms
     */
    private RawProfile.Platforms mapPlatforms(ProfileIngestionRequest.Platforms reqPlatforms) {
        if (reqPlatforms == null) {
            return null;
        }

        return RawProfile.Platforms.builder()
                .os(reqPlatforms.getOs())
                .device(reqPlatforms.getDevice())
                .browser(reqPlatforms.getBrowser())
                .appVersion(reqPlatforms.getAppVersion())
                .build();
    }

    /**
     * Map request campaign to RawProfile campaign
     */
    private RawProfile.Campaign mapCampaign(ProfileIngestionRequest.Campaign reqCampaign) {
        if (reqCampaign == null) {
            return null;
        }

        return RawProfile.Campaign.builder()
                .utmSource(reqCampaign.getUtmSource())
                .utmCampaign(reqCampaign.getUtmCampaign())
                .utmMedium(reqCampaign.getUtmMedium())
                .utmContent(reqCampaign.getUtmContent())
                .utmTerm(reqCampaign.getUtmTerm())
                .utmCustom(reqCampaign.getUtmCustom())
                .build();
    }
}