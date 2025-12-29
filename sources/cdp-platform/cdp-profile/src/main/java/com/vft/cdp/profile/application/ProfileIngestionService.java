package com.vft.cdp.profile.application;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE INGESTION SERVICE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Publishes RawProfile to Kafka for async processing
 *
 * FLOW:
 * API Request → RawProfile → Kafka → ProfileInboundProcessor
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileIngestionService {

    private static final String PROFILE_RAW_TOPIC = "cdp.profiles.raw";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Ingest profile data to Kafka
     *
     * tenant_id and app_id are from auth context
     */
    public String ingestProfile(ApiKeyAuthContext authContext, ProfileIngestionRequest req) {

        log.info("📨 Ingesting profile to Kafka: tenant={}, app={}, user={}",
                authContext.getTenantId(), authContext.getAppId(), req.getUserId());

        // 1. Build RawProfile from cdp-common
        RawProfile rawProfile = RawProfile.builder()
                // From auth context (header)
                .tenantId(authContext.getTenantId())
                .appId(authContext.getAppId())

                // From request body
                .type(req.getType())
                .userId(req.getUserId())
                .traits(mapTraits(req.getTraits()))
                .platforms(mapPlatforms(req.getPlatforms()))
                .campaign(mapCampaign(req.getCampaign()))
                .metadata(req.getMetadata())
                .build();

        // 2. Kafka key = userId (for ordering)
        String kafkaKey = rawProfile.getUserId();

        // 3. Publish to Kafka
        kafkaTemplate.send(PROFILE_RAW_TOPIC, kafkaKey, rawProfile)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("❌ Failed to send profile to Kafka: topic={}, key={}",
                                PROFILE_RAW_TOPIC, kafkaKey, ex);
                    } else {
                        log.info(" Sent profile to Kafka: topic={}, partition={}, offset={}",
                                PROFILE_RAW_TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        // 4. Return request ID
        String requestId = UUID.randomUUID().toString();

        log.info("📋 Profile ingestion request created: requestId={}", requestId);

        return requestId;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MAPPERS (Request → RawProfile)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private RawProfile.Traits mapTraits(ProfileIngestionRequest.Traits reqTraits) {
        if (reqTraits == null) return null;

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

    private RawProfile.Platforms mapPlatforms(ProfileIngestionRequest.Platforms reqPlatforms) {
        if (reqPlatforms == null) return null;

        return RawProfile.Platforms.builder()
                .os(reqPlatforms.getOs())
                .device(reqPlatforms.getDevice())
                .browser(reqPlatforms.getBrowser())
                .appVersion(reqPlatforms.getAppVersion())
                .build();
    }

    private RawProfile.Campaign mapCampaign(ProfileIngestionRequest.Campaign reqCampaign) {
        if (reqCampaign == null) return null;

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