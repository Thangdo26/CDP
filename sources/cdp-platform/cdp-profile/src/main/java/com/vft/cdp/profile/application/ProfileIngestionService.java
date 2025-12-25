package com.vft.cdp.profile.application;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.domain.model.RawProfile;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class ProfileIngestionService {

    private static final String PROFILE_RAW_TOPIC = "cdp.profiles.raw";
    private final KafkaTemplate<String, Object> kafkaTemplate;

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
        kafkaTemplate.send(PROFILE_RAW_TOPIC, kafkaKey, profile)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send profile to Kafka, topic={}, key={}",
                                PROFILE_RAW_TOPIC, kafkaKey, ex);
                    } else {
                        log.info("Sent profile to Kafka topic={}, key={}, partition={}, offset={}",
                                PROFILE_RAW_TOPIC,
                                kafkaKey,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                        log.info("📋 [Profile]: Send profile_raw successfully");
                    }
                });

        // 4. Return request ID
        return UUID.randomUUID().toString();
    }

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