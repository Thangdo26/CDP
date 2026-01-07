package com.vft.cdp.profile.application;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.mapper.ProfileIngestionMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE INGESTION SERVICE - UPDATED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * NEW FLOW:
 * 1. Receive request from /v1/profiles/track
 * 2. Send to Kafka for async processing
 *    OR
 *    Process synchronously with ProfileTrackService (configurable)
 *
 * The actual business logic is now in ProfileTrackService
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileIngestionService {

    private static final String PROFILE_RAW_TOPIC = "cdp.profiles.raw";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProfileTrackService trackService;  // NEW

    /**
     * Async ingestion via Kafka (default mode)
     */
    public String ingestProfileAsync(ApiKeyAuthContext authContext, ProfileIngestionRequest req) {

        RawProfile rawProfile = ProfileIngestionMapper.toRawProfile(
                authContext.getTenantId(),
                authContext.getAppId(),
                req
        );

        String kafkaKey = rawProfile.getUserId();

        kafkaTemplate.send(PROFILE_RAW_TOPIC, kafkaKey, rawProfile)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("❌ Failed to send profile to Kafka: topic={}, key={}",
                                PROFILE_RAW_TOPIC, kafkaKey, ex);
                    } else {
                        log.info("Sent profile to Kafka: topic={}, partition={}, offset={}",
                                PROFILE_RAW_TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        return UUID.randomUUID().toString();
    }

    /**
     * Sync ingestion - Process immediately with new flow
     * Use this for testing or low-volume scenarios
     */
    public ProfileTrackService.ProcessResult ingestProfileSync(
            ApiKeyAuthContext authContext,
            ProfileIngestionRequest req
    ) {
        log.info("Sync ingestion: tenant={}, app={}, user={}",
                authContext.getTenantId(),
                authContext.getAppId(),
                req.getUserId());

        // Convert to command
        CreateProfileCommand command = toCommand(authContext, req);

        // Process with new flow
        ProfileTrackService.ProcessResult result = trackService.processTrack(command);

        log.info("Sync ingestion complete: action={}, profileId={}",
                result.getAction(),
                result.getProfileId());

        return result;
    }

    /**
     * Convert request to command
     */
    private CreateProfileCommand toCommand(
            ApiKeyAuthContext authContext,
            ProfileIngestionRequest req
    ) {
        CreateProfileCommand.TraitsCommand traits = null;
        if (req.getTraits() != null) {
            traits = CreateProfileCommand.TraitsCommand.builder()
                    .fullName(req.getTraits().getFullName())
                    .firstName(req.getTraits().getFirstName())
                    .lastName(req.getTraits().getLastName())
                    .idcard(req.getTraits().getIdcard())
                    .oldIdcard(req.getTraits().getOldIdcard())
                    .phone(req.getTraits().getPhone())
                    .email(req.getTraits().getEmail())
                    .gender(req.getTraits().getGender())
                    .dob(req.getTraits().getDob())
                    .address(req.getTraits().getAddress())
                    .religion(req.getTraits().getReligion())
                    .build();
        }

        CreateProfileCommand.PlatformsCommand platforms = null;
        if (req.getPlatforms() != null) {
            platforms = CreateProfileCommand.PlatformsCommand.builder()
                    .os(req.getPlatforms().getOs())
                    .device(req.getPlatforms().getDevice())
                    .browser(req.getPlatforms().getBrowser())
                    .appVersion(req.getPlatforms().getAppVersion())
                    .build();
        }

        CreateProfileCommand.CampaignCommand campaign = null;
        if (req.getCampaign() != null) {
            campaign = CreateProfileCommand.CampaignCommand.builder()
                    .utmSource(req.getCampaign().getUtmSource())
                    .utmCampaign(req.getCampaign().getUtmCampaign())
                    .utmMedium(req.getCampaign().getUtmMedium())
                    .utmContent(req.getCampaign().getUtmContent())
                    .utmTerm(req.getCampaign().getUtmTerm())
                    .utmCustom(req.getCampaign().getUtmCustom())
                    .build();
        }

        // Add updated_at to metadata if not present
        Map<String, Object> metadata = req.getMetadata() != null
                ? new HashMap<>(req.getMetadata())
                : new HashMap<>();

        if (!metadata.containsKey("updated_at")) {
            metadata.put("updated_at", java.time.Instant.now().toString());
        }

        return CreateProfileCommand.builder()
                .tenantId(authContext.getTenantId())
                .appId(authContext.getAppId())
                .userId(req.getUserId())
                .type(req.getType())
                .traits(traits)
                .platforms(platforms)
                .campaign(campaign)
                .metadata(metadata)
                .build();
    }
}