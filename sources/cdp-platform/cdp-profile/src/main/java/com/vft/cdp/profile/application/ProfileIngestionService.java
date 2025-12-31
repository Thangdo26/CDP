package com.vft.cdp.profile.application;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import com.vft.cdp.profile.application.mapper.ProfileIngestionMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileIngestionService {

    private static final String PROFILE_RAW_TOPIC = "cdp.profiles.raw";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public String ingestProfile(ApiKeyAuthContext authContext, ProfileIngestionRequest req) {

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
                        log.info(" Sent profile to Kafka: topic={}, partition={}, offset={}",
                                PROFILE_RAW_TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        return UUID.randomUUID().toString();
    }
}