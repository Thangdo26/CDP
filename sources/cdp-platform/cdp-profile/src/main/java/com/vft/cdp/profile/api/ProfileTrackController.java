package com.vft.cdp.profile.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.ErrorCodes;
import com.vft.cdp.common.constant.ErrorMessages;
import com.vft.cdp.common.dto.ApiResponse;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import com.vft.cdp.profile.application.ProfileIngestionService;
import com.vft.cdp.profile.application.ProfileTrackService;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE TRACK CONTROLLER - UPDATED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * NEW FLOW:
 * 1. Check profile_mapping for (tenant_id, app_id, user_id)
 * 2. If exists → Find profile, compare updated_at
 * 3. If not exists → Merge Service (find by idcard)
 *
 * ENDPOINTS:
 * - POST /v1/profiles/track - Async mode (Kafka)
 * - POST /v1/profiles/track/sync - Sync mode (direct processing)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class ProfileTrackController {

    private final ProfileIngestionService ingestionService;

    /**
     * Async ingestion via Kafka
     * Use for high-volume production traffic
     */
    @PostMapping("/profiles/track")
    public ResponseEntity<ApiResponse> ingestProfile(
            @Valid @RequestBody ProfileIngestionRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("Track request (async): tenant={}, app={}, user={}",
                authContext.getTenantId(),
                authContext.getAppId(),
                request.getUserId());

        String requestId = ingestionService.ingestProfileAsync(authContext, request);

        return ResponseEntity.ok(
                ApiResponse.builder()
                        .code(ErrorCodes.OK)
                        .message(ErrorMessages.OK)
                        .transactionId(requestId)
                        .build()
        );
    }

    /**
     * Sync ingestion - Direct processing
     * Use for testing or low-volume scenarios
     * Returns detailed result of processing
     */
    @PostMapping("/profiles/track/sync")
    public ResponseEntity<TrackResponse> ingestProfileSync(
            @Valid @RequestBody ProfileIngestionRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("Track request (sync): tenant={}, app={}, user={}",
                authContext.getTenantId(),
                authContext.getAppId(),
                request.getUserId());

        ProfileTrackService.ProcessResult result =
                ingestionService.ingestProfileSync(authContext, request);

        TrackResponse response = TrackResponse.builder()
                .code(200)
                .message(result.getMessage())
                .action(result.getAction().name())
                .profileId(result.getProfileId())
                .mappingCreated(result.isMappingCreated())
                .processedAt(Instant.now())
                .build();

        return ResponseEntity.ok(response);
    }

    /**
     * Response for sync track endpoint
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TrackResponse {
        private int code;
        private String message;

        /**
         * Action taken: CREATED, UPDATED, SKIPPED, MAPPING_ONLY
         */
        private String action;

        /**
         * Profile ID in profiles_thang_dev
         */
        @JsonProperty("profile_id")
        private String profileId;

        /**
         * Whether a new mapping was created
         */
        @JsonProperty("mapping_created")
        private boolean mappingCreated;

        /**
         * Processing timestamp
         */
        @JsonProperty("processed_at")
        private Instant processedAt;
    }
}