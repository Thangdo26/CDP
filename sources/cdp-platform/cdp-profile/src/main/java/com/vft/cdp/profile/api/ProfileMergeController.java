package com.vft.cdp.profile.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.api.request.AutoMergeRequest;
import com.vft.cdp.profile.api.request.ManualMergeRequest;
import com.vft.cdp.profile.api.response.AutoMergeResponse;
import com.vft.cdp.profile.api.response.MergeResponse;
import com.vft.cdp.profile.application.ProfileMergeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import com.vft.cdp.profile.api.response.MasterProfileResponse;
import com.vft.cdp.profile.application.dto.MasterProfileDTO;

import java.time.Instant;

/*
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MERGE API CONTROLLER - DOMAIN MASTER PROFILE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 *  Uses Domain MasterProfile (not cdp-common)
 *  Pure domain-driven design
 *
 * ENDPOINTS:
 * - POST /v1/profiles/merge_auto   - Automatic duplicate detection & merge
 * - POST /v1/profiles/merge_manual - Manual merge of specified profiles
 * - GET  /v1/profiles/master/{id}  - Get master profile by ID
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@RestController
@RequestMapping("/v1/profiles")
@RequiredArgsConstructor
public class ProfileMergeController {

    private final ProfileMergeService mergeService;
    
    @PostMapping("/merge_auto")
    public ResponseEntity<AutoMergeResponse> autoMerge(
            @Valid @RequestBody AutoMergeRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext) {

        long startTime = System.currentTimeMillis();

        try {
            ProfileMergeService.AutoMergeResult result = mergeService.autoMerge(
                    request.getTenantId(),
                    request.getMergeStrategy(),
                    request.getDryRun(),
                    request.getMaxGroups()
            );

            long processingTime = System.currentTimeMillis() - startTime;

            AutoMergeResponse response = AutoMergeResponse.builder()
                    .code(200)
                    .message(request.getDryRun()
                            ? "Duplicate detection completed (dry run)"
                            : "Auto merge completed successfully")
                    .tenantId(request.getTenantId())
                    .duplicateGroupsFound(result.getDuplicateGroupsFound())
                    .masterProfilesCreated(result.getMasterProfilesCreated())
                    .totalProfilesMerged(result.getTotalProfilesMerged())
                    .dryRun(request.getDryRun())
                    .mergeDetails(result.getMergeDetails())
                    .processingTimeMs(processingTime)
                    .processedAt(Instant.now())
                    .build();

            log.info(" AUTO MERGE completed: {} master profiles created in {}ms",
                    result.getMasterProfilesCreated(), processingTime);

            return ResponseEntity.ok(response);

        } catch (Exception ex) {
            log.error("❌ AUTO MERGE failed", ex);

            AutoMergeResponse errorResponse = AutoMergeResponse.builder()
                    .code(500)
                    .message("Auto merge failed: " + ex.getMessage())
                    .tenantId(request.getTenantId())
                    .duplicateGroupsFound(0)
                    .masterProfilesCreated(0)
                    .totalProfilesMerged(0)
                    .processedAt(Instant.now())
                    .build();

            return ResponseEntity.status(500).body(errorResponse);
        }
    }

    /*
     * MANUAL MERGE: Merge specified profiles
     *
     * ENDPOINT: POST /v1/profiles/merge_manual
     *
     * REQUEST BODY:
     * {
     *   "tenant_id": "tenant-demo",
     *   "profile_ids": [
     *     "tenant-demo|app-demo|thang_test",
     *     "tenant-demo|app-demo|thang_test2"
     *   ],
     *   "force_merge": false,
     *   "keep_originals": false
     * }
     *
     * RESPONSE:
     * {
     *   "code": 200,
     *   "message": "Profiles merged successfully",
     *   "master_profile_id": "mp_123e4567-e89b-12d3-a456-426614174000",
     *   "profiles_merged_count": 2,
     *   "merged_profile_ids": [
     *     "tenant-demo|app-demo|thang_test",
     *     "tenant-demo|app-demo|thang_test2"
     *   ],
     *   "merged_at": "2025-12-25T10:00:00Z"
     * }
     */
    @PostMapping("/merge_manual")
    public ResponseEntity<MergeResponse> manualMerge(
            @Valid @RequestBody ManualMergeRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext) {

        log.info("👤 MANUAL MERGE request received: tenant={}, profiles={}",
                request.getTenantId(),
                request.getProfileIds().size());

        try {
            if (request.getProfileIds().size() < 2) {
                return ResponseEntity.badRequest().body(
                        MergeResponse.builder()
                                .code(400)
                                .message("At least 2 profiles required for merge")
                                .build()
                );
            }

            MasterProfileDTO masterProfile = mergeService.manualMerge(
                    request.getTenantId(),
                    request.getProfileIds(),
                    request.getForceMerge(),
                    request.getKeepOriginals()
            );

            MergeResponse response = MergeResponse.builder()
                    .code(200)
                    .message("Profiles merged successfully")
                    .masterProfileId(masterProfile.getProfileId())
                    .profilesMergedCount(request.getProfileIds().size())
                    .mergedProfileIds(request.getProfileIds())
                    .mergedAt(Instant.now())
                    .build();

            log.info(" MANUAL MERGE completed: master_profile_id={}",
                    masterProfile.getProfileId());

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException ex) {
            log.warn("⚠️  MANUAL MERGE validation failed: {}", ex.getMessage());

            return ResponseEntity.badRequest().body(
                    MergeResponse.builder()
                            .code(400)
                            .message(ex.getMessage())
                            .build()
            );

        } catch (Exception ex) {
            log.error("❌ MANUAL MERGE failed", ex);

            return ResponseEntity.status(500).body(
                    MergeResponse.builder()
                            .code(500)
                            .message("Manual merge failed: " + ex.getMessage())
                            .build()
            );
        }
    }

    /*
     * GET MASTER PROFILE: Retrieve merged profile by ID
     *
     * ENDPOINT: GET /v1/profiles/master/{masterProfileId}
     *
     * EXAMPLE:
     * GET /v1/profiles/master/mp_123e4567-e89b-12d3-a456-426614174000
     */
    @GetMapping("/master/{masterProfileId}")
    public ResponseEntity<MasterProfileResponse> getMasterProfile(
            @PathVariable("masterProfileId") String masterProfileId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext) {

        log.info("🔍 GET MASTER PROFILE: id={}", masterProfileId);

        try {
            // Service returns DTO
            MasterProfileDTO masterProfileDTO = mergeService.getMasterProfile(masterProfileId);

            // Convert DTO → Response
            MasterProfileResponse response = MasterProfileResponse.fromDTO(masterProfileDTO);

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException ex) {
            log.warn("⚠️  Master profile not found: {}", ex.getMessage());
            return ResponseEntity.notFound().build();

        } catch (Exception ex) {
            log.error("❌ GET MASTER PROFILE failed", ex);
            return ResponseEntity.status(500).build();
        }
    }
}