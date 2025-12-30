package com.vft.cdp.profile.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.api.request.UpdateProfileRequest;
import com.vft.cdp.profile.api.response.DeleteProfileResponse;
import com.vft.cdp.profile.api.response.ProfileListResponse;
import com.vft.cdp.profile.api.response.ProfileResponse;
import com.vft.cdp.profile.application.ProfileService;
import com.vft.cdp.profile.application.command.UpdateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.exception.ProfileNotFoundException;
import com.vft.cdp.profile.application.mapper.CommandMapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE REST API CONTROLLER - NO ENRICHED PROFILE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 *  Uses ProfileDTO instead of EnrichedProfile
 *  Pure domain-driven design
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@RestController
@RequestMapping("/v1/profiles")
@RequiredArgsConstructor
public class ProfileMainController {

    private final ProfileService profileService;

    /**
     * GET single profile by tenant_id, app_id, and user_id
     *
     * Example: GET /v1/profiles/tenant_1/app_1/user_123
     */
    @GetMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<ProfileResponse> getProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("GET profile: tenant={}, app={}, userId={}", tenantId, appId, userId);

        Optional<ProfileDTO> profileOpt = profileService.getProfile(tenantId, appId, userId);

        return profileOpt
                .map(ProfileResponse::fromProfileDTO)
                .map(ResponseEntity::ok)
                .orElseGet(() -> {
                    log.warn("Profile not found: tenant={}, app={}, userId={}", tenantId, appId, userId);
                    return ResponseEntity.notFound().build();
                });
    }

    /**
     * POST search profiles by criteria
     *
     * Example:
     * POST /v1/profiles/search
     * Body: {
     *   "tenant_id": "tenant_1",
     *   "traits": {
     *     "email": "user@example.com"
     *   },
     *   "page": 0,
     *   "page_size": 20
     * }
     */
    @PostMapping("/search")
    public ResponseEntity<ProfileListResponse> searchProfiles(
            @Valid @RequestBody SearchProfileRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        log.info("Search profiles: tenant={}, criteria={}", request.getTenantId(), request);

        Page<ProfileDTO> profilePage = profileService.searchProfiles(request);

        Page<ProfileResponse> responsePage = profilePage.map(ProfileResponse::fromProfileDTO);
        ProfileListResponse response = ProfileListResponse.fromPage(responsePage);

        log.info("Found {} profiles", profilePage.getTotalElements());

        return ResponseEntity.ok(response);
    }

    /**
     * Update existing profile
     *
     * ENDPOINT: PUT /v1/profiles/{tenantId}/{appId}/{userId}
     *
     * EXAMPLE:
     * PUT /v1/profiles/tenant-demo/app-demo/user_001
     * Body: {
     *   "traits": {
     *     "email": "newemail@example.com",
     *     "address": "Ho Chi Minh City"
     *   }
     * }
     */
    @PutMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<ProfileResponse> updateProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @Valid @RequestBody UpdateProfileRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        try {
            UpdateProfileCommand command = CommandMapper.toUpdateCommand(
                    tenantId, appId, userId, request);

            ProfileDTO updated = profileService.updateProfile(command);

            ProfileResponse response = ProfileResponse.fromProfileDTO(updated);

            log.info(" Profile updated successfully");

            return ResponseEntity.ok(response);

        } catch (ProfileNotFoundException ex) {
            log.warn("Profile not found: {}", ex.getMessage());
            return ResponseEntity.notFound().build();

        } catch (Exception ex) {
            log.error("Error updating profile", ex);
            return ResponseEntity.status(500).build();
        }
    }

    /**
     * Delete profile
     *
     * ENDPOINT: DELETE /v1/profiles/{tenantId}/{appId}/{userId}
     *
     * STRATEGY:
     * - Check profile exists
     * - Soft delete (mark as deleted)
     * - Return deleted profile ID
     *
     * EXAMPLE:
     * DELETE /v1/profiles/tenant-demo/app-demo/user_001
     *
     * RESPONSE:
     * {
     *   "code": 200,
     *   "message": "Profile deleted successfully",
     *   "deleted_profile_id": "tenant-demo|app-demo|user_001"
     * }
     */
    @DeleteMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<DeleteProfileResponse> deleteProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        log.info("🗑️  DELETE profile: tenant={}, app={}, userId={}", tenantId, appId, userId);

        try {
            profileService.deleteProfile(tenantId, appId, userId);

            String deletedProfileId = tenantId + "|" + appId + "|" + userId;

            DeleteProfileResponse response = new DeleteProfileResponse(
                    200,
                    "Profile deleted successfully",
                    deletedProfileId
            );

            log.info("Profile deleted successfully: {}", deletedProfileId);

            return ResponseEntity.ok(response);

        } catch (ProfileNotFoundException ex) {
            log.warn("Profile not found: {}", ex.getMessage());

            DeleteProfileResponse response = new DeleteProfileResponse(
                    404,
                    "Profile not found",
                    null
            );

            return ResponseEntity.status(404).body(response);

        } catch (Exception ex) {
            log.error("Error deleting profile", ex);

            DeleteProfileResponse response = new DeleteProfileResponse(
                    500,
                    "Internal server error",
                    null
            );

            return ResponseEntity.status(500).body(response);
        }
    }
}