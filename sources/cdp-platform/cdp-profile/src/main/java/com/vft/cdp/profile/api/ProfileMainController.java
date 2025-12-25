package com.vft.cdp.profile.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.domain.model.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.api.request.UpdateProfileRequest;
import com.vft.cdp.profile.api.response.DeleteProfileResponse;
import com.vft.cdp.profile.api.response.ProfileListResponse;
import com.vft.cdp.profile.api.response.ProfileResponse;
import com.vft.cdp.profile.application.ProfileService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import com.vft.cdp.profile.application.exception.ProfileNotFoundException;


import java.util.Optional;

/**
 * Profile REST API Controller
 */
@Slf4j
@RestController
@RequestMapping("/v1/profiles")
@RequiredArgsConstructor
public class ProfileMainController {

    private final ProfileService profileService;

    /*
     * GET single profile by tenant_id and user_id
     *
     * Example: GET /v1/profiles/tenant_1/user_123
     */
    @GetMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<ProfileResponse> getProfile(
            @PathVariable ("tenantId") String tenantId,
            @PathVariable ("appId") String appId,
            @PathVariable ("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("GET profile: tenant={}, userId={}", tenantId, userId);

        Optional<EnrichedProfile> profileOpt = profileService.getProfile(tenantId,appId, userId);

        return profileOpt
                .map(ProfileResponse::fromEnrichedProfile)
                .map(ResponseEntity::ok)
                .orElseGet(() -> {
                    log.warn("Profile not found: tenant={}, userId={}", tenantId, userId);
                    return ResponseEntity.notFound().build();
                });
    }

    /*
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

        Page<EnrichedProfile> profilePage = profileService.searchProfiles(request);

        // Convert to response
        Page<ProfileResponse> responsePage = profilePage.map(ProfileResponse::fromEnrichedProfile);
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
            @PathVariable ("tenantId") String tenantId,
            @PathVariable ("appId") String appId,
            @PathVariable ("userId") String userId,
            @Valid @RequestBody UpdateProfileRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        try {
            // Update profile
            EnrichedProfile updated = profileService.updateProfile(
                    tenantId, appId, userId, request);

            // Convert to response
            ProfileResponse response = ProfileResponse.fromEnrichedProfile(updated);

            log.info("✅ Profile updated successfully");

            return ResponseEntity.ok(response);

        } catch (ProfileNotFoundException ex) {
            log.warn("Profile not found: {}", ex.getMessage());
            return ResponseEntity.notFound().build();

        } catch (Exception ex) {
            log.error("Error updating profile", ex);
            return ResponseEntity.status(500).build();
        }
    }

    // ========================================
    // DELETE PROFILE
    // ========================================

    /**
     * Delete profile
     *
     * ENDPOINT: DELETE /v1/profiles/{tenantId}/{appId}/{userId}
     *
     * STRATEGY:
     * - Check profile exists
     * - Hard delete from Elasticsearch
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
            @PathVariable ("tenantId") String tenantId,
            @PathVariable ("appId") String appId,
            @PathVariable ("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        log.info("🗑️  DELETE profile: tenant={}, app={}, userId={}", tenantId, appId, userId);

        try {
            // Delete profile
            String deletedProfileId = profileService.deleteProfile(tenantId, appId, userId);

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