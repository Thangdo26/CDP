package com.vft.cdp.profile.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.api.request.UpdateProfileRequest;
import com.vft.cdp.profile.api.response.ProfileListResponse;
import com.vft.cdp.profile.api.response.ProfileResponse;
import com.vft.cdp.profile.application.ProfileService;
import com.vft.cdp.profile.application.command.UpdateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.exception.ProfileNotFoundException;
import com.vft.cdp.profile.application.mapper.CommandMapper;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/v1/profiles")
@RequiredArgsConstructor
public class ProfileMainController {

    private final ProfileService profileService;

    // ═══════════════════════════════════════════════════════════════
    // GET PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * GET single profile by tenant_id, app_id, and user_id
     *
     * FLOW:
     * 1. Lookup profile_mapping: (tenant_id, app_id, user_id) → profile_id
     * 2. Fetch profile from profiles_thang_dev
     * 3. Return profile data
     *
     * Example: GET /v1/profiles/tenant_1/app_1/user_123
     *
     * Response includes profile_id showing which profile this user maps to
     */
    @GetMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<GetProfileResponse> getProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("📖 GET profile: tenant={}, app={}, userId={}", tenantId, appId, userId);

        Optional<ProfileDTO> profileOpt = profileService.getProfile(tenantId, appId, userId);

        return profileOpt
                .map(profile -> {
                    GetProfileResponse response = GetProfileResponse.builder()
                            .code(200)
                            .message("Profile found")
                            .profileId(extractProfileId(profile))
                            .profile(ProfileResponse.fromProfileDTO(profile))
                            .build();
                    return ResponseEntity.ok(response);
                })
                .orElseGet(() -> {
                    log.warn("❌ Profile not found: tenant={}, app={}, userId={}", tenantId, appId, userId);
                    GetProfileResponse response = GetProfileResponse.builder()
                            .code(404)
                            .message("Profile not found. No mapping exists for this user.")
                            .build();
                    return ResponseEntity.status(404).body(response);
                });
    }

    // ═══════════════════════════════════════════════════════════════
    // UPDATE PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * Update existing profile
     *
     * FLOW:
     * 1. Lookup profile_mapping: (tenant_id, app_id, user_id) → profile_id
     * 2. Fetch profile from profiles_thang_dev
     * 3. Apply updates (merge non-null fields)
     * 4. Save back to ES
     * 5. Invalidate cache for ALL users linked to this profile
     *
     * ENDPOINT: PUT /v1/profiles/{tenantId}/{appId}/{userId}
     *
     * NOTE: If multiple users are linked to the same profile,
     * updating via any user_id will update the shared profile.
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
    public ResponseEntity<UpdateProfileResponse> updateProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @Valid @RequestBody UpdateProfileRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("🔄 PUT profile: tenant={}, app={}, userId={}", tenantId, appId, userId);

        try {
            UpdateProfileCommand command = CommandMapper.toUpdateCommand(
                    tenantId, appId, userId, request);

            ProfileDTO updated = profileService.updateProfile(command);

            UpdateProfileResponse response = UpdateProfileResponse.builder()
                    .code(200)
                    .message("Profile updated successfully")
                    .profileId(extractProfileId(updated))
                    .profile(ProfileResponse.fromProfileDTO(updated))
                    .build();

            log.info("✅ Profile updated successfully");

            return ResponseEntity.ok(response);

        } catch (ProfileNotFoundException ex) {
            UpdateProfileResponse response = UpdateProfileResponse.builder()
                    .code(404)
                    .message("Profile not found. No mapping exists for this user.")
                    .build();

            return ResponseEntity.status(404).body(response);

        } catch (Exception ex) {
            log.error("❌ Error updating profile", ex);

            UpdateProfileResponse response = UpdateProfileResponse.builder()
                    .code(500)
                    .message("Internal server error: " + ex.getMessage())
                    .build();

            return ResponseEntity.status(500).body(response);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // DELETE PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * Delete profile
     *
     * FLOW:
     * 1. Lookup profile_mapping: (tenant_id, app_id, user_id) → profile_id
     * 2. Check how many mappings point to this profile
     * 3. If this is the ONLY mapping → Soft delete the profile
     * 4. If there are OTHER mappings → Only remove this mapping
     * 5. Remove the mapping entry
     * 6. Invalidate cache
     *
     * STRATEGY:
     * - deleteProfile=true (default): Delete profile if this is the last mapping
     * - deleteProfile=false: Only remove mapping, never delete the profile
     *
     * ENDPOINT: DELETE /v1/profiles/{tenantId}/{appId}/{userId}?deleteProfile=true
     *
     * EXAMPLE:
     * DELETE /v1/profiles/tenant-demo/app-demo/user_001
     *
     * RESPONSE:
     * {
     *   "code": 200,
     *   "message": "Profile deleted successfully",
     *   "mapping_removed": true,
     *   "profile_deleted": true,
     *   "profile_id": "idcard:012345678901",
     *   "remaining_mappings": 0
     * }
     */
    @DeleteMapping("/{tenantId}/{appId}/{userId}")
    public ResponseEntity<DeleteProfileResponse> deleteProfile(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @RequestParam(name = "deleteProfile", defaultValue = "true") boolean deleteProfileIfLast,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("🗑️ DELETE profile: tenant={}, app={}, userId={}, deleteIfLast={}",
                tenantId, appId, userId, deleteProfileIfLast);

        try {
            ProfileService.DeleteResult result = profileService.deleteProfile(
                    tenantId, appId, userId, deleteProfileIfLast);

            String message = result.isProfileDeleted()
                    ? "Profile deleted successfully"
                    : "Mapping removed. Profile kept (has other linked users).";

            DeleteProfileResponse response = DeleteProfileResponse.builder()
                    .code(200)
                    .message(message)
                    .mappingRemoved(result.isMappingRemoved())
                    .profileDeleted(result.isProfileDeleted())
                    .profileId(result.getProfileId())
                    .remainingMappings(result.getRemainingMappings())
                    .build();

            log.info("✅ Delete complete: {}", message);

            return ResponseEntity.ok(response);

        } catch (ProfileNotFoundException ex) {
            log.warn("❌ Profile not found: {}", ex.getMessage());

            DeleteProfileResponse response = DeleteProfileResponse.builder()
                    .code(404)
                    .message("Profile not found. No mapping exists for this user.")
                    .mappingRemoved(false)
                    .profileDeleted(false)
                    .build();

            return ResponseEntity.status(404).body(response);

        } catch (IllegalStateException ex) {
            log.warn("⚠️ Cannot delete: {}", ex.getMessage());

            DeleteProfileResponse response = DeleteProfileResponse.builder()
                    .code(400)
                    .message(ex.getMessage())
                    .mappingRemoved(false)
                    .profileDeleted(false)
                    .build();

            return ResponseEntity.status(400).body(response);

        } catch (Exception ex) {
            log.error("❌ Error deleting profile", ex);

            DeleteProfileResponse response = DeleteProfileResponse.builder()
                    .code(500)
                    .message("Internal server error: " + ex.getMessage())
                    .mappingRemoved(false)
                    .profileDeleted(false)
                    .build();

            return ResponseEntity.status(500).body(response);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // SEARCH PROFILES
    // ═══════════════════════════════════════════════════════════════

    /**
     * POST search profiles by criteria
     *
     * NOTE: Search operates directly on profiles_thang_dev,
     * not through the mapping layer.
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
        log.info("🔍 Search profiles: tenant={}, criteria={}", request.getTenantId(), request);

        Page<ProfileDTO> profilePage = profileService.searchProfiles(request);

        Page<ProfileResponse> responsePage = profilePage.map(ProfileResponse::fromProfileDTO);
        ProfileListResponse response = ProfileListResponse.fromPage(responsePage);

        log.info("✅ Found {} profiles", profilePage.getTotalElements());

        return ResponseEntity.ok(response);
    }

    // ═══════════════════════════════════════════════════════════════
    // GET LINKED ACCOUNTS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get all user identities linked to the same profile
     *
     * This is useful for showing "linked accounts" or
     * understanding which users share the same underlying profile.
     *
     * ENDPOINT: GET /v1/profiles/{tenantId}/{appId}/{userId}/linked
     *
     * EXAMPLE RESPONSE:
     * {
     *   "profile_id": "idcard:012345678901",
     *   "linked_accounts": [
     *     {"tenant_id": "tenant_1", "app_id": "app_1", "user_id": "user_001"},
     *     {"tenant_id": "tenant_1", "app_id": "app_2", "user_id": "user_abc"}
     *   ],
     *   "total_linked": 2
     * }
     */
    @GetMapping("/{tenantId}/{appId}/{userId}/linked")
    public ResponseEntity<LinkedAccountsResponse> getLinkedAccounts(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("appId") String appId,
            @PathVariable("userId") String userId,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        log.info("🔗 GET linked accounts: tenant={}, app={}, userId={}", tenantId, appId, userId);

        List<ProfileService.MappingInfo> linkedAccounts =
                profileService.getLinkedAccounts(tenantId, appId, userId);

        if (linkedAccounts.isEmpty()) {
            LinkedAccountsResponse response = LinkedAccountsResponse.builder()
                    .code(404)
                    .message("No mapping found for this user")
                    .build();
            return ResponseEntity.status(404).body(response);
        }

        String profileId = linkedAccounts.get(0).getProfileId();

        List<LinkedAccountInfo> accountInfos = linkedAccounts.stream()
                .map(m -> LinkedAccountInfo.builder()
                        .tenantId(m.getTenantId())
                        .appId(m.getAppId())
                        .userId(m.getUserId())
                        .build())
                .toList();

        LinkedAccountsResponse response = LinkedAccountsResponse.builder()
                .code(200)
                .message("Found " + linkedAccounts.size() + " linked account(s)")
                .profileId(profileId)
                .linkedAccounts(accountInfos)
                .totalLinked(linkedAccounts.size())
                .build();

        return ResponseEntity.ok(response);
    }

    // ═══════════════════════════════════════════════════════════════
    // HELPER METHODS
    // ═══════════════════════════════════════════════════════════════

    private String extractProfileId(ProfileDTO profile) {
        // Ưu tiên lấy từ idcard
        if (profile.getTraits() != null && profile.getTraits().getIdcard() != null) {
            String idcard = profile.getTraits().getIdcard();
            if (!idcard.isBlank()) {
                return idcard;  // ✅ RAW idcard
            }
        }

        // Fallback về userId (có thể là uuid:xxx)
        return profile.getUserId();
    }

    // ═══════════════════════════════════════════════════════════════
    // RESPONSE CLASSES
    // ═══════════════════════════════════════════════════════════════

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class GetProfileResponse {
        private int code;
        private String message;

        @JsonProperty("profile_id")
        private String profileId;

        private ProfileResponse profile;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class UpdateProfileResponse {
        private int code;
        private String message;

        @JsonProperty("profile_id")
        private String profileId;

        private ProfileResponse profile;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DeleteProfileResponse {
        private int code;
        private String message;

        @JsonProperty("mapping_removed")
        private boolean mappingRemoved;

        @JsonProperty("profile_deleted")
        private boolean profileDeleted;

        @JsonProperty("profile_id")
        private String profileId;

        @JsonProperty("remaining_mappings")
        private long remainingMappings;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class LinkedAccountsResponse {
        private int code;
        private String message;

        @JsonProperty("profile_id")
        private String profileId;

        @JsonProperty("linked_accounts")
        private List<LinkedAccountInfo> linkedAccounts;

        @JsonProperty("total_linked")
        private int totalLinked;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class LinkedAccountInfo {
        @JsonProperty("tenant_id")
        private String tenantId;

        @JsonProperty("app_id")
        private String appId;

        @JsonProperty("user_id")
        private String userId;
    }
}