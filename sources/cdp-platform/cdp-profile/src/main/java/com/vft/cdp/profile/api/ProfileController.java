package com.vft.cdp.profile.api;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.api.response.ProfileResponse;
import com.vft.cdp.profile.application.ProfileService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class ProfileController {

    @GetMapping("/v1/profiles/{tenantId}/{userId}")
    public ResponseEntity<ProfileResponse> getProfile(
            @PathVariable String tenantId,
            @PathVariable String userId) {

        Optional<EnrichedProfile> profileOpt = profileService.getProfile(tenantId, userId);

        return profileOpt
                .map(ProfileResponse::fromEnrichedProfile)  // Use mapper
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}