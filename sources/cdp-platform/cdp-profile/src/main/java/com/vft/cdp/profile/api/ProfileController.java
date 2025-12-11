package com.vft.cdp.profile.api;

import com.vft.cdp.profile.api.request.UpdateProfileTraitsRequest;
import com.vft.cdp.profile.api.response.ProfileResponse;
import com.vft.cdp.profile.application.ProfileService;
import com.vft.cdp.profile.application.command.UpsertProfileFromEventCommand;
import com.vft.cdp.profile.application.dto.ProfileDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/profiles")
@RequiredArgsConstructor
public class ProfileController {

    private final ProfileService profileService;

    @GetMapping("/{tenantId}/{profileId}")
    public ResponseEntity<ProfileResponse> getProfile(
            @PathVariable String tenantId,
            @PathVariable String profileId
    ) {
        ProfileDto dto = profileService.getProfile(tenantId, profileId);
        return ResponseEntity.ok(ProfileResponse.fromDto(dto));
    }

    /**
     * Ví dụ API manual upsert profile (không bắt buộc nếu chỉ nhận từ event).
     */
    @PutMapping("/{tenantId}/{profileId}")
    public ResponseEntity<ProfileResponse> upsertProfile(
            @PathVariable String tenantId,
            @PathVariable String profileId,
            @RequestBody UpdateProfileTraitsRequest request
    ) {
        UpsertProfileFromEventCommand cmd = new UpsertProfileFromEventCommand(
                tenantId,
                profileId,
                request.identifiers(),
                request.traits()
        );

        ProfileDto dto = profileService.upsertProfile(cmd);
        return ResponseEntity.ok(ProfileResponse.fromDto(dto));
    }
}
