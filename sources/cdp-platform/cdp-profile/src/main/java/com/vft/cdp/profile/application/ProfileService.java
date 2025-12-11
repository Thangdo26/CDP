package com.vft.cdp.profile.application;

import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.profile.application.command.UpsertProfileFromEventCommand;
import com.vft.cdp.profile.application.dto.ProfileDto;
import com.vft.cdp.profile.domain.model.Profile;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProfileService {

    private final ProfileRepository profileRepository;

    public ProfileDto getProfile(String tenantId, String profileId) {
        Profile profile = profileRepository
                .find(tenantId, profileId)
                .orElseThrow(() -> new IllegalArgumentException("Profile not found"));

        return ProfileDto.fromDomain(profile);
    }

    public ProfileDto upsertProfile(UpsertProfileFromEventCommand cmd) {
        Optional<Profile> existingOpt = profileRepository.find(cmd.tenantId(), cmd.profileId());

        Profile profile;
        if (existingOpt.isPresent()) {
            profile = existingOpt.get();
            profile.mergeIdentifiers(cmd.identifiers());
            profile.mergeTraits(cmd.traits());
        } else {
            profile = Profile.newProfile(
                    cmd.tenantId(),
                    cmd.profileId(),
                    cmd.identifiers(),
                    cmd.traits()
            );
        }

        Profile saved = profileRepository.save(profile);
        log.debug("Upserted profile tenant={} profileId={}", saved.getTenantId(), saved.getProfileId());
        return ProfileDto.fromDomain(saved);
    }

    /**
     * Adapter để được gọi từ inbound khi nhận EnrichedEvent.
     */
    public void upsertProfileFromEvent(EnrichedEvent event) {
        UpsertProfileFromEventCommand cmd = mapEventToCommand(event);
        upsertProfile(cmd);
    }

    private UpsertProfileFromEventCommand mapEventToCommand(EnrichedEvent event) {
        // TODO: ánh xạ lại cho đúng với structure thực tế của EnrichedEvent
        return new UpsertProfileFromEventCommand(
                event.getTenantId(),
                event.getProfileId(),
                event.getProperties(),
                event.getTraits()
        );
    }
}
