package com.vft.cdp.profile.application.mapper;

import com.vft.cdp.profile.application.dto.MasterProfileDTO;
import com.vft.cdp.profile.application.model.MasterProfileModel;
import com.vft.cdp.profile.domain.MasterProfile;

import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE DTO MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Convert between:
 * - MasterProfile (Domain) → MasterProfileDTO (Application)
 * - MasterProfileModel (Interface) → MasterProfileDTO (Application)
 *
 * USAGE:
 * - Service layer returns DTO, not domain objects
 * - API layer works only with DTOs
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class MasterProfileDTOMapper {

    private MasterProfileDTOMapper() {
        throw new AssertionError("Utility class");
    }

    
    // DOMAIN → DTO
    

    /**
     * Convert MasterProfile domain entity to MasterProfileDTO
     */
    public static MasterProfileDTO toDTO(MasterProfile master) {
        if (master == null) return null;

        return MasterProfileDTO.builder()
                .profileId(master.getProfileId())
                .tenantId(master.getTenantId())
                .appId(master.getAppId())
                .status(master.getStatus())
                .anonymous(master.isAnonymous())
                .deviceId(master.getDeviceId())
                .mergedIds(master.getMergedIds())
                .traits(mapTraits(master.getTraits()))
                .segments(master.getSegments())
                .scores(master.getScores())
                .consents(master.getConsents() != null
                        ? master.getConsents().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey(),
                                e -> mapConsent(e.getValue())
                        ))
                        : null)
                .createdAt(master.getCreatedAt())
                .updatedAt(master.getUpdatedAt())
                .firstSeenAt(master.getFirstSeenAt())
                .lastSeenAt(master.getLastSeenAt())
                .sourceSystems(master.getSourceSystems())
                .version(master.getVersion())
                .build();
    }

    /**
     * Convert MasterProfileModel interface to MasterProfileDTO
     */
    public static MasterProfileDTO toDTO(MasterProfileModel model) {
        if (model == null) return null;

        return MasterProfileDTO.builder()
                .profileId(model.getProfileId())
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .status(model.getStatus())
                .anonymous(model.isAnonymous())
                .deviceId(model.getDeviceId())
                .mergedIds(model.getMergedIds())
                .traits(mapTraits(model.getTraits()))
                .segments(model.getSegments())
                .scores(model.getScores())
                .consents(model.getConsents() != null
                        ? model.getConsents().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey(),
                                e -> mapConsent(e.getValue())
                        ))
                        : null)
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .sourceSystems(model.getSourceSystems())
                .version(model.getVersion())
                .build();
    }

    
    // PRIVATE MAPPERS
    

    /**
     * Map traits from domain/model to DTO
     */
    private static MasterProfileDTO.MasterTraitsDTO mapTraits(
            MasterProfileModel.MasterTraitsModel traits) {

        if (traits == null) return null;

        return MasterProfileDTO.MasterTraitsDTO.builder()
                .email(traits.getEmail())
                .phone(traits.getPhone())
                .userId(traits.getUserId())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .country(traits.getCountry())
                .city(traits.getCity())
                .address(traits.getAddress())
                .lastPurchaseAmount(traits.getLastPurchaseAmount())
                .lastPurchaseAt(traits.getLastPurchaseAt())
                .build();
    }

    /**
     * Map consent from model to DTO
     */
    private static MasterProfileDTO.ConsentDTO mapConsent(
            MasterProfileModel.ConsentModel consent) {

        if (consent == null) return null;

        return MasterProfileDTO.ConsentDTO.builder()
                .status(consent.getStatus())
                .updatedAt(consent.getUpdatedAt())
                .source(consent.getSource())
                .build();
    }
}