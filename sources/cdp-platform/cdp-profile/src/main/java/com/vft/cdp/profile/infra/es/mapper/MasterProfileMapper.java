package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.application.model.MasterProfileModel;
import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE INFRASTRUCTURE MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Maps between:
 * - MasterProfile (Domain) ↔ MasterProfileDocument (ES)
 * - MasterProfileModel (Interface) ↔ MasterProfileDocument (ES)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Component
public class MasterProfileInfraMapper {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN → DOCUMENT (for saving)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public MasterProfileDocument toDocument(MasterProfile master) {
        if (master == null) return null;

        String id = buildId(master.getTenantId(), master.getAppId(), master.getMasterId());

        return MasterProfileDocument.builder()
                .id(id)
                .tenantId(master.getTenantId())
                .appId(master.getAppId())
                .masterId(master.getMasterId())
                .mergedProfileIds(master.getMergedProfileIds())
                .mergeCount(master.getMergeCount())
                .traits(mapTraitsToDoc(master.getTraits()))
                .platforms(mapPlatformsToDoc(master.getPlatforms()))
                .campaign(mapCampaignToDoc(master.getCampaign()))
                .metadata(master.getMetadata())
                .createdAt(master.getCreatedAt())
                .updatedAt(master.getUpdatedAt())
                .firstSeenAt(master.getFirstSeenAt())
                .lastSeenAt(master.getLastSeenAt())
                .version(master.getVersion())
                .build();
    }

    public MasterProfileDocument toDocument(MasterProfileModel model) {
        if (model == null) return null;

        if (model instanceof MasterProfile) {
            return toDocument((MasterProfile) model);
        }

        String id = buildId(model.getTenantId(), model.getAppId(), model.getMasterId());

        return MasterProfileDocument.builder()
                .id(id)
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .masterId(model.getMasterId())
                .mergedProfileIds(model.getMergedProfileIds())
                .mergeCount(model.getMergeCount())
                .traits(mapTraitsToDoc(model.getTraits()))
                .platforms(mapPlatformsToDoc(model.getPlatforms()))
                .campaign(mapCampaignToDoc(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOCUMENT → DOMAIN (for business logic)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public MasterProfile toDomain(MasterProfileDocument doc) {
        if (doc == null) return null;

        return MasterProfile.builder()
                .tenantId(doc.getTenantId())
                .appId(doc.getAppId())
                .masterId(doc.getMasterId())
                .mergedProfileIds(doc.getMergedProfileIds() != null ? doc.getMergedProfileIds() : new ArrayList<>())
                .mergeCount(doc.getMergeCount() != null ? doc.getMergeCount() : 0)
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata() != null ? doc.getMetadata() : new HashMap<>())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .version(doc.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TRAITS MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private MasterProfileDocument.Traits mapTraitsToDoc(MasterProfile.Traits traits) {
        if (traits == null) return null;

        return MasterProfileDocument.Traits.builder()
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .phone(traits.getPhone())
                .email(traits.getEmail())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .religion(traits.getReligion())
                .build();
    }

    private MasterProfileDocument.Traits mapTraitsToDoc(MasterProfileModel.TraitsModel traits) {
        if (traits == null) return null;

        if (traits instanceof MasterProfile.Traits) {
            return mapTraitsToDoc((MasterProfile.Traits) traits);
        }

        return MasterProfileDocument.Traits.builder()
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .phone(traits.getPhone())
                .email(traits.getEmail())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .religion(traits.getReligion())
                .build();
    }

    private MasterProfile.Traits mapTraitsToDomain(MasterProfileDocument.Traits doc) {
        if (doc == null) return null;

        return MasterProfile.Traits.builder()
                .fullName(doc.getFullName())
                .firstName(doc.getFirstName())
                .lastName(doc.getLastName())
                .idcard(doc.getIdcard())
                .oldIdcard(doc.getOldIdcard())
                .phone(doc.getPhone())
                .email(doc.getEmail())
                .gender(doc.getGender())
                .dob(doc.getDob())
                .address(doc.getAddress())
                .religion(doc.getReligion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PLATFORMS MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private MasterProfileDocument.Platforms mapPlatformsToDoc(MasterProfile.Platforms platforms) {
        if (platforms == null) return null;

        return MasterProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private MasterProfileDocument.Platforms mapPlatformsToDoc(MasterProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;

        if (platforms instanceof MasterProfile.Platforms) {
            return mapPlatformsToDoc((MasterProfile.Platforms) platforms);
        }

        return MasterProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private MasterProfile.Platforms mapPlatformsToDomain(MasterProfileDocument.Platforms doc) {
        if (doc == null) return null;

        return MasterProfile.Platforms.builder()
                .os(doc.getOs())
                .device(doc.getDevice())
                .browser(doc.getBrowser())
                .appVersion(doc.getAppVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CAMPAIGN MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private MasterProfileDocument.Campaign mapCampaignToDoc(MasterProfile.Campaign campaign) {
        if (campaign == null) return null;

        return MasterProfileDocument.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private MasterProfileDocument.Campaign mapCampaignToDoc(MasterProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;

        if (campaign instanceof MasterProfile.Campaign) {
            return mapCampaignToDoc((MasterProfile.Campaign) campaign);
        }

        return MasterProfileDocument.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private MasterProfile.Campaign mapCampaignToDomain(MasterProfileDocument.Campaign doc) {
        if (doc == null) return null;

        return MasterProfile.Campaign.builder()
                .utmSource(doc.getUtmSource())
                .utmCampaign(doc.getUtmCampaign())
                .utmMedium(doc.getUtmMedium())
                .utmContent(doc.getUtmContent())
                .utmTerm(doc.getUtmTerm())
                .utmCustom(doc.getUtmCustom())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UTILITIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public String buildId(String tenantId, String appId, String masterId) {
        return tenantId + "|" + appId + "|" + masterId;
    }
}