package com.vft.cdp.profile.infra.es.model;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MODEL IMPLEMENTATION (INFRASTRUCTURE ADAPTER)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * PATTERN: Adapter Pattern
 *
 * PURPOSE:
 * - Adapts ES ProfileDocument to ProfileModel interface
 * - Lightweight wrapper (no business logic)
 * - Infrastructure layer returns this to Application layer
 *
 * RESPONSIBILITY:
 * - Delegates all getters to underlying ProfileDocument
 * - Converts ES types to interface types
 * - No business logic, no validation
 *
 * WHY ADAPTER INSTEAD OF DOMAIN ENTITY?
 * - Domain entity (Profile) has business logic
 * - When loading from DB, we may not need business logic
 * - Adapter is lightweight, faster to create
 * - Application layer works with interface, doesn't care about implementation
 *
 * USAGE:
 * ```
 * ProfileDocument doc = esRepository.findById(id);
 * ProfileModel model = new ProfileModelImpl(doc);
 * return model;  // Application receives ProfileModel interface
 * ```
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@AllArgsConstructor
public class ProfileModelImpl implements ProfileModel {

    private final ProfileDocument document;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public String getTenantId() {
        return document.getTenantId();
    }

    @Override
    public String getAppId() {
        return document.getAppId();
    }

    @Override
    public String getUserId() {
        return document.getUserId();
    }

    @Override
    public String getType() {
        return document.getType();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public String getStatus() {
        return document.getStatus();
    }

    @Override
    public String getMergedToMasterId() {
        return document.getMergedToMasterId();
    }

    @Override
    public Instant getMergedAt() {
        return document.getMergedAt();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public TraitsModel getTraits() {
        ProfileDocument.Traits docTraits = document.getTraits();
        return docTraits != null ? new TraitsModelImpl(docTraits) : null;
    }

    @Override
    public PlatformsModel getPlatforms() {
        ProfileDocument.Platforms docPlatforms = document.getPlatforms();
        return docPlatforms != null ? new PlatformsModelImpl(docPlatforms) : null;
    }

    @Override
    public CampaignModel getCampaign() {
        ProfileDocument.Campaign docCampaign = document.getCampaign();
        return docCampaign != null ? new CampaignModelImpl(docCampaign) : null;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return document.getMetadata();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public Instant getCreatedAt() {
        return document.getCreatedAt();
    }

    @Override
    public Instant getUpdatedAt() {
        return document.getUpdatedAt();
    }

    @Override
    public Instant getFirstSeenAt() {
        return document.getFirstSeenAt();
    }

    @Override
    public Instant getLastSeenAt() {
        return document.getLastSeenAt();
    }

    @Override
    public Integer getVersion() {
        return document.getVersion();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED ADAPTERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @AllArgsConstructor
    private static class TraitsModelImpl implements TraitsModel {
        private final ProfileDocument.Traits traits;

        @Override
        public String getFullName() {
            return traits.getFullName();
        }

        @Override
        public String getFirstName() {
            return traits.getFirstName();
        }

        @Override
        public String getLastName() {
            return traits.getLastName();
        }

        @Override
        public String getIdcard() {
            return traits.getIdcard();
        }

        @Override
        public String getOldIdcard() {
            return traits.getOldIdcard();
        }

        @Override
        public String getPhone() {
            return traits.getPhone();
        }

        @Override
        public String getEmail() {
            return traits.getEmail();
        }

        @Override
        public String getGender() {
            return traits.getGender();
        }

        @Override
        public String getDob() {
            return traits.getDob();
        }

        @Override
        public String getAddress() {
            return traits.getAddress();
        }

        @Override
        public String getReligion() {
            return traits.getReligion();
        }
    }

    @AllArgsConstructor
    private static class PlatformsModelImpl implements PlatformsModel {
        private final ProfileDocument.Platforms platforms;

        @Override
        public String getOs() {
            return platforms.getOs();
        }

        @Override
        public String getDevice() {
            return platforms.getDevice();
        }

        @Override
        public String getBrowser() {
            return platforms.getBrowser();
        }

        @Override
        public String getAppVersion() {
            return platforms.getAppVersion();
        }
    }

    @AllArgsConstructor
    private static class CampaignModelImpl implements CampaignModel {
        private final ProfileDocument.Campaign campaign;

        @Override
        public String getUtmSource() {
            return campaign.getUtmSource();
        }

        @Override
        public String getUtmCampaign() {
            return campaign.getUtmCampaign();
        }

        @Override
        public String getUtmMedium() {
            return campaign.getUtmMedium();
        }

        @Override
        public String getUtmContent() {
            return campaign.getUtmContent();
        }

        @Override
        public String getUtmTerm() {
            return campaign.getUtmTerm();
        }

        @Override
        public String getUtmCustom() {
            return campaign.getUtmCustom();
        }
    }
}