package com.vft.cdp.profile.infra.es;

import com.vft.cdp.common.profile.EnrichedProfile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProfileMapper {

    /**
     * Build document ID from tenantId and profileId
     * Made public so it can be used by repository
     */
    public static String buildId(String tenantId, String profileId) {
        return tenantId + "|" + profileId;
    }

    /**
     * Convert EnrichedProfile to ProfileDocument
     */
    public static ProfileDocument toDocument(EnrichedProfile profile) {
        ProfileDocument doc = new ProfileDocument();

        doc.setId(buildId(profile.getTenantId(), profile.getProfileId()));
        doc.setTenantId(profile.getTenantId());
        doc.setProfileId(profile.getProfileId());
        doc.setAppId(profile.getAppId());
        doc.setStatus(profile.getStatus());
        doc.setAnonymous(profile.getAnonymous());
        doc.setIdentities(profile.getIdentities());
        doc.setTraits(profile.getTraits());
        doc.setSegments(profile.getSegments());
        doc.setScores(profile.getScores());

        // Map consents
        if (profile.getConsents() != null) {
            Map<String, ProfileDocument.ConsentInfoDocument> consentDocs = profile.getConsents().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> {
                                ProfileDocument.ConsentInfoDocument consentDoc = new ProfileDocument.ConsentInfoDocument();
                                consentDoc.setStatus(e.getValue().getStatus());
                                consentDoc.setUpdatedAt(e.getValue().getUpdatedAt());
                                consentDoc.setSource(e.getValue().getSource());
                                return consentDoc;
                            }
                    ));
            doc.setConsents(consentDocs);
        }

        // Map metadata
        if (profile.getMetadata() != null) {
            ProfileDocument.ProfileMetadataDocument meta = new ProfileDocument.ProfileMetadataDocument();
            meta.setCreatedAt(profile.getMetadata().getCreatedAt());
            meta.setUpdatedAt(profile.getMetadata().getUpdatedAt());
            meta.setFirstSeenAt(profile.getMetadata().getFirstSeenAt());
            meta.setLastSeenAt(profile.getMetadata().getLastSeenAt());
            meta.setSourceSystems(profile.getMetadata().getSourceSystems());
            meta.setVersion(profile.getMetadata().getVersion());
            doc.setMetadata(meta);
        }

        return doc;
    }

    /**
     * Convert ProfileDocument to EnrichedProfile
     */
    public static EnrichedProfile toDomain(ProfileDocument doc) {
        EnrichedProfile.EnrichedProfileBuilder builder = EnrichedProfile.builder();

        builder.profileId(doc.getProfileId());
        builder.tenantId(doc.getTenantId());
        builder.appId(doc.getAppId());
        builder.status(doc.getStatus());
        builder.anonymous(doc.getAnonymous());
        builder.identities(doc.getIdentities());
        builder.traits(doc.getTraits());
        builder.segments(doc.getSegments());
        builder.scores(doc.getScores());

        // Map consents back
        if (doc.getConsents() != null) {
            Map<String, EnrichedProfile.ConsentInfo> consents = doc.getConsents().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> EnrichedProfile.ConsentInfo.builder()
                                    .status(e.getValue().getStatus())
                                    .updatedAt(e.getValue().getUpdatedAt())
                                    .source(e.getValue().getSource())
                                    .build()
                    ));
            builder.consents(consents);
        }

        // Map metadata back
        if (doc.getMetadata() != null) {
            EnrichedProfile.ProfileMetadata metadata = EnrichedProfile.ProfileMetadata.builder()
                    .createdAt(doc.getMetadata().getCreatedAt())
                    .updatedAt(doc.getMetadata().getUpdatedAt())
                    .firstSeenAt(doc.getMetadata().getFirstSeenAt())
                    .lastSeenAt(doc.getMetadata().getLastSeenAt())
                    .sourceSystems(doc.getMetadata().getSourceSystems())
                    .version(doc.getMetadata().getVersion())
                    .build();
            builder.metadata(metadata);
        }

        // Partition key can be rebuilt
        builder.partitionKey(buildId(doc.getTenantId(), doc.getProfileId()));

        return builder.build();
    }
}