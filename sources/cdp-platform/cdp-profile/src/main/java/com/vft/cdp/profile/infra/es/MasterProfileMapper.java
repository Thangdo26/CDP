package com.vft.cdp.profile.infra.es;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.MasterProfile;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Mapper: EnrichedProfile → MasterProfile
 */
public final class MasterProfileMapper {

    private MasterProfileMapper() {
        throw new AssertionError("Utility class");
    }

    /**
     * Build master profile ID
     */
    public static String buildMasterProfileId() {
        return "mp_" + UUID.randomUUID();
    }

    /**
     * Merge multiple profiles into one master profile
     */
    public static MasterProfile mergeProfiles(List<EnrichedProfile> profiles) {
        if (profiles == null || profiles.isEmpty()) {
            throw new IllegalArgumentException("Profiles list cannot be empty");
        }

        List<EnrichedProfile> sorted = profiles.stream()
                .sorted(Comparator.comparing(EnrichedProfile::getUpdatedAt).reversed())
                .collect(Collectors.toList());

        EnrichedProfile latestProfile = sorted.get(0);

        return MasterProfile.builder()
                .profileId(buildMasterProfileId())
                .tenantId(latestProfile.getTenantId())
                .appId(extractUniqueAppIds(profiles))
                .status(MasterProfile.ProfileStatus.ACTIVE)
                .anonymous(false)
                .deviceId(extractDeviceIds(profiles))
                .mergedIds(extractMergedIds(profiles))
                .traits(buildMasterTraits(profiles, latestProfile))
                .segments(new ArrayList<>())
                .scores(new HashMap<>())
                .consents(new HashMap<>())
                .metadata(buildMetadata(profiles))
                .build();
    }

    private static List<String> extractUniqueAppIds(List<EnrichedProfile> profiles) {
        return profiles.stream()
                .map(EnrichedProfile::getAppId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    private static List<String> extractDeviceIds(List<EnrichedProfile> profiles) {
        return profiles.stream()
                .map(EnrichedProfile::getPlatforms)
                .filter(Objects::nonNull)
                .map(p -> p.getDevice())
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    private static List<String> extractMergedIds(List<EnrichedProfile> profiles) {
        return profiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());
    }

    /**
     * Build master traits
     */
    private static MasterProfile.MasterTraits buildMasterTraits(
            List<EnrichedProfile> profiles,
            EnrichedProfile latestProfile) {

        List<String> emails = profiles.stream()
                .map(p -> p.getTraits() != null ? p.getTraits().getEmail() : null)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        List<String> phones = profiles.stream()
                .map(p -> p.getTraits() != null ? p.getTraits().getPhone() : null)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        List<String> userIds = profiles.stream()
                .map(EnrichedProfile::getUserId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        var latestTraits = latestProfile.getTraits();

        return MasterProfile.MasterTraits.builder()
                .email(emails)
                .phone(phones)
                .userId(userIds)
                .firstName(latestTraits != null ? latestTraits.getFirstName() : null)
                .lastName(latestTraits != null ? latestTraits.getLastName() : null)
                .gender(latestTraits != null ? latestTraits.getGender() : null)
                .dob(latestTraits != null ? latestTraits.getDob() : null)
                .address(latestTraits != null ? latestTraits.getAddress() : null)
                .country("VN")
                .city(extractCity(latestTraits != null ? latestTraits.getAddress() : null))
                .build();
    }

    /**
     * Build metadata
     */
    private static MasterProfile.MasterMetadata buildMetadata(List<EnrichedProfile> profiles) {
        Instant createdAt = profiles.stream()
                .map(EnrichedProfile::getCreatedAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(Instant.now());

        Instant updatedAt = profiles.stream()
                .map(EnrichedProfile::getUpdatedAt)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        List<String> sourceSystems = profiles.stream()
                .map(EnrichedProfile::getAppId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        return MasterProfile.MasterMetadata.builder()
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .firstSeenAt(createdAt)
                .lastSeenAt(updatedAt)
                .sourceSystems(sourceSystems)
                .version(1)
                .build();
    }

    /* ================= CITY NORMALIZATION ================= */

    private static final Map<String, String> CITY_KEYWORDS = Map.ofEntries(
            Map.entry("ha noi", "Hanoi"),
            Map.entry("hanoi", "Hanoi"),

            Map.entry("ho chi minh", "Ho Chi Minh"),
            Map.entry("tp ho chi minh", "Ho Chi Minh"),
            Map.entry("tphcm", "Ho Chi Minh"),
            Map.entry("hcm", "Ho Chi Minh"),
            Map.entry("sai gon", "Ho Chi Minh"),

            Map.entry("da nang", "Da Nang"),
            Map.entry("hai phong", "Hai Phong"),
            Map.entry("can tho", "Can Tho"),

            Map.entry("ha giang", "Ha Giang"),
            Map.entry("cao bang", "Cao Bang"),
            Map.entry("bac kan", "Bac Kan"),
            Map.entry("lang son", "Lang Son"),
            Map.entry("tuyen quang", "Tuyen Quang"),
            Map.entry("thai nguyen", "Thai Nguyen"),
            Map.entry("phu tho", "Phu Tho"),
            Map.entry("bac giang", "Bac Giang"),
            Map.entry("quang ninh", "Quang Ninh"),
            Map.entry("lao cai", "Lao Cai"),
            Map.entry("yen bai", "Yen Bai"),
            Map.entry("dien bien", "Dien Bien"),
            Map.entry("lai chau", "Lai Chau"),
            Map.entry("son la", "Son La"),
            Map.entry("hoa binh", "Hoa Binh"),
            Map.entry("vinh phuc", "Vinh Phuc"),
            Map.entry("bac ninh", "Bac Ninh"),
            Map.entry("hai duong", "Hai Duong"),
            Map.entry("hung yen", "Hung Yen"),
            Map.entry("thai binh", "Thai Binh"),
            Map.entry("ha nam", "Ha Nam"),
            Map.entry("nam dinh", "Nam Dinh"),
            Map.entry("ninh binh", "Ninh Binh"),

            Map.entry("thanh hoa", "Thanh Hoa"),
            Map.entry("nghe an", "Nghe An"),
            Map.entry("ha tinh", "Ha Tinh"),
            Map.entry("quang binh", "Quang Binh"),
            Map.entry("quang tri", "Quang Tri"),
            Map.entry("thua thien hue", "Thua Thien Hue"),
            Map.entry("hue", "Thua Thien Hue"),
            Map.entry("quang nam", "Quang Nam"),
            Map.entry("quang ngai", "Quang Ngai"),
            Map.entry("binh dinh", "Binh Dinh"),
            Map.entry("phu yen", "Phu Yen"),
            Map.entry("khanh hoa", "Khanh Hoa"),
            Map.entry("ninh thuan", "Ninh Thuan"),
            Map.entry("binh thuan", "Binh Thuan"),

            Map.entry("kon tum", "Kon Tum"),
            Map.entry("gia lai", "Gia Lai"),
            Map.entry("dak lak", "Dak Lak"),
            Map.entry("dak nong", "Dak Nong"),
            Map.entry("lam dong", "Lam Dong"),

            Map.entry("binh phuoc", "Binh Phuoc"),
            Map.entry("tay ninh", "Tay Ninh"),
            Map.entry("binh duong", "Binh Duong"),
            Map.entry("dong nai", "Dong Nai"),
            Map.entry("ba ria vung tau", "Ba Ria - Vung Tau"),
            Map.entry("vung tau", "Ba Ria - Vung Tau"),
            Map.entry("long an", "Long An"),
            Map.entry("tien giang", "Tien Giang"),
            Map.entry("ben tre", "Ben Tre"),
            Map.entry("tra vinh", "Tra Vinh"),
            Map.entry("vinh long", "Vinh Long"),
            Map.entry("dong thap", "Dong Thap"),
            Map.entry("an giang", "An Giang"),
            Map.entry("kien giang", "Kien Giang"),
            Map.entry("hau giang", "Hau Giang"),
            Map.entry("soc trang", "Soc Trang"),
            Map.entry("bac lieu", "Bac Lieu"),
            Map.entry("ca mau", "Ca Mau")
    );

    private static String extractCity(String address) {
        if (address == null || address.isBlank()) {
            return null;
        }

        String normalized = address.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9\\s]", " ")
                .replaceAll("\\s+", " ")
                .trim();

        return CITY_KEYWORDS.entrySet().stream()
                .filter(e -> normalized.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    /* ================= DOCUMENT MAPPING ================= */

    public static MasterProfileDocument toDocument(MasterProfile master) {
        MasterProfileDocument doc = new MasterProfileDocument();

        doc.setId(master.getProfileId());
        doc.setProfileId(master.getProfileId());
        doc.setTenantId(master.getTenantId());
        doc.setAppId(master.getAppId());
        doc.setStatus(master.getStatus().name().toLowerCase());
        doc.setAnonymous(master.isAnonymous());
        doc.setDeviceId(master.getDeviceId());
        doc.setMergedIds(master.getMergedIds());
        doc.setSegments(master.getSegments());
        doc.setScores(master.getScores());

        if (master.getTraits() != null) {
            MasterProfileDocument.TraitsDocument traitsDoc = new MasterProfileDocument.TraitsDocument();
            traitsDoc.setEmail(master.getTraits().getEmail());
            traitsDoc.setPhone(master.getTraits().getPhone());
            traitsDoc.setUserId(master.getTraits().getUserId());
            traitsDoc.setFirstName(master.getTraits().getFirstName());
            traitsDoc.setLastName(master.getTraits().getLastName());
            traitsDoc.setGender(master.getTraits().getGender());
            traitsDoc.setDob(master.getTraits().getDob());
            traitsDoc.setCountry(master.getTraits().getCountry());
            traitsDoc.setCity(master.getTraits().getCity());
            traitsDoc.setAddress(master.getTraits().getAddress());
            traitsDoc.setLastPurchaseAmount(master.getTraits().getLastPurchaseAmount());
            traitsDoc.setLastPurchaseAt(master.getTraits().getLastPurchaseAt());
            doc.setTraits(traitsDoc);
        }

        if (master.getMetadata() != null) {
            MasterProfileDocument.MetadataDocument metaDoc = new MasterProfileDocument.MetadataDocument();
            metaDoc.setCreatedAt(master.getMetadata().getCreatedAt());
            metaDoc.setUpdatedAt(master.getMetadata().getUpdatedAt());
            metaDoc.setFirstSeenAt(master.getMetadata().getFirstSeenAt());
            metaDoc.setLastSeenAt(master.getMetadata().getLastSeenAt());
            metaDoc.setSourceSystems(master.getMetadata().getSourceSystems());
            metaDoc.setVersion(master.getMetadata().getVersion());
            doc.setMetadata(metaDoc);
        }

        return doc;
    }

    public static MasterProfile toDomain(MasterProfileDocument doc) {
        return null; // TODO
    }
}
