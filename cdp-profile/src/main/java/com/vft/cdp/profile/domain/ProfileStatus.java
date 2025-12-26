package com.vft.cdp.profile.domain;

/**
 * Profile Status Enum
 *
 * Tracks lifecycle of a profile:
 * - ACTIVE: Profile is active and can be merged
 * - MERGED: Profile has been merged into a master profile
 * - DELETED: Profile has been soft-deleted
 */
public enum ProfileStatus {
    /**
     * Profile is active and available for operations
     * Default status for new profiles
     */
    ACTIVE("active"),

    /**
     * Profile has been merged into a master profile
     * Should not be included in duplicate detection
     */
    MERGED("merged"),

    /**
     * Profile has been soft-deleted
     * Excluded from searches and operations
     */
    DELETED("deleted");

    private final String value;

    ProfileStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Convert string to enum
     */
    public static ProfileStatus fromValue(String value) {
        if (value == null) {
            return ACTIVE; // Default
        }

        for (ProfileStatus status : values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }

        return ACTIVE; // Fallback to default
    }

    @Override
    public String toString() {
        return value;
    }
}