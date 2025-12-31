package com.vft.cdp.profile.domain;

import lombok.Getter;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE STATUS ENUM
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Getter
public enum ProfileStatus {
    ACTIVE("active"),
    MERGED("merged"),
    DELETED("deleted");

    private final String value;

    ProfileStatus(String value) {
        this.value = value;
    }

    public static ProfileStatus fromValue(String value) {
        if (value == null) return ACTIVE;

        for (ProfileStatus status : values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }
        return ACTIVE;
    }
}