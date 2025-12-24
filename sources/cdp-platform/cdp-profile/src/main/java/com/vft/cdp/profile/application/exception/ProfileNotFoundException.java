package com.vft.cdp.profile.application.exception;

/**
 * Exception thrown when profile is not found
 */
public class ProfileNotFoundException extends RuntimeException {

    public ProfileNotFoundException(String message) {
        super(message);
    }

    public ProfileNotFoundException(String tenantId, String appId, String userId) {
        super(String.format("Profile not found: %s|%s|%s", tenantId, appId, userId));
    }
}