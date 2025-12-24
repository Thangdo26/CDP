package com.vft.cdp.profile.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response for delete profile operation
 */
public record DeleteProfileResponse(
        int code,
        String message,

        @JsonProperty("deleted_profile_id")
        String deletedProfileId
) {
}