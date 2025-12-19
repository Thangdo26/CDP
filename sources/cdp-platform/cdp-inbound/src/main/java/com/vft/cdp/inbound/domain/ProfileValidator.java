package com.vft.cdp.inbound.domain;

import com.vft.cdp.common.exception.EventValidationException;
import com.vft.cdp.common.profile.RawProfile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class ProfileValidator {

    public void validate(RawProfile profile) {
        // Check tenant_id
        if (!StringUtils.hasText(profile.getTenantId())) {
            throw new EventValidationException("tenant_id is required");
        }

        // Check app_id
        if (!StringUtils.hasText(profile.getAppId())) {
            throw new EventValidationException("app_id is required");
        }

        // Check type
        if (!StringUtils.hasText(profile.getType())) {
            throw new EventValidationException("type is required");
        }

        // Check user_id
        if (!StringUtils.hasText(profile.getUserId())) {
            throw new EventValidationException("user_id is required");
        }

        // Check traits
        if (profile.getTraits() == null) {
            throw new EventValidationException("traits is required");
        }
    }
}