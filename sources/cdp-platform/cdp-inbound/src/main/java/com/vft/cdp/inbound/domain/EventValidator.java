package com.vft.cdp.inbound.domain;

import com.vft.cdp.common.event.IngestionEvent;
import com.vft.cdp.common.exception.EventValidationException;
import org.springframework.util.StringUtils;

public class EventValidator {

    public void validate(IngestionEvent event) {
        if (!StringUtils.hasText(event.getTenantId())) {
            throw new EventValidationException("tenantId is required");
        }
        if (!StringUtils.hasText(event.getAppId())) {
            throw new EventValidationException("appId is required");
        }
        if (!StringUtils.hasText(event.getType())) {
            throw new EventValidationException("type is required");
        }
        if (!StringUtils.hasText(event.getEventName())) {
            throw new EventValidationException("event name is required");
        }
        if (event.getEventTime() == null) {
            throw new EventValidationException("eventTime is required");
        }

        // Ít nhất phải có 1 trong 2: userId hoặc anonymousId
        if (!StringUtils.hasText(event.getUserId()) && !StringUtils.hasText(event.getAnonymousId())) {
            throw new EventValidationException("Either userId or anonymousId is required");
        }
    }
}
