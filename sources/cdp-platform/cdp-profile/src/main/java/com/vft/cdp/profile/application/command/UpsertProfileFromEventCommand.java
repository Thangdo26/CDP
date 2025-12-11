package com.vft.cdp.profile.application.command;

import java.util.Map;

public record UpsertProfileFromEventCommand(
        String tenantId,
        String profileId,
        Map<String, Object> identifiers,
        Map<String, Object> traits
) { }
