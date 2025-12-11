package com.vft.cdp.profile.api.request;

import java.util.Map;

public record UpdateProfileTraitsRequest(
        Map<String, Object> identifiers,
        Map<String, Object> traits
) { }
