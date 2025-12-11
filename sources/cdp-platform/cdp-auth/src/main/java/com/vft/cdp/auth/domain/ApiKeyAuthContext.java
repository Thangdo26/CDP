package com.vft.cdp.auth.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class ApiKeyAuthContext {

    String tenantId;
    String appId;
    String keyId;
}
