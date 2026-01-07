package com.vft.cdp.auth.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiKeyRecord {

    private Long id;
    private String keyId;

    // DEV: có thể so sánh plaintext khi chưa set hash
    private String rawKey;

    // PROD: BCrypt hash của full API key
    private String keyHash;

    private String tenantId;
    private String appId;
    private ApiKeyStatus status;
    private OffsetDateTime createdAt;
    private OffsetDateTime revokedAt;
}
