package com.vft.cdp.auth.infra;

import jakarta.persistence.*;
import lombok.Data;

import java.time.OffsetDateTime;

@Entity
@Table(name = "cdp_api_keys")
@Data
public class ApiKeyEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "key_id", nullable = false, unique = true)
    private String keyId;

    @Column(name = "raw_key")
    private String rawKey;

    @Column(name = "key_hash")
    private String keyHash;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "app_id", nullable = false)
    private String appId;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "revoked_at")
    private OffsetDateTime revokedAt;
}
