package com.vft.cdp.auth.infra;

import com.vft.cdp.auth.domain.ApiKeyRecord;
import com.vft.cdp.auth.domain.ApiKeyStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class DbApiKeyRepository implements ApiKeyRepository {

    private final ApiKeyEntityJpaRepository jpaRepository;

    @Override
    public Optional<ApiKeyRecord> findByKeyId(String keyId) {
        return jpaRepository.findByKeyId(keyId)
                .map(this::mapToDomain);
    }

    private ApiKeyRecord mapToDomain(ApiKeyEntity entity) {
        return ApiKeyRecord.builder()
                .id(entity.getId())
                .keyId(entity.getKeyId())
                .rawKey(entity.getRawKey())
                .keyHash(entity.getKeyHash())
                .tenantId(entity.getTenantId())
                .appId(entity.getAppId())
                .status(ApiKeyStatus.valueOf(entity.getStatus()))
                .createdAt(entity.getCreatedAt())
                .revokedAt(entity.getRevokedAt())
                .build();
    }
}
