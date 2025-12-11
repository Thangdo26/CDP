package com.vft.cdp.auth.infra;

import com.vft.cdp.auth.domain.ApiKeyRecord;

import java.util.Optional;

public interface ApiKeyRepository {

    Optional<ApiKeyRecord> findByKeyId(String keyId);
}
