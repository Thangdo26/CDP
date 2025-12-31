package com.vft.cdp.auth.infra;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ApiKeyEntityJpaRepository extends JpaRepository<ApiKeyEntity, Long> {

    Optional<ApiKeyEntity> findByKeyId(String keyId);
}
