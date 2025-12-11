package com.vft.cdp.auth.infra;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "cdp.auth.apikey-cache")
public class ApiKeyCacheProperties {

    /**
     * TTL cache cho 1 API key (seconds).
     */
    private long ttlSeconds = 600;
}
