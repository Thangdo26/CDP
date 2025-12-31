package com.vft.cdp.auth.security;

import com.vft.cdp.auth.infra.ApiKeyCacheProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration
@EnableConfigurationProperties(ApiKeyCacheProperties.class)
public class ApiKeyAuthConfig {

    // Ở đây chỉ còn bean chung, không đăng ký filter nữa
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
