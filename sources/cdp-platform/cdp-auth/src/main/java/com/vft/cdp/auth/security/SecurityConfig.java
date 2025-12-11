package com.vft.cdp.auth.security;

import com.vft.cdp.auth.application.ApiKeyAuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@RequiredArgsConstructor
public class SecurityConfig {

    private final ApiKeyAuthService apiKeyAuthService;

    // 1) Định nghĩa bean ApiKeyAuthFilter
    @Bean
    public ApiKeyAuthFilter apiKeyAuthFilter() {
        String[] publicPaths = new String[] {
                "/health",
                "/actuator/health",
                "/actuator/info"
        };
        return new ApiKeyAuthFilter(apiKeyAuthService, publicPaths);
    }

    // 2) Đăng ký filter vào SecurityFilterChain
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http,
                                           ApiKeyAuthFilter apiKeyAuthFilter) throws Exception {

        http.csrf(csrf -> csrf.disable());

        http.authorizeHttpRequests(auth -> auth
                .requestMatchers("/health", "/actuator/**").permitAll()
                .anyRequest().authenticated()
        );

        // Thêm filter API Key trước UsernamePasswordAuthenticationFilter
        http.addFilterBefore(apiKeyAuthFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
