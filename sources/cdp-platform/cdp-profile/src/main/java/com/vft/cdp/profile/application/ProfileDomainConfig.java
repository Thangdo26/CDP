package com.vft.cdp.profile.application;
import com.vft.cdp.profile.domain.ProfileEnricher;
import com.vft.cdp.profile.domain.ProfileValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProfileDomainConfig {
    @Bean
    public ProfileValidator profileValidator() {
        return new ProfileValidator();
    }

    @Bean
    public ProfileEnricher profileEnricher() {
        return new ProfileEnricher();
    }
}
