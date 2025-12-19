package com.vft.cdp.inbound.application;

import com.vft.cdp.inbound.domain.EventEnricher;
import com.vft.cdp.inbound.domain.EventValidator;
import com.vft.cdp.inbound.domain.ProfileEnricher;
import com.vft.cdp.inbound.domain.ProfileValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InboundDomainConfig {

    @Bean
    public EventValidator eventValidator() {
        return new EventValidator();
    }

    @Bean
    public EventEnricher eventEnricher() {
        return new EventEnricher();
    }

    @Bean
    public ProfileValidator profileValidator() {
        return new ProfileValidator();
    }

    @Bean
    public ProfileEnricher profileEnricher() {
        return new ProfileEnricher();
    }
}
