package com.vft.cdp.inbound.application;

import com.vft.cdp.inbound.domain.EventEnricher;
import com.vft.cdp.inbound.domain.EventValidator;
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
}
