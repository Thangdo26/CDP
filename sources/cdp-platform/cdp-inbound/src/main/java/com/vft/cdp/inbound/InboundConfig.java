package com.vft.cdp.inbound;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
public class InboundConfig {
    // Hiện tại chỉ cần bật @EnableKafka, mọi bean dùng chung từ cdp-infra
}
