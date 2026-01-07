package com.vft.cdp.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "com.vft.cdp")
@EnableJpaRepositories(basePackages = "com.vft.cdp")
@EntityScan(basePackages = "com.vft.cdp")
public class CdpApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdpApplication.class, args);
    }
}
