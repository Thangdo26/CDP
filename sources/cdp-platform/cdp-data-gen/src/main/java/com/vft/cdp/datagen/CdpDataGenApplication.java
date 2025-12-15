package com.vft.cdp.datagen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

@SpringBootApplication(
        scanBasePackages = {
                "com.vft.cdp.datagen",
                "com.vft.cdp.infra.es",     // nơi cấu hình ES client, nếu tách riêng
                "com.vft.cdp.profile.infra.es" // nếu cần dùng ProfileDocument, repo
        },
        exclude = {
                DataSourceAutoConfiguration.class,
                HibernateJpaAutoConfiguration.class
        }
)
public class CdpDataGenApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdpDataGenApplication.class, args);
    }
}
