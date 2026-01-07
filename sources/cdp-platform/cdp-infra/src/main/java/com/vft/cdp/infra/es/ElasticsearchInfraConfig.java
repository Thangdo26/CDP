package com.vft.cdp.infra.es;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@Configuration
@EnableElasticsearchRepositories(basePackages = {
        "com.vft.cdp"    // cho phép các module khác đặt repo ES trong namespace com.vft.cdp.*
})
public class ElasticsearchInfraConfig {
/**
 * Không cần cấu hình auto-config của Spring Boot:
 *  - spring.elasticsearch.uris
 *  - spring.elasticsearch.username
 *  - spring.elasticsearch.password
 *
 * Các module khác chỉ cần:
 *  - @Autowired ElasticsearchOperations operations;
 *  - hoặc khai báo interface extends ElasticsearchRepository<Doc, String>.
 */

}
