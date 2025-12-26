package com.vft.cdp.profile.infra.es;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Elasticsearch repository for ProfileDocument
 */
@Repository
public interface SpringDataProfileRepository extends ElasticsearchRepository<ProfileDocument, String> {
    // Spring Data will auto-implement:
    // - findById(String id)
    // - save(ProfileDocument doc)
    // - delete(ProfileDocument doc)
    // - etc.
}