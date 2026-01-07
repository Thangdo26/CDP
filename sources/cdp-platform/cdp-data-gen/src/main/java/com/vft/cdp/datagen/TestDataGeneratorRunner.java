package com.vft.cdp.datagen;

import com.vft.cdp.datagen.es.EventMetricsDailyDocument;
import com.vft.cdp.datagen.es.RawEventDocument;
import com.vft.cdp.profile.infra.es.ProfileDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

@Slf4j
@Component
@Profile("data-gen")
@RequiredArgsConstructor
public class TestDataGeneratorRunner implements CommandLineRunner {

    private static final String TENANT_ID = "tenant_1";

    private static final int PROFILE_COUNT = 500_000;
    private static final int MIN_EVENTS_PER_PROFILE = 50;
    private static final int MAX_EVENTS_PER_PROFILE = 200;
    private static final int DAYS = 60;

    private static final int BATCH_SIZE_PROFILES = 2_000;
    private static final int BATCH_SIZE_EVENTS = 5_000;
    private static final int BATCH_SIZE_METRICS = 5_000;

    private final ElasticsearchOperations esOps;

    @Override
    public void run(String... args) {
        log.info("=== START GENERATING TEST DATA (profiles={}, days={}, eventsPerProfile={}..{}) ===",
                PROFILE_COUNT, DAYS, MIN_EVENTS_PER_PROFILE, MAX_EVENTS_PER_PROFILE);

        generateProfiles();
        generateRawEventsAndMetrics();

        log.info("=== DONE GENERATING TEST DATA ===");
    }

    // ------------------------------------------------------------------------
    // 1) PROFILES
    // ------------------------------------------------------------------------
    private void generateProfiles() {
        IndexOperations indexOps = esOps.indexOps(ProfileDocument.class);
        if (!indexOps.exists()) {
            indexOps.create();
            indexOps.putMapping(indexOps.createMapping());
            log.info("Created index for ProfileDocument: {}", indexOps.getIndexCoordinates().getIndexName());
        }

        List<ProfileDocument> batch = new ArrayList<>(BATCH_SIZE_PROFILES);
        Random random = new Random();

        String[] cities = {"Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue", "Nha Trang"};
        String[] genders = {"male", "female"};
        Instant now = Instant.now();

        for (int i = 0; i < PROFILE_COUNT; i++) {
            String profileId = "user_" + i;

            ProfileDocument doc = new ProfileDocument();
            doc.setId(TENANT_ID + "|" + profileId);
            doc.setTenantId(TENANT_ID);
            doc.setProfileId(profileId);

            Map<String, Object> identifiers = new HashMap<>();
            identifiers.put("email", "user" + i + "@example.com");
            identifiers.put("phone", "09" + String.format("%08d", i));
            doc.setIdentifiers(identifiers);

            Map<String, Object> traits = new HashMap<>();
            traits.put("gender", genders[random.nextInt(genders.length)]);
            traits.put("city", cities[random.nextInt(cities.length)]);
            traits.put("age", 18 + random.nextInt(40));
            traits.put("vip_level", random.nextInt(5)); // 0-4
            doc.setTraits(traits);

            doc.setStatus("ACTIVE");
            Instant createdAt = now.minusSeconds(random.nextInt(60 * 60 * 24 * 90)); // trong 90 ngày gần
            doc.setCreatedAt(createdAt);
            doc.setUpdatedAt(now);

            batch.add(doc);

            if (batch.size() >= BATCH_SIZE_PROFILES) {
                esOps.save(batch);
                batch.clear();
                if (i % 50_000 == 0 && i > 0) {
                    log.info("Profiles generated: {}", i);
                }
            }
        }

        if (!batch.isEmpty()) {
            esOps.save(batch);
        }

        log.info("Finished generating {} profiles", PROFILE_COUNT);
    }

    // ------------------------------------------------------------------------
    // 2) RAW EVENTS + METRICS DAILY (giản lược)
    // ------------------------------------------------------------------------
    private void generateRawEventsAndMetrics() {
        String[] eventNames = {"view_product", "add_to_cart", "purchase", "login"};
        String[] sources = {"web", "app"};
        Random random = new Random();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);

        List<Object> rawEventBatch = new ArrayList<>(BATCH_SIZE_EVENTS);
        List<EventMetricsDailyDocument> metricsBatch = new ArrayList<>(BATCH_SIZE_METRICS);

        for (int i = 0; i < PROFILE_COUNT; i++) {
            String profileId = "user_" + i;

            // Random tổng số event cho profile này
            int eventsForProfile = MIN_EVENTS_PER_PROFILE
                    + random.nextInt(MAX_EVENTS_PER_PROFILE - MIN_EVENTS_PER_PROFILE + 1);

            // Dùng Map tạm để tính metrics daily (key: date|eventName)
            Map<String, MetricsAcc> metricsAccMap = new HashMap<>();

            for (int e = 0; e < eventsForProfile; e++) {
                // Chọn ngày trong 60 ngày gần nhất
                int dayOffset = random.nextInt(DAYS);
                LocalDate eventDate = today.minusDays(dayOffset);

                // Thời điểm cụ thể trong ngày
                Instant eventTime = eventDate
                        .atStartOfDay()
                        .plusSeconds(random.nextInt(24 * 60 * 60))
                        .toInstant(ZoneOffset.UTC);

                String eventName = eventNames[random.nextInt(eventNames.length)];
                String source = sources[random.nextInt(sources.length)];

                Map<String, Object> props = new HashMap<>();
                double amount = 0;
                if ("purchase".equals(eventName)) {
                    amount = 50_000 + random.nextInt(2_000_000);
                    props.put("amount", amount);
                }
                props.put("channel", source);

                RawEventDocument raw = new RawEventDocument();
                raw.setId(UUID.randomUUID().toString());
                raw.setTenantId(TENANT_ID);
                raw.setProfileId(profileId);
                raw.setEventName(eventName);
                raw.setEventTime(eventTime);
                raw.setSource(source);
                raw.setProperties(props);

                // index theo ngày: cdp-events-raw-YYYY.MM.DD
                String indexName = "cdp-events-raw-" + eventDate;
                IndexCoordinates idx = IndexCoordinates.of(indexName);
                rawEventBatch.add(new IndexedDoc(raw, idx));

                if (rawEventBatch.size() >= BATCH_SIZE_EVENTS) {
                    bulkSaveRaw(rawEventBatch);
                    rawEventBatch.clear();
                }

                // Accumulate metrics
                String key = eventDate.toString() + "|" + eventName;
                MetricsAcc acc = metricsAccMap.computeIfAbsent(key, k -> new MetricsAcc());
                acc.count++;
                if (amount > 0) {
                    acc.amountSum += amount;
                    acc.amountMax = Math.max(acc.amountMax, amount);
                }
            }

            // Sau khi xử lý xong 1 profile -> đẩy metrics ra batch
            for (Map.Entry<String, MetricsAcc> entry : metricsAccMap.entrySet()) {
                String[] parts = entry.getKey().split("\\|");
                LocalDate date = LocalDate.parse(parts[0]);
                String eventName = parts[1];
                MetricsAcc acc = entry.getValue();

                EventMetricsDailyDocument m = new EventMetricsDailyDocument();
                m.setId(UUID.randomUUID().toString());
                m.setTenantId(TENANT_ID);
                m.setProfileId(profileId);
                m.setEventName(eventName);
                m.setEventDate(date);
                m.setEventCount(acc.count);
                m.setAmountSum(acc.amountSum == 0 ? null : acc.amountSum);
                m.setAmountMax(acc.amountMax == 0 ? null : acc.amountMax);

                metricsBatch.add(m);
                if (metricsBatch.size() >= BATCH_SIZE_METRICS) {
                    bulkSaveMetrics(metricsBatch);
                    metricsBatch.clear();
                }
            }

            if (i > 0 && i % 10_000 == 0) {
                log.info("Events + metrics generated for {} profiles", i);
            }
        }

        if (!rawEventBatch.isEmpty()) {
            bulkSaveRaw(rawEventBatch);
        }
        if (!metricsBatch.isEmpty()) {
            bulkSaveMetrics(metricsBatch);
        }

        log.info("Finished generating raw events + metrics for {} profiles", PROFILE_COUNT);
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private void bulkSaveRaw(List<Object> batch) {
        // batch: list IndexedDoc (wrapper)
        List<Object> docs = new ArrayList<>(batch.size());
        Map<IndexCoordinates, List<Object>> byIndex = new HashMap<>();

        for (Object o : batch) {
            IndexedDoc idoc = (IndexedDoc) o;
            byIndex.computeIfAbsent(idoc.index, k -> new ArrayList<>()).add(idoc.doc);
        }

        byIndex.forEach((idx, docsForIndex) -> esOps.save(docsForIndex, idx));
    }

    private void bulkSaveMetrics(List<EventMetricsDailyDocument> batch) {
        IndexCoordinates idx = IndexCoordinates.of("event-metrics-daily-v1");
        esOps.save(batch, idx);
    }

    private static class MetricsAcc {
        int count = 0;
        double amountSum = 0;
        double amountMax = 0;
    }

    private record IndexedDoc(Object doc, IndexCoordinates index) {}
}
