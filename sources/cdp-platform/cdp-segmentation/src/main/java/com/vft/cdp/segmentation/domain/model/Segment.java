package com.vft.cdp.segmentation.domain.model;

import java.time.Instant;
import java.util.Objects;

public class Segment {

    private final String id;
    private final String tenantId;

    private String name;
    private String description;
    private SegmentDefinition definition;
    private SegmentStatus status;

    private Instant createdAt;
    private Instant updatedAt;
    private Instant lastBuiltAt;
    private Long estimatedSize;

    private Segment(Builder builder) {
        this.id = Objects.requireNonNull(builder.id);
        this.tenantId = Objects.requireNonNull(builder.tenantId);
        this.name = Objects.requireNonNull(builder.name);
        this.description = builder.description;
        this.definition = Objects.requireNonNull(builder.definition);
        this.status = builder.status != null ? builder.status : SegmentStatus.DRAFT;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.lastBuiltAt = builder.lastBuiltAt;
        this.estimatedSize = builder.estimatedSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String tenantId;
        private String name;
        private String description;
        private SegmentDefinition definition;
        private SegmentStatus status;
        private Instant createdAt;
        private Instant updatedAt;
        private Instant lastBuiltAt;
        private Long estimatedSize;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder definition(SegmentDefinition definition) {
            this.definition = definition;
            return this;
        }

        public Builder status(SegmentStatus status) {
            this.status = status;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder lastBuiltAt(Instant lastBuiltAt) {
            this.lastBuiltAt = lastBuiltAt;
            return this;
        }

        public Builder estimatedSize(Long estimatedSize) {
            this.estimatedSize = estimatedSize;
            return this;
        }

        public Segment build() {
            return new Segment(this);
        }
    }

    // -------------------------------
    // Các business method giữ nguyên
    // -------------------------------

    public void rename(String newName, String newDescription) {
        this.name = Objects.requireNonNull(newName);
        this.description = newDescription;
        touch();
    }

    public void changeDefinition(SegmentDefinition newDef) {
        this.definition = Objects.requireNonNull(newDef);
        this.lastBuiltAt = null;
        this.estimatedSize = null;
        touch();
    }

    public void updateBuildInfo(Instant builtAt, Long size) {
        this.lastBuiltAt = builtAt;
        this.estimatedSize = size;
        touch();
    }

    private void touch() {
        this.updatedAt = Instant.now();
    }

    // ---------------------------
// Getters
// ---------------------------
    public String getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public SegmentDefinition getDefinition() {
        return definition;
    }

    public SegmentStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getLastBuiltAt() {
        return lastBuiltAt;
    }

    public Long getEstimatedSize() {
        return estimatedSize;
    }

}
