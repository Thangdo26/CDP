package com.vft.cdp.segmentation.application.command;

import com.fasterxml.jackson.databind.JsonNode;

public record UpdateSegmentCommand(
        String tenantId,
        String segmentId,
        String name,
        String description,
        JsonNode definitionJson
) { }
