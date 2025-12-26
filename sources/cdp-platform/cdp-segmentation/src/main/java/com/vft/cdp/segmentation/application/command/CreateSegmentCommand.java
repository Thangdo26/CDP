package com.vft.cdp.segmentation.application.command;

import com.fasterxml.jackson.databind.JsonNode;

public record CreateSegmentCommand(
        String tenantId,
        String name,
        String description,
        JsonNode definitionJson
) { }
