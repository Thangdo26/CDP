package com.vft.cdp.segmentation.api.request;

import com.fasterxml.jackson.databind.JsonNode;

public record UpdateSegmentRequest(
        String name,
        String description,
        JsonNode definitionJson

) { }
