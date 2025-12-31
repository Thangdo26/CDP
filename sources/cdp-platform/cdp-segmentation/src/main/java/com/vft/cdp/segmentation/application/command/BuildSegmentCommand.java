package com.vft.cdp.segmentation.application.command;

public record BuildSegmentCommand(
        String tenantId,
        String segmentId
) { }
