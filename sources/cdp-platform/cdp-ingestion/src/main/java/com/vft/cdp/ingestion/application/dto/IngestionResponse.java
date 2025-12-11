package com.vft.cdp.ingestion.application.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IngestionResponse {

    String status;      // "ok"
    String message;     // mô tả ngắn
    String requestId;   // optional, sau này có traceId / eventId
}
