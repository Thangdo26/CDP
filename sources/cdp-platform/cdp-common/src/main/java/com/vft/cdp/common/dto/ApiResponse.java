package com.vft.cdp.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class ApiResponse {

    String code;      // ma loi
    String message;     // mô tả ngắn

    @JsonProperty("transaction_id")
    String transactionId;   // optional, sau này có traceId / eventId
}
