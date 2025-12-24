package com.vft.cdp.ingestion.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.ErrorCodes;
import com.vft.cdp.common.constant.ErrorMessages;
import com.vft.cdp.common.dto.ApiResponse;
import com.vft.cdp.ingestion.dto.TrackRequest;
import com.vft.cdp.ingestion.application.IngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/v1/events")
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/track")
    public ResponseEntity<ApiResponse> trackEvent(
            @Valid @RequestBody TrackRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {

        String requestId = ingestionService.ingestEvent(authContext, request);

        return ResponseEntity.ok(
                ApiResponse.builder()
                        .code(ErrorCodes.OK)
                        .message(ErrorMessages.OK)
                        .transactionId(requestId)
                        .build()
        );
    }
}
