package com.vft.cdp.ingestion.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.ingestion.application.dto.IngestionResponse;
import com.vft.cdp.ingestion.application.dto.TrackRequest;
import com.vft.cdp.ingestion.application.IngestionService;
import com.vft.cdp.common.constant.Constants;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/events")
    public ResponseEntity<IngestionResponse> trackEvent(
            @Valid @RequestBody TrackRequest request,
            HttpServletRequest servletRequest
    ) {

        // Lấy auth context từ filter cdp-auth
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute( Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            // trường hợp filter chưa chạy hoặc có vấn đề ngoài dự kiến
            log.warn("Missing ApiKeyAuthContext in request attributes");
            return ResponseEntity.status(401)
                    .body(IngestionResponse.builder()
                            .status("error")
                            .message("Unauthorized")
                            .requestId(null)
                            .build());
        }

        String requestId = ingestionService.ingestEvent(authContext, request);

        return ResponseEntity.ok(
                IngestionResponse.builder()
                        .status("ok")
                        .message("Accepted")
                        .requestId(requestId)
                        .build()
        );
    }
}
