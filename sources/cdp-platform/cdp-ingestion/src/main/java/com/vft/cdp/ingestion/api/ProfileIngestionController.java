package com.vft.cdp.ingestion.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.Constants;
import com.vft.cdp.ingestion.application.IngestionService;
import com.vft.cdp.ingestion.application.dto.IngestionResponse;
import com.vft.cdp.ingestion.application.dto.ProfileIngestionRequest;
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
public class ProfileIngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/profiles")
    public ResponseEntity<IngestionResponse> ingestProfile(
            @Valid @RequestBody ProfileIngestionRequest request,
            HttpServletRequest servletRequest
    ) {
        // Get auth context from filter
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401)
                    .body(IngestionResponse.builder()
                            .status("error")
                            .message("Unauthorized")
                            .requestId(null)
                            .build());
        }

        String requestId = ingestionService.ingestProfile(authContext, request);

        return ResponseEntity.ok(
                IngestionResponse.builder()
                        .status("ok")
                        .message("Profile accepted")
                        .requestId(requestId)
                        .build()
        );
    }
}