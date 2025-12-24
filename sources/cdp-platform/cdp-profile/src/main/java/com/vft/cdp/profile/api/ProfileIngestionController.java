package com.vft.cdp.profile.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.ErrorCodes;
import com.vft.cdp.common.constant.ErrorMessages;
import com.vft.cdp.common.dto.ApiResponse;
import com.vft.cdp.profile.api.request.ProfileIngestionRequest;
import com.vft.cdp.profile.application.ProfileIngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class ProfileIngestionController {

    private final ProfileIngestionService ingestionService;

    @PostMapping("/profiles")
    public ResponseEntity<ApiResponse> ingestProfile(
            @Valid @RequestBody ProfileIngestionRequest request,
            @AuthenticationPrincipal ApiKeyAuthContext authContext
    ) {
        String requestId = ingestionService.ingestProfile(authContext, request);

        return ResponseEntity.ok(
                ApiResponse.builder()
                        .code(ErrorCodes.OK)
                        .message(ErrorMessages.OK)
                        .transactionId(requestId)
                        .build()
        );
    }
}