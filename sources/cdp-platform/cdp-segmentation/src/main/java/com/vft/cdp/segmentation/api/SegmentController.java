package com.vft.cdp.segmentation.api;

import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.Constants;
import com.vft.cdp.segmentation.api.request.CreateSegmentRequest;
import com.vft.cdp.segmentation.api.request.UpdateSegmentRequest;
import com.vft.cdp.segmentation.api.response.SegmentListItemResponse;
import com.vft.cdp.segmentation.api.response.SegmentResponse;
import com.vft.cdp.segmentation.application.SegmentAppService;
import com.vft.cdp.segmentation.application.command.BuildSegmentCommand;
import com.vft.cdp.segmentation.application.command.CreateSegmentCommand;
import com.vft.cdp.segmentation.application.command.UpdateSegmentCommand;
import com.vft.cdp.segmentation.application.dto.SegmentDto;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/v1/segments")
@RequiredArgsConstructor
public class SegmentController {

    private final SegmentAppService segmentAppService;

    @PostMapping("/{tenantId}")
    public ResponseEntity<SegmentResponse> createSegment(
            @PathVariable("tenantId") String tenantId,
            @RequestBody CreateSegmentRequest request,
            HttpServletRequest servletRequest
    ) {
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401).build();
        }

        CreateSegmentCommand cmd = new CreateSegmentCommand(
                tenantId,
                request.name(),
                request.description(),
                request.definitionJson()
        );

        SegmentDto dto = segmentAppService.createSegment(cmd);
        return ResponseEntity.ok(SegmentResponse.fromDto(dto));
    }

    @PutMapping("/{tenantId}/{segmentId}")
    public ResponseEntity<SegmentResponse> updateSegment(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("segmentId") String segmentId,
            @RequestBody UpdateSegmentRequest request,
            HttpServletRequest servletRequest
    ) {
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401).build();
        }

        UpdateSegmentCommand cmd = new UpdateSegmentCommand(
                tenantId,
                segmentId,
                request.name(),
                request.description(),
                request.definitionJson()
        );

        SegmentDto dto = segmentAppService.updateSegment(cmd);
        return ResponseEntity.ok(SegmentResponse.fromDto(dto));
    }

    @GetMapping("/{tenantId}/{segmentId}")
    public ResponseEntity<SegmentResponse> getSegment(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("segmentId") String segmentId,
            HttpServletRequest servletRequest
    ) {
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401).build();
        }

        SegmentDto dto = segmentAppService.getSegment(tenantId, segmentId);
        return ResponseEntity.ok(SegmentResponse.fromDto(dto));
    }

    @GetMapping("/{tenantId}")
    public ResponseEntity<List<SegmentListItemResponse>> listSegments(
            @PathVariable("tenantId") String tenantId,
            HttpServletRequest servletRequest
    ) {
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401).build();
        }

        List<SegmentListItemResponse> list = segmentAppService.listSegments(tenantId)
                .stream()
                .map(SegmentListItemResponse::fromDto)
                .toList();

        return ResponseEntity.ok(list);
    }

    @PostMapping("/{tenantId}/{segmentId}/build")
    public ResponseEntity<Void> buildSegment(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("segmentId") String segmentId,
            HttpServletRequest servletRequest
    ) {
        ApiKeyAuthContext authContext =
                (ApiKeyAuthContext) servletRequest.getAttribute(Constants.ATTR_AUTH_CONTEXT);

        if (authContext == null) {
            log.warn(Constants.MESSAGE_MISSING_AUTH_KEY);
            return ResponseEntity.status(401).build();
        }

        segmentAppService.buildSegment(new BuildSegmentCommand(tenantId, segmentId));
        return ResponseEntity.accepted().build();
    }
}
