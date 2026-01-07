package com.vft.cdp.segmentation.application;

import com.vft.cdp.segmentation.application.command.BuildSegmentCommand;
import com.vft.cdp.segmentation.application.command.CreateSegmentCommand;
import com.vft.cdp.segmentation.application.command.UpdateSegmentCommand;
import com.vft.cdp.segmentation.application.dto.SegmentDto;
import com.vft.cdp.segmentation.domain.model.Segment;
import com.vft.cdp.segmentation.domain.model.SegmentDefinition;
import com.vft.cdp.segmentation.domain.model.SegmentStatus;
import com.vft.cdp.segmentation.domain.repository.SegmentMemberRepository;
import com.vft.cdp.segmentation.domain.repository.SegmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class SegmentAppService {

    private final SegmentRepository segmentRepository;
    private final SegmentMemberRepository segmentMemberRepository;
    // private final SegmentDomainService segmentDomainService; // nếu cần validate definition

    public SegmentDto createSegment(CreateSegmentCommand cmd) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();

        SegmentDefinition def = new SegmentDefinition(cmd.definitionJson().toString());

        // nếu dùng SegmentDomainService:
        // segmentDomainService.validateDefinition(def);

        Segment segment = Segment.builder()
                .id(id)
                .tenantId(cmd.tenantId())
                .name(cmd.name())
                .description(cmd.description())
                .definition(def)
                .status(SegmentStatus.DRAFT)
                .createdAt(now)
                .updatedAt(now)
                .build();

        Segment saved = segmentRepository.save(segment);
        return SegmentDto.fromDomain(saved);
    }

    public SegmentDto updateSegment(UpdateSegmentCommand cmd) {
        Segment segment = segmentRepository
                .findById(cmd.tenantId(), cmd.segmentId())
                .orElseThrow(() -> new IllegalArgumentException("Segment not found"));

        SegmentDefinition newDef = new SegmentDefinition(cmd.definitionJson().toString());
        // segmentDomainService.validateDefinition(newDef);

        segment.rename(cmd.name(), cmd.description());
        segment.changeDefinition(newDef);

        Segment saved = segmentRepository.save(segment);
        return SegmentDto.fromDomain(saved);
    }

    public SegmentDto getSegment(String tenantId, String segmentId) {
        Segment segment = segmentRepository
                .findById(tenantId, segmentId)
                .orElseThrow(() -> new IllegalArgumentException("Segment not found"));
        return SegmentDto.fromDomain(segment);
    }

    public List<SegmentDto> listSegments(String tenantId) {
        return segmentRepository.findAllByTenant(tenantId)
                .stream()
                .map(SegmentDto::fromDomain)
                .toList();
    }

    /**
     * Trigger build segment members.
     * Hiện tại vẫn mock engine – sau này sẽ query ES profile + event_metrics_daily thật.
     */
    public void buildSegment(BuildSegmentCommand cmd) {
        Segment segment = segmentRepository
                .findById(cmd.tenantId(), cmd.segmentId())
                .orElseThrow(() -> new IllegalArgumentException("Segment not found"));

        // TODO: gọi segment engine thật để tính danh sách userId
        // List<String> userIds = segmentEngine.buildMembers(segment);

        List<String> userIds = List.of(); // placeholder

        segmentMemberRepository.replaceMembers(cmd.tenantId(), cmd.segmentId(), userIds);
        segment.updateBuildInfo(Instant.now(), (long) userIds.size());
        segmentRepository.save(segment);

        log.info("Built segment {} for tenant {} with {} members",
                cmd.segmentId(), cmd.tenantId(), userIds.size());
    }
}
