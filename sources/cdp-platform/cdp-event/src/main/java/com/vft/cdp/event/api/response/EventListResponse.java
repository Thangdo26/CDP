package com.vft.cdp.event.api.response;

import java.util.List;

/**
 * Paginated list response
 *
 * PAGINATION METADATA:
 * - content: Actual data
 * - page: Current page number
 * - size: Items per page
 * - totalElements: Total items across all pages
 * - totalPages: Total number of pages
 */
public record EventListResponse(
        List<EventResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    /**
     * Factory method from Spring Data Page
     */
    public static EventListResponse fromPage(
            org.springframework.data.domain.Page<EventResponse> page) {

        return new EventListResponse(
                page.getContent(),
                page.getNumber(),
                page.getSize(),
                page.getTotalElements(),
                page.getTotalPages()
        );
    }
}