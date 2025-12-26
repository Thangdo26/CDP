package com.vft.cdp.profile.api.response;

import org.springframework.data.domain.Page;

import java.util.List;

/**
 * Paginated list response for profiles
 */
public record ProfileListResponse(
        List<ProfileResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    public static ProfileListResponse fromPage(Page<ProfileResponse> page) {
        return new ProfileListResponse(
                page.getContent(),
                page.getNumber(),
                page.getSize(),
                page.getTotalElements(),
                page.getTotalPages()
        );
    }
}