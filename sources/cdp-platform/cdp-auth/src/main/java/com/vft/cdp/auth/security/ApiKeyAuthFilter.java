package com.vft.cdp.auth.security;

import com.vft.cdp.auth.application.ApiKeyAuthService;
import com.vft.cdp.auth.application.ApiKeyAuthService.ApiKeyAuthException;
import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.constant.Constants;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

@Slf4j
public class ApiKeyAuthFilter extends OncePerRequestFilter {


    private final ApiKeyAuthService authService;
    private final String[] publicPaths;
    private final AntPathMatcher matcher = new AntPathMatcher();

    public ApiKeyAuthFilter(ApiKeyAuthService authService, String[] publicPaths) {
        this.authService = authService;
        this.publicPaths = publicPaths;
        log.info(">>> ApiKeyAuthFilter constructed");

    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        for (String pattern : publicPaths) {
            if (matcher.match(pattern, path)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {
        // === 1) Lấy traceId hoặc tạo mới ===
        String traceId = request.getHeader("X-Trace-Id");
        if (traceId == null || traceId.isBlank()) {
            traceId = java.util.UUID.randomUUID().toString();
        }

        // Put vào MDC để logging tự động có traceId
        MDC.put("traceId", traceId);
        String apiKey = request.getHeader(Constants.HEADER_API_KEY);
        try {
            ApiKeyAuthContext ctx = authService.authenticate(apiKey);
            request.setAttribute(Constants.ATTR_AUTH_CONTEXT, ctx);
            // 2) Tạo Authentication cho Spring Security
            UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken(
                            ctx,                                       // principal
                            null,                                      // credentials
                            List.of(new SimpleGrantedAuthority("ROLE_API_CLIENT")) // quyền basic
                    );

            authentication.setDetails(request);
            SecurityContextHolder.getContext().setAuthentication(authentication);

            // 3) Cho phép đi tiếp
            filterChain.doFilter(request, response);
        } catch (ApiKeyAuthException ex) {
            String shortKey = (apiKey != null && apiKey.length() > 6)
                    ? apiKey.substring(0, 6) + "..."
                    : "null";
            log.warn("API Key auth failed, key prefix={}, reason={}", shortKey, ex.getMessage());

            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "ApiKey realm=\"cdp\"");
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"unauthorized\"}");
        } finally {
            // === 3) Clear MDC để tránh leak giữa requests ===
            MDC.remove("traceId");
        }
    }
}
