package com.vft.cdp.common.exception;

/**
 * Base runtime exception cho toàn bộ hệ thống CDP.
 * Các exception nghiệp vụ khác nên extend từ class này.
 */
public class CdpException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CdpException(String message) {
        super(message);
    }

    public CdpException(String message, Throwable cause) {
        super(message, cause);
    }
}
