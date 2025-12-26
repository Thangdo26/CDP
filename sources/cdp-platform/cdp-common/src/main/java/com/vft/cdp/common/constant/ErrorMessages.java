package com.vft.cdp.common.constant;

/**
 * Centralized error messages corresponding to ErrorCodes
 */
public final class ErrorMessages {
    private ErrorMessages() {
    }

    // Success
    public static final String OK = "Success";

    // 4xx – client errors
    public static final String BAD_REQUEST = "Invalid request parameters";
    public static final String UNAUTHORIZED = "Authentication required or invalid credentials";
    public static final String FORBIDDEN = "Access denied - insufficient permissions";
    public static final String NOT_FOUND = "Resource not found";
    public static final String REQUEST_TIMEOUT = "Request timeout - please try again";
    public static final String CONFLICT = "Resource conflict or duplicate request";
    public static final String UNPROCESSABLE_ENTITY = "Business logic validation failed";
    public static final String TOO_MANY_REQUESTS = "Too many requests - please try again later";

    // 5xx – server / upstream
    public static final String FAILED = "Failed";
    public static final String SERVER_ERROR = "Internal server error";
    public static final String BAD_GATEWAY = "Bad gateway - upstream service error";
    public static final String SERVICE_UNAVAILABLE = "Service temporarily unavailable";
    public static final String GATEWAY_TIMEOUT = "Gateway timeout - upstream service timeout";

    // Business specific errors
    public static final String INSUFFICIENT_BALANCE = "Insufficient account balance";
    public static final String ACCOUNT_LOCKED_OVERDUE = "Account locked due to overdue payments";

    // Payment status messages
    public static final String STATUS_PROCESSING = "Payment is being processed";
    public static final String STATUS_SUCCESS = "Payment completed successfully";
    public static final String STATUS_FAIL = "Payment failed";
}
