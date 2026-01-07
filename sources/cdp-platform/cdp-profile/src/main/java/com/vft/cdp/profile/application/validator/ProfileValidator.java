package com.vft.cdp.profile.application.validator;

import com.vft.cdp.profile.application.command.CreateProfileCommand;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE VALIDATOR - Utility class for validating profile data
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileValidator {

    // Private constructor to prevent instantiation
    private ProfileValidator() {
        throw new AssertionError("Utility class - cannot instantiate");
    }

    /**
     * Validate traits data
     *
     * @param traits TraitsCommand to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateTraits(CreateProfileCommand.TraitsCommand traits) {
        if (traits == null) {
            return; // Null traits is allowed
        }

        // Validate idcard format (12 digits)
        if (traits.getIdcard() != null && !traits.getIdcard().isBlank()) {
            if (!traits.getIdcard().matches("\\d{12}")) {
                throw new IllegalArgumentException(
                        "Invalid idcard format. Must be 12 digits. Received: " + traits.getIdcard()
                );
            }
        }

        // Validate email format
        if (traits.getEmail() != null && !traits.getEmail().isBlank()) {
            String emailRegex = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$";
            if (!traits.getEmail().matches(emailRegex)) {
                throw new IllegalArgumentException(
                        "Invalid email format: " + traits.getEmail()
                );
            }
        }

        // Validate phone format (Vietnamese phone)
        if (traits.getPhone() != null && !traits.getPhone().isBlank()) {
            String phoneRegex = "^(0|\\+84)[1-9]\\d{8,9}$";
            if (!traits.getPhone().matches(phoneRegex)) {
                throw new IllegalArgumentException(
                        "Invalid phone format. Must be Vietnamese phone number. Received: " + traits.getPhone()
                );
            }
        }

        // Validate date of birth format (yyyy-MM-dd)
        if (traits.getDob() != null && !traits.getDob().isBlank()) {
            try {
                LocalDate dob = LocalDate.parse(traits.getDob());

                // Additional validation: DoB should be in the past
                if (dob.isAfter(LocalDate.now())) {
                    throw new IllegalArgumentException(
                            "Date of birth cannot be in the future: " + traits.getDob()
                    );
                }

                // Additional validation: DoB should be reasonable (not too old)
                if (dob.isBefore(LocalDate.now().minusYears(150))) {
                    throw new IllegalArgumentException(
                            "Date of birth is too far in the past: " + traits.getDob()
                    );
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "Invalid date of birth format. Must be yyyy-MM-dd. Received: " + traits.getDob()
                );
            }
        }
    }

    /**
     * Validate full profile command (can be extended in the future)
     *
     * @param command CreateProfileCommand to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateProfileCommand(CreateProfileCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("Profile command cannot be null");
        }

        // Validate required fields
        if (command.getTenantId() == null || command.getTenantId().isBlank()) {
            throw new IllegalArgumentException("Tenant ID is required");
        }

        if (command.getAppId() == null || command.getAppId().isBlank()) {
            throw new IllegalArgumentException("App ID is required");
        }

        if (command.getUserId() == null || command.getUserId().isBlank()) {
            throw new IllegalArgumentException("User ID is required");
        }

        if (command.getType() == null || command.getType().isBlank()) {
            throw new IllegalArgumentException("Profile type is required");
        }

        // Validate traits
        validateTraits(command.getTraits());
    }
}