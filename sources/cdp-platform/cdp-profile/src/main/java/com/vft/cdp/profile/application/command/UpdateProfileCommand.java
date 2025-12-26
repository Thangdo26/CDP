package com.vft.cdp.profile.application.command;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * UPDATE PROFILE COMMAND
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateProfileCommand {

    // Identity
    private String tenantId;
    private String appId;
    private String userId;

    // Data
    private String type;
    private CreateProfileCommand.TraitsCommand traits;
    private CreateProfileCommand.PlatformsCommand platforms;
    private CreateProfileCommand.CampaignCommand campaign;
    private Map<String, Object> metadata;
}