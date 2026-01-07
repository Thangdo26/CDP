package com.vft.cdp.profile.application;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service to find matching profiles based on multiple criteria
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileMatchingService {

    private final ProfileRepository profileRepository;

    /**
     * Find matching profile using multiple strategies
     * Priority: idcard > phone > email+dob
     *
     * @return MatchResult with matched profile and strategy used
     */
    public MatchResult findMatchingProfile(
            String tenantId,
            String idcard,
//            String phone,
            String email,
            String dob
    ) {
        log.info("🔍 Finding matching profile: tenant={}, idcard={}, email={}, dob={}",
                tenantId, idcard, email, dob);

        // ✅ Strategy 1: Match by IDCARD (highest priority)
        if (idcard != null && !idcard.isBlank()) {
            List<ProfileModel> matchedByIdcard = profileRepository.findByIdcard(tenantId, idcard);
            if (!matchedByIdcard.isEmpty()) {
                log.info("✅ Found match by IDCARD: {}", matchedByIdcard.get(0).getUserId());
                return MatchResult.found(
                        matchedByIdcard.get(0),
                        MergeStrategy.IDCARD
                );
            }
        }

//        // ✅ Strategy 2: Match by PHONE
//        if (phone != null && !phone.isBlank()) {
//            List<ProfileModel> matchedByPhone = profileRepository.findByPhone(tenantId, phone);
//            if (!matchedByPhone.isEmpty()) {
//                log.info("✅ Found match by PHONE: {}", matchedByPhone.get(0).getUserId());
//                return MatchResult.found(
//                        matchedByPhone.get(0),
//                        MergeStrategy.PHONE
//                );
//            }
//        }

        // ✅ Strategy 3: Match by EMAIL + DOB
        if (email != null && !email.isBlank() && dob != null && !dob.isBlank()) {
            List<ProfileModel> matchedByEmail = profileRepository.findByEmail(tenantId, email);

            // Filter by DOB
            Optional<ProfileModel> matchedByEmailDob = matchedByEmail.stream()
                    .filter(p -> p.getTraits() != null)
                    .filter(p -> dob.equals(p.getTraits().getDob()))
                    .findFirst();

            if (matchedByEmailDob.isPresent()) {
                log.info("✅ Found match by EMAIL+DOB: {}", matchedByEmailDob.get().getUserId());
                return MatchResult.found(
                        matchedByEmailDob.get(),
                        MergeStrategy.EMAIL_DOB
                );
            }
        }
        return MatchResult.notFound();
    }

    /**
     * Merge strategy enum
     */
    public enum MergeStrategy {
        IDCARD("idcard"),
//        PHONE("phone"),
        EMAIL_DOB("email+dob");

        private final String value;

        MergeStrategy(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * Match result
     */
    @lombok.Data
    @lombok.AllArgsConstructor
    public static class MatchResult {
        private boolean found;
        private ProfileModel profile;
        private MergeStrategy strategy;

        public static MatchResult found(ProfileModel profile, MergeStrategy strategy) {
            return new MatchResult(true, profile, strategy);
        }

        public static MatchResult notFound() {
            return new MatchResult(false, null, null);
        }
    }
}