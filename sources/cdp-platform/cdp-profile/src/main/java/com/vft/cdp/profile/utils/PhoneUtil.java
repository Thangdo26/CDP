package com.vft.cdp.profile.utils;

import java.util.ArrayList;
import java.util.List;

public final class PhoneUtil {
    private PhoneUtil() {}

    public static String normalize(String p) {
        if (p == null) return null;
        p = p.trim();
        return p.isBlank() ? null : p;
    }

    public static boolean addIfAbsent(List<String> list, String phone) {
        phone = normalize(phone);
        if (phone == null) return false;
        if (list == null) return false;
        if (list.contains(phone)) return false;
        list.add(phone);
        return true;
    }

    public static List<String> union(List<String> a, List<String> b) {
        List<String> out = new ArrayList<>();
        if (a != null) for (String x : a) {
            x = normalize(x);
            if (x != null && !out.contains(x)) out.add(x);
        }
        if (b != null) for (String x : b) {
            x = normalize(x);
            if (x != null && !out.contains(x)) out.add(x);
        }
        return out;
    }
}
