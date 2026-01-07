package com.vft.cdp.profile.utils;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public final class AutoIdUtil {

    // Epoch: 2026-01-01T00:00:00Z in millis (giảm độ dài ID)
    // 2026-01-01 00:00:00 UTC = 1767225600000
    private static final long EPOCH_MS = 1767225600000L;

    // Bit allocation (tổng 63 bits để luôn dương)
    // time: 41 bits (~69 năm tính từ epoch)
    // worker: 10 bits (0..1023)
    // seq: 12 bits (0..4095 / ms / worker)
    private static final int WORKER_BITS = 10;
    private static final int SEQ_BITS = 12;

    private static final long MAX_WORKER = (1L << WORKER_BITS) - 1; // 1023
    private static final long MAX_SEQ = (1L << SEQ_BITS) - 1;       // 4095

    private static final int WORKER_SHIFT = SEQ_BITS;
    private static final int TIME_SHIFT = WORKER_BITS + SEQ_BITS;

    private static final long WORKER_ID = initWorkerId();

    // State (thread-safe)
    private static volatile long lastMs = -1L;
    private static final AtomicInteger seq = new AtomicInteger(0);

    private AutoIdUtil() {}

    /**
     * workerId = hash(hostname) % 1024, fallback random
     */
    private static long initWorkerId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            CRC32 crc = new CRC32();
            crc.update(host.getBytes(StandardCharsets.UTF_8));
            return (crc.getValue() & 0xffffffffL) % (MAX_WORKER + 1);
        } catch (Exception e) {
            return ThreadLocalRandom.current().nextLong(0, MAX_WORKER + 1);
        }
    }

    /**
     * Gen ID numeric string, sortable theo thời gian.
     * Không trùng trong cùng workerId với throughput <= 4096 IDs/ms
     */
    public static String genProfileId() {
        long now = System.currentTimeMillis();
        long delta = now - EPOCH_MS;
        if (delta < 0) {
            // clock lỗi/đi lùi so với epoch -> fallback dùng now luôn
            delta = now;
        }

        long s;
        synchronized (AutoIdUtil.class) {
            if (now == lastMs) {
                int next = (seq.incrementAndGet()) & (int) MAX_SEQ;
                if (next == 0) {
                    // overflow seq trong cùng 1ms -> đợi sang ms tiếp theo
                    now = waitNextMs(lastMs);
                    lastMs = now;
                    seq.set(0);
                }
            } else {
                lastMs = now;
                seq.set(0);
            }
            s = seq.get() & MAX_SEQ;
        }

        long id = (delta << TIME_SHIFT) | (WORKER_ID << WORKER_SHIFT) | s;
        // đảm bảo luôn dương
        if (id < 0) id = -id;

        return Long.toString(id);
    }

    private static long waitNextMs(long last) {
        long now = System.currentTimeMillis();
        while (now <= last) {
            now = System.currentTimeMillis();
        }
        return now;
    }
}
