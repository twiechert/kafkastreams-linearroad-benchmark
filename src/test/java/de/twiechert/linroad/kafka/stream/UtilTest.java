package de.twiechert.linroad.kafka.stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Util helper methods used across the streaming logic.
 */
class UtilTest {

    @Test
    void minuteOfReport_exactMinuteBoundary() {
        // At exactly 0 seconds, minute should be 0
        assertEquals(0, Util.minuteOfReport(0));
        // At exactly 60 seconds, minute should be 1
        assertEquals(1, Util.minuteOfReport(60));
        // At exactly 120 seconds, minute should be 2
        assertEquals(2, Util.minuteOfReport(120));
    }

    @Test
    void minuteOfReport_withinMinute() {
        // At 1 second, should be minute 1 (ceiling)
        assertEquals(1, Util.minuteOfReport(1));
        // At 30 seconds, should be minute 1
        assertEquals(1, Util.minuteOfReport(30));
        // At 59 seconds, should be minute 1
        assertEquals(1, Util.minuteOfReport(59));
        // At 61 seconds, should be minute 2
        assertEquals(2, Util.minuteOfReport(61));
        // At 90 seconds, should be minute 2
        assertEquals(2, Util.minuteOfReport(90));
    }

    @Test
    void minuteOfReport_largerTimestamps() {
        // 300 seconds = 5 minutes exactly
        assertEquals(5, Util.minuteOfReport(300));
        // 301 seconds = minute 6
        assertEquals(6, Util.minuteOfReport(301));
    }

    @Test
    void pInt_parsesCorrectly() {
        assertEquals(42, Util.pInt("42"));
        assertEquals(0, Util.pInt("0"));
        assertEquals(-1, Util.pInt("-1"));
        // With whitespace
        assertEquals(42, Util.pInt("  42  "));
    }

    @Test
    void pLng_parsesCorrectly() {
        assertEquals(42L, Util.pLng("42"));
        assertEquals(0L, Util.pLng("0"));
        assertEquals(1000000000L, Util.pLng("1000000000"));
    }

    @Test
    void pDob_parsesCorrectly() {
        assertEquals(3.14, Util.pDob("3.14"), 0.001);
        assertEquals(0.0, Util.pDob("0.0"), 0.001);
    }
}
