package de.twiechert.linroad.kafka.stream;


/**
 * Provides common LR-related functionality.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class Util {

    /**
     * Given a timestamp in seconds, this method will return the execution method according to the LR specification
     * @param timestamp a tuple's timestamp
     * @return the execution method of the respective tuple
     */
    /**
     * Given a timestamp in seconds, returns the execution minute according to the LR specification.
     */
    public static long minuteOfReport(long timestampSeconds) {
        return (timestampSeconds % 60 == 0) ? (timestampSeconds / 60) : (timestampSeconds / 60) + 1;
    }

    /**
     * Converts a Kafka window boundary (in milliseconds) to LR seconds, then computes the minute.
     */
    public static long minuteOfWindowEnd(long windowEndMs) {
        long seconds = windowEndMs / 1000;
        return minuteOfReport(seconds);
    }

    /**
     * Converts a String to an Integer
     * @param str the string to convert
     * @return the integer value
     */
    public static Integer pInt(String str) {
        return Integer.parseInt(str.trim());
    }

    /**
     * Converts a String to an Long
     * @param str the string to convert
     * @return the long value
     */
    public static Long pLng(String str) {
        return Long.parseLong(str.trim());
    }

    /**
     * Converts a String to a Double
     * @param str the string to convert
     * @return the double value
     */
    public static Double pDob(String str) {
        return Double.parseDouble(str.trim());
    }


}
