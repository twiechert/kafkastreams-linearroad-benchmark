package de.twiechert.linroad.kafka.model;

/**
 * This interface declares that a tuple has incorporated event time in minutes.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public interface TimedOnMinute extends java.io.Serializable {

    long getMinute();

    /**
     * This interface declares that a tuple has also incorporated window end event time in minutes.
     * This interface is used, if a the tuples have been created by windowed aggregation.
     *
     * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
     */
    interface TimedOnMinuteWithWindowEnd extends TimedOnMinute {
        long getWindowEndMinute();
    }
}
