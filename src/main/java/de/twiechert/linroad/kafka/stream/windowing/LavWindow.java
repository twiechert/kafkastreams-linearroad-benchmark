package de.twiechert.linroad.kafka.stream.windowing;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * This class creates time-windows for the LAV-stream. It generally behaves like a normal sliding-window generator besides in the first
 * five minutes, where additionally windows for 0..4, 0..3, 0..2, 0..1 are created.

 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class LavWindow extends Windows<TimeWindow> {

    private final static long size = 300;

    private final static long advance = 60;

    private LavWindow() {
    }

    /**
     * Returns a window definition. Size and advance are fixed to the LAV use case.
     * @return a new window definition
     */
    public static LavWindow of() {
        return new LavWindow();
    }

    /**
     * @deprecated Use {@link #of()} instead.
     */
    @Deprecated
    public static LavWindow of(String name) {
        return new LavWindow();
    }


    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        long windowStart = (Math.max(0, timestamp - size + advance) / advance) * advance;
        Map<Long, TimeWindow> windows = new HashMap<>();
        while (windowStart <= timestamp) {
            TimeWindow window = new TimeWindow(windowStart, windowStart + size);
            windows.put(windowStart, window);
            windowStart += advance;
        }

        /*
          Special treatment for elements in the first five minutes..
          We need additionaly to consider the windows 0..4, 0..3, 0..2, 0..1 and check if the timestamp falls in these intervals
         */
        if (timestamp < 300) {

            for (long i = (300 - 60); i >= timestamp && i > 0; i -= 60) {
                TimeWindow window = new TimeWindow(0L, i);
                windows.put(0L, window);
            }
        }
        return windows;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long gracePeriodMs() {
        return size;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LavWindow)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (advance ^ (advance >>> 32));
        return result;
    }
}
