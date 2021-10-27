package net.kdb4j.utils;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayPool {
    public static char[] takeChar(int vectorLength) {
        return new char[vectorLength];
    }

    public static long[] takeLong(int vectorLength) {
        return new long[vectorLength];
    }

    public static double[] takeDouble(int vectorLength) {
        return new double[vectorLength];
    }

    public Collection<String> takeStringCollection() {
        return new ArrayList<>();
    }

    public TimeUtils.TimestampVector takeTimestamp(int vectorLength) {
        return new TimeUtils.TimestampVector(vectorLength);
    }

    public void putBack(Object o) {

    }
}
