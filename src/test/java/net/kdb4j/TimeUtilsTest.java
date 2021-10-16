package net.kdb4j;

import net.kdb4j.utils.TimeUtils;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class TimeUtilsTest {
    @Test
    public void testKdbTimeStampFromNanos() {

        assertEquals(0, TimeUtils.toKdbTimestamp(getEpochNanos("2000-01-01T00:00:00.00Z")));
        assertEquals(86400000000000L, TimeUtils.toKdbTimestamp(getEpochNanos("2000-01-02T00:00:00.00Z")));
        assertEquals(86400340000000L, TimeUtils.toKdbTimestamp(getEpochNanos("2000-01-02T00:00:00.34Z")));
        assertEquals(-315446399660000000L, TimeUtils.toKdbTimestamp(getEpochNanos("1990-01-02T00:00:00.34Z")));
    }


    public static long getEpochNanos(String text) {
        return TimeUtils.epochMillisToNanos(Instant.parse(text).toEpochMilli());
    }
}
