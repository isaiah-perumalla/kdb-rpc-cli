package net.kdb4j.utils;

import net.kdb4j.codecs.KdbEncoder;
import net.kdb4j.codecs.KdbType;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*

  * Constructs {@code Timespan} using time since midnight and default timezone.
 */
public class TimeUtils {
    private static final TimeZone timeZone = TimeZone.getTimeZone("UTC");
    public static final long MID_NIGHT_NANOS;
    public static final long TIMESTAMP_OFFSET_NANOS = 946684800000L * 1000000L;


    static {
        Calendar c = Calendar.getInstance(timeZone);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        MID_NIGHT_NANOS = c.getTimeInMillis() * 1000000L;

    }

    public static long toKdbTimestamp(long nanoTime) {
        return nanoTime - TIMESTAMP_OFFSET_NANOS;
    }

    public static long toKdbTimestamp(String dateTime) {
        return toKdbTimestamp(TimeUtils.epochMillisToNanos(Instant.parse(dateTime).toEpochMilli()));
    }
    public static LocalTime epochMillsToInstant(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalTime();
    }
    public static long epochMillisToNanos(long epochMillis) {
        return epochMillis * 1000000L;
    }

    public static class TimespanVector {

        private final long[]  values;

        public TimespanVector(int size) {
            this.values = new long[size];
        }

        public int encode(MutableDirectBuffer buffer, int offset, ByteOrder order) {
            return KdbEncoder.encodeNanosToTimspan(buffer, offset, values, order);
        }

        public void setNanoTimeAt(int i, long nanos) {
            this.values[i] = nanos - MID_NIGHT_NANOS;
        }

        public void setKdbTimespanAt(int i, long kdbTimespan) {
            this.values[i] = kdbTimespan;
        }
    }

    public static class TimestampVector  {
        private final long[] values;

        public TimestampVector(int length) {
            this.values = new long[length];
        }

        public static long[] toArray(TimestampVector array) {
            TimestampVector a = (TimestampVector) array;
            return Arrays.copyOf(a.values, a.length());
        }

        public int encode(MutableDirectBuffer buffer, int offset, ByteOrder order) {
            return KdbEncoder.encodeNanosToTimestamp(buffer, offset, values, order);
        }

        public void setEpochNanoTimeAt(int i, long nanos) {
            this.values[i] = toKdbTimestamp(nanos);
        }

        public void setEpochMillisTimeAt(int i, long epochMillis) {
            this.values[i] = toKdbTimestamp(epochMillisToNanos(epochMillis));
        }


        public int length() {
            return this.values.length;
        }


        public KdbType type() {
            return KdbType.Timestamp;
        }

        public void setKdbTsAt(int i, long kdbTs) {
            this.values[i] = kdbTs;
        }

        @Override
        public String toString() {
            Stream<String> stringStream = Arrays.stream(values).mapToObj(ts -> epochMillsToInstant(TimeUnit.NANOSECONDS.toMillis(ts)).toString());
            String valuesStr = stringStream.collect(Collectors.joining(";"));
            return "TimestampVector{" +
                    "values=" + valuesStr + "}";
        }
    }
}
