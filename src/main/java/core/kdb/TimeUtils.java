package core.kdb;

import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.TimeZone;

/*

  * Constructs {@code Timespan} using time since midnight and default timezone.
 */
public class TimeUtils {
    private static final TimeZone timeZone = TimeZone.getTimeZone("UTC");
    public static final long MID_NIGHT_NANOS;
    private  final long refTime = MID_NIGHT_NANOS;


    static {
        Calendar c = Calendar.getInstance(timeZone);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        MID_NIGHT_NANOS = c.getTimeInMillis() * 1000000L;
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

}
