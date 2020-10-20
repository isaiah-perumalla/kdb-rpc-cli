package kx;

import core.kdb.TimeUtils;
import kx.c;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {
    public static TimeUtils.TimespanVector getTimespanVector(c.Timespan[] time) {
        TimeUtils.TimespanVector tsVector = new TimeUtils.TimespanVector(time.length);
        for (int i = 0; i < time.length; i++) {
            tsVector.setKdbTimespanAt(i, time[i].j);
        }
        return tsVector;
    }

    public static Object[] generateData(boolean useTimespan) {
        // Bulk row insert - more efficient
        String[] syms = new String[]{"AAPL", "AMZN", "MSFT", "GOOG", "GOOGL", "BABA", "FB", "TSM", "NVDA", "TSLA", "JNJ", "WMT",
                "NSRGF", "NFLX", "JPM"};
        // Allocate one array per column
        c.Timespan[] time = new c.Timespan[10];
        String[] sym = new String[10];
        double[] bid = new double[10];
        double[] ask = new double[10];
        long[] bsize = new long[10];
        long[] asize = new long[10];
        char[] mode = new char[10];
        char[] ex = new char[10];
        // populate the arrays with sample data
        Timestamp[] ts = new Timestamp[10];
        for (int i = 0; i < 10; i++) {
            time[i] = new c.Timespan();
            ThreadLocalRandom current = ThreadLocalRandom.current();
            int s = i +1;
            sym[i] = syms[current.nextInt(0, syms.length)]; // choose a random symbol
            bid[i] = s*current.nextDouble();
            ask[i] = s*current.nextDouble();
            bsize[i] = s*100;
            asize[i] = s*200;
            mode[i] = 'c';
            ex[i] = 'b';
            ts[i] = new Timestamp(Instant.now().toEpochMilli());
        }
        // Note that we don't need to supply a flip with columns names for .u.upd.
        // Just the column data in the correct order is sufficient.

//        c.k("");


        Object timespanVector = useTimespan ? time : getTimespanVector(time);
        return new Object[]{timespanVector, sym, bid, ask, bsize, asize};
    }
}
