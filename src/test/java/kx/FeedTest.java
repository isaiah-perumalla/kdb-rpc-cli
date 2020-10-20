package kx;

import net.kdb4j.codecs.KdbEncoder;
import net.kdb4j.utils.TimeUtils;
import org.agrona.ExpandableArrayBuffer;

import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class FeedTest {

    public static void main(String[] args) {// example tick feed
        c c = null;
        try {
            c = new c("localhost", 5010);
            // Single row insert - not as efficient as bulk insert
           /* for (int i = 0; i < 10; i++) {
                // Assumes a remote schema of quote:([]time:timespan$(); sym:`g#`symbol$(); bid:`float$(); ask:`float$(); bsize:`long$(); asize:`long$(); mode:`char$(); ex:`char$()
                Object[] row = {new c.Timespan(), "SYMBOL", new Double(93.5), new Double(93.9), new Long(300), new Long(500), 'c', 'b'};
                c.ks(".u.upd", "quote", row);
            }*/
            // Bulk row insert - more efficient
            String[] syms = new String[]{"ABC", "DEF", "GHI", "JKL"}; // symbols to randomly choose from
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
                sym[i] = syms[current.nextInt(0, syms.length)]; // choose a random symbol
                bid[i] = i*current.nextDouble();
                ask[i] = i*current.nextDouble();
                bsize[i] = i*100;
                asize[i] = i*200;
                mode[i] = 'c';
                ex[i] = 'b';
                ts[i] = new Timestamp(Instant.now().toEpochMilli());
            }
            // Note that we don't need to supply a flip with columns names for .u.upd.
            // Just the column data in the correct order is sufficient.
            c.ks(".u.upd", "quote",  new Object[]{time, sym, bid, ask, bsize, asize});
            c.k("");
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
            TimeUtils.TimespanVector tsVector = new TimeUtils.TimespanVector(time.length);
            for (int i = 0; i < time.length; i++) {
                tsVector.setKdbTimespanAt(i, time[i].j);
            }
            decorateSym(sym, "-encoder");
            int size = KdbEncoder.encodeRpcCall(buffer, 0, ByteOrder.LITTLE_ENDIAN, ".u.upd".toCharArray(), "quote",  new Object[]{tsVector, sym, bid, ask, bsize, asize});
            c.writeSocket(buffer.byteArray(), 0, size);
            // if we did want to supply a flip, it can be done as
//            c.ks(".u.upd", "quote", new c.Flip(new c.Dict(cols, new Object[]{time, sym, bid, ask, bsize, asize, mode, ex})));
            c.k(""); // sync chase ensures the remote has processed all msgs
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                c.close();
            } catch (java.io.IOException e) {
            }
        }
    }

    private static void decorateSym(String[] sym, String s) {
        for (int i = 0; i < sym.length; i++) {
            sym[i] += s;
        }
    }

}
