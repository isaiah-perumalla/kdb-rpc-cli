package system;

import core.io.TcpTransportPoller;
import core.kdb.KdbConnectionAdapter;
import core.kdb.KdbEncoder;
import core.kdb.KdbEventHandler;
import core.kdb.TimeUtils;
import kx.c;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class SystemTests {

    public static void main(String[] args) throws IOException, InterruptedException {
        KdbEventHandler kdbHandler = (k) -> {

            if(k.isWritable()) {
                MutableDirectBuffer writeBuffer = k.getWritBuffer();
                int size = writeData(writeBuffer, 0);
                return k.write(0, size);
            }
            return 0;

        };
        KdbConnectionAdapter connectionHandler = new KdbConnectionAdapter("isaiahp", kdbHandler);
        TcpTransportPoller tcpTransportPoller = new TcpTransportPoller();

        tcpTransportPoller.addEndpoint("localhost", 5010, connectionHandler);
        while(true) {
            tcpTransportPoller.pollEndpoints();
            Thread.sleep(200);
        }



    }

    private static int writeData(MutableDirectBuffer buffer, int offset) {
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

//        c.k("");

        TimeUtils.TimespanVector tsVector = new TimeUtils.TimespanVector(time.length);
        for (int i = 0; i < time.length; i++) {
            tsVector.setKdbTimespanAt(i, time[i].j);
        }

        int size = KdbEncoder.encodeRpcCall(buffer, offset, ".u.upd".toCharArray(), "quote",  new Object[]{tsVector, sym, bid, ask, bsize, asize});
        return size;
    }
}
