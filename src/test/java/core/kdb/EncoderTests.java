package core.kdb;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

import java.io.IOException;

import static core.kdb.KdbEncoder.bytesToHex;
import static org.junit.Assert.assertEquals;

public class EncoderTests {

    @Test
    public void testEncodeSymbolArray() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        String[] sym = {"time", "bid", "ask"};
        int size = KdbEncoder.encodeSymArray(buffer, 0, sym);
        assertEquals(19, size);
        assertEquals("0B000300000074696D65006269640061736B00", bytesToHex(buffer.byteArray(), 0, size));

    }

    @Test
    public void testEncodeDoubleArray() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        double[] vector = {100.5d, 200, 5000};
        int size = KdbEncoder.encodeDoubleArray(buffer, 0, vector);
        assertEquals(30, size);
        assertEquals("09000300000000000000002059400000000000006940000000000088B340", bytesToHex(buffer.byteArray(), 0, size));

    }

    @Test
    public void testEncodeLongArray() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        long[] vector = {100L, 200, 5000000, -1000};
        int size = KdbEncoder.encodeLongArray(buffer, 0, vector);
        assertEquals(38, size);
        assertEquals("0700040000006400000000000000C800000000000000404B4C000000000018FCFFFFFFFFFFFF", bytesToHex(buffer.byteArray(), 0, size));

    }

    @Test
    public void testEncodeCharArray() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        char[] vector = {'c', 'a', 'z'};
        int size = KdbEncoder.encodeCharArray(buffer, 0, vector);
        assertEquals(9, size);
        assertEquals("0A000300000063617A", bytesToHex(buffer.byteArray(), 0, size));

    }

    @Test
    public void testEncodeTimespanVector() {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        long[] vector = new long[] { 1L, 2L, 655994693693813000L};
        TimeUtils timespanEncoder = new TimeUtils();
        int size = KdbEncoder.encodeNanosToTimspan(buffer, 0, vector);
        assertEquals(30, size);
        assertEquals("10000300000001000000000000000200000000000000083129C4B28F1A09", bytesToHex(buffer.byteArray(), 0, size));
    }

    @Test
    public void testEncodeRpcCall() {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);


        long[] time = new long[]{1, 2, 1000_000L};
        String[] sym = new String[]{"abc", "efg", "xyz"};
        int size = KdbEncoder.encodeRpcCall(buffer, 0, ".u.upd".toCharArray(), "quote",  new Object[]{time, sym});
        assertEquals(87, size);
        String bytesToHex = bytesToHex(buffer.byteArray(), 0, size);
        String expected = "01000000570000000000030000000A00060000002E752E757064F571756F7465000000020000000700030000000100000000000000020000000000000040420F00000000000B0003000000616263006566670078797A00";
        assertEquals(pprint(bytesToHex), expected, bytesToHex);

    }

    private String pprint(String bytesToHex) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytesToHex.length(); ) {
            sb.append(bytesToHex.charAt(i));
            sb.append(bytesToHex.charAt(i+1));
            i +=2;
            sb.append(";");
        }
        return sb.toString();
    }

    @Test
    public void testEncodeList() {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        Object[] vector = new Object[] { "u.upd", "quote", new long[] {1, 2, 1000_000L}};

        int size = KdbEncoder.encodeList(buffer, 0, vector);
        assertEquals(50, size);
        assertEquals("000003000000F5752E75706400F571756F7465000700030000000100000000000000020000000000000040420F0000000000", bytesToHex(buffer.byteArray(), 0, size));
    }

}
