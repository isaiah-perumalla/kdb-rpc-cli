package net.kdb4j;

import kx.TestUtils;
import kx.c;
import net.kdb4j.codecs.KdbEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class TickerPlantEncoderTest {

    private static final kx.c k = new kx.c();

    @Test
    public void testEncodeRpcCall() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        Object[] vector = new Object[]{new long[]{1, 2, 1000_000L}};

        char[] function = ".u.upd".toCharArray();
        int size = KdbEncoder.encodeRpcCall(buffer, offset, ByteOrder.BIG_ENDIAN, function, "quote", vector);
        byte[] expectedBytes = k.serialize(0, new Object[] {function, "quote", vector}, false);

        Assert.assertEquals(ByteUtils.bytesToHex(expectedBytes, 0, expectedBytes.length), ByteUtils.bytesToHex(buffer.byteArray(), 0, size));
    }

    @Test
    public void testEncodeRpcCalls() throws IOException {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
        int offset = 0;
        for (int i = 0; i < 1000; i++) {
            Arrays.fill(buffer.byteArray(), (byte) 0);

            Object[] data = TestUtils.generateData(true);

            char[] function = ".u.upd".toCharArray();
            byte[] expectedBytes = k.serialize(0, new Object[] {function, "quote", data}, false);
            data[0] = TestUtils.getTimespanVector((c.Timespan[]) data[0]);
            int size = KdbEncoder.encodeRpcCall(buffer, offset, ByteOrder.BIG_ENDIAN, function, "quote", data);


            Assert.assertEquals(ByteUtils.bytesToHex(expectedBytes, 0, expectedBytes.length), ByteUtils.bytesToHex(buffer.byteArray(), 0, size));
        }

    }
}
