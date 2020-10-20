package system;

import core.io.TcpTransportPoller;
import core.kdb.KdbConnectionAdapter;
import core.kdb.KdbEncoder;
import core.kdb.KdbEventHandler;
import kx.TestUtils;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

public class SystemTests {

    private  static MutableDirectBuffer writeBuffer = new ExpandableDirectByteBuffer(4096);
    public static void main(String[] args) throws IOException, InterruptedException {
        KdbEventHandler kdbHandler = (k) -> publishData(k);
        KdbConnectionAdapter connectionHandler = new KdbConnectionAdapter("isaiahp", kdbHandler);
        TcpTransportPoller tcpTransportPoller = new TcpTransportPoller();

        tcpTransportPoller.addEndpoint("localhost", 5010, connectionHandler);
        while(true) {
            tcpTransportPoller.pollEndpoints();
            Thread.sleep(2000);
            publishData(connectionHandler);

        }



    }

    private static int publishData(KdbConnectionAdapter k) {
        if(k.isWritable()) {

            MutableDirectBuffer buff = writeBuffer;
            zeros(writeBuffer);
            int size = writeData(buff, 0);
            int write = k.write(buff, 0, size);
            write += k.writeSync();
            return write;
        }
        return 0;
    }

    private static void zeros(MutableDirectBuffer writeBuffer) {
        if(null != writeBuffer.byteArray()) {
            Arrays.fill(writeBuffer.byteArray(), (byte) 0);
        }
        else {
            for (int i = 0; i < writeBuffer.capacity(); i++) {
                writeBuffer.putByte(i, (byte) 0);
            }
        }
    }

    private static int writeData(MutableDirectBuffer buffer, int offset) {
        Object[] objects = TestUtils.generateData(false);
        int size = KdbEncoder.encodeRpcCall(buffer, offset, ByteOrder.LITTLE_ENDIAN, ".u.upd".toCharArray(), "quote", objects);
        return size;
    }

}
