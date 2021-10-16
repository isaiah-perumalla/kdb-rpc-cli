package system;

import kx.TestUtils;
import net.kdb4j.KdbConnection;
import net.kdb4j.KdbEventHandler;
import net.kdb4j.codecs.KdbEncoder;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.util.Arrays;

public class TestDataPublisher implements KdbEventHandler {
    private  final MutableDirectBuffer writeBuffer = new ExpandableDirectByteBuffer(4096);
    private KdbConnection currentSession;
    private boolean closed;

    @Override
    public int onConnected(KdbConnection c) {
        this.currentSession = c;
        return publishData(c);
    }

    @Override
    public int onDisconnected(KdbConnection c) {
        this.closed = true;
        System.err.println("session disconnected ");
        return 0;
    }

    private  int publishData(KdbConnection k) {
        if(k.isWritable()) {

            MutableDirectBuffer buff = writeBuffer;
//            zeros(writeBuffer);
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
        zeros(buffer);
        int size = KdbEncoder.encodeRpcCall(buffer, offset, ByteOrder.LITTLE_ENDIAN, ".u.upd".toCharArray(), "quote", objects);
        return size;
    }

    public void publish() {
        if(currentSession != null)
            publishData(currentSession);

    }

    public boolean isClosed() {
        return this.closed;
    }
}
