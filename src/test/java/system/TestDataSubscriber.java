package system;

import net.kdb4j.KdbConnection;
import net.kdb4j.KdbEventHandler;
import net.kdb4j.codecs.KdbEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;

import java.io.IOException;
import java.nio.ByteOrder;

public class TestDataSubscriber implements KdbEventHandler {
    final kx.c c = new kx.c();
    @Override
    public int onConnected(KdbConnection conn) {

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        int size = KdbEncoder.encodeRpcCall(buffer, 0, ByteOrder.LITTLE_ENDIAN, ".u.sub".toCharArray(), "quote", "");
        conn.write(buffer, 0, size);
        return size;

    }

    @Override
    public int onDisconnected(KdbConnection c) {
        System.err.println("disconnected ");
        return 0;
    }
}
