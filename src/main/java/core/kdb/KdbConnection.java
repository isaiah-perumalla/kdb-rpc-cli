package core.kdb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class KdbConnection {
    private static String encoding = "ISO-8859-1";
    /*
    0	(V2.5) no compression, no timestamp, no timespan, no UUID
    1..2	(V2.6-2.8) compression, timestamp, timespan
    3	(V3.0) compression, timestamp, timespan, UUID
     */
    private final byte capabilities;
    private final SocketChannel channel;

    private KdbConnection(byte capabilities, SocketChannel channel) {
        this.channel = channel;
        if(capabilities > 3) {
            throw new UnsupportedOperationException("capabilities not supported " + capabilities);
        }
        this.capabilities = capabilities;
    }

    public static KdbConnection open(String host, int port, String creds) throws IOException {
        ByteBuffer buff = ByteBuffer.allocate(128);
        ByteBuffer readBuff = ByteBuffer.allocate(128);
        SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port));

        buff.put( (creds + "\3").getBytes(encoding));
        buff.put((byte) 0);
        buff.flip();

        while(buff.hasRemaining()) {
            channel.write(buff);
        }
        int size = channel.read(readBuff);

        if(size == -1) {
            channel.close();
            throw new RuntimeException("failed to connect ");
        }
        readBuff.flip();
        byte reply = readBuff.get();

        return null;
    }
}
