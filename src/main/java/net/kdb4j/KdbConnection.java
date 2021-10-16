package net.kdb4j;

import net.kdb4j.codecs.KdbEncoder;
import net.kdb4j.io.TcpChannelHandler;
import net.kdb4j.io.TcpEndPoint;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

public class KdbConnection implements TcpChannelHandler {
    private final String creds;
    private final MutableDirectBuffer writeBuffer = new ExpandableDirectByteBuffer(1024);
    private final MutableDirectBuffer readBuffer = new ExpandableDirectByteBuffer(1024);
    private byte capabilities;
    private boolean isSessionConnected = false;
    private final KdbEventHandler kdbHandler;
    private TcpEndPoint tcpEndpoint;
    private static final DirectBuffer SYNC_MSG = new UnsafeBuffer(KdbEncoder.SYNC_MSG);

    public KdbConnection(String creds, KdbEventHandler kdbHandler) {

        this.creds = creds;
        this.kdbHandler = kdbHandler;
    }

    @Override
    public int onConnected(TcpEndPoint tcpEndPoint) {
        int size = KdbEncoder.encodeLogin(writeBuffer, 0, creds);
        return tcpEndPoint.write(writeBuffer, 0, size);


    }

    @Override
    public int onBytesReceived(TcpEndPoint tcpEndPoint, DirectBuffer directBuffer, int offset, int length) {
        assert length > 0;
        if(!this.isSessionConnected) {
            assert length ==  1;
            byte reply = directBuffer.getByte(offset);
            this.capabilities = reply;
            this.isSessionConnected = true;
            this.tcpEndpoint = tcpEndPoint;
            return this.kdbHandler.onConnected(this);
        }
        else {
            if(-128 == directBuffer.getByte(offset + 8)) {

                StringBuilder sb = new StringBuilder();
                for (int i = 8; i < length; i++) {
                    sb.append((char)directBuffer.getByte(i+offset));

                }
                System.err.println("error " + sb.toString());
            }
            System.out.println(ByteUtils.bytesToHex(directBuffer, offset, length));
            return length;
        }

    }

    @Override
    public int onDisconnected(TcpEndPoint tcpEndPoint) {
        this.isSessionConnected = false;
        return  this.kdbHandler.onDisconnected(this);
    }

    public boolean isWritable() {
        return isSessionConnected && tcpEndpoint.isWritable();
    }



    public int writeSync() {
       /* try {
            this.tcpEndpoint.write(SYNC_MSG, 0, 14);
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return 14;*/
       return 0;
    }

    public int write(MutableDirectBuffer writeBuffer, int offset, int length) {
        return tcpEndpoint.write(writeBuffer, offset, length);
    }

    public int k(CharSequence command) {

        ExpandableArrayBuffer e = new ExpandableArrayBuffer();
        int size = KdbEncoder.encodeCharsequence(e, 0, command, ByteOrder.LITTLE_ENDIAN );
        return tcpEndpoint.write(e, 0, size);

    }
}
