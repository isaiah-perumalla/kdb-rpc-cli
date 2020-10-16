package core.kdb;

import core.io.TcpChannelHandler;
import core.io.TcpTransportPoller;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;

public class KdbConnectionAdapter implements TcpChannelHandler {
    private final String creds;
    private final MutableDirectBuffer writeBuffer = new ExpandableDirectByteBuffer(1024);
    private final MutableDirectBuffer readBuffer = new ExpandableDirectByteBuffer(1024);
    private byte capabilities;
    private boolean isSessionConnected = false;
    private final KdbEventHandler kdbHandler;
    private TcpTransportPoller.TcpEndPoint tcpEndpoint;

    public KdbConnectionAdapter(String creds, KdbEventHandler kdbHandler) {

        this.creds = creds;
        this.kdbHandler = kdbHandler;
    }

    @Override
    public int onConnected(TcpTransportPoller.TcpEndPoint tcpEndPoint) {
        int size = KdbEncoder.encodeLogin(writeBuffer, 0, creds);
        try {
            return tcpEndPoint.write(writeBuffer, 0, size);
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;

    }

    @Override
    public int onBytesReceived(TcpTransportPoller.TcpEndPoint tcpEndPoint, DirectBuffer directBuffer, int offset, int length) {
        if(!this.isSessionConnected) {
            assert length ==  1;
            byte reply = directBuffer.getByte(offset);
            this.capabilities = reply;
            this.isSessionConnected = true;
            this.tcpEndpoint = tcpEndPoint;
            return this.kdbHandler.sessionConnected(this);
        }
        else {
            throw new UnsupportedOperationException("need to implement reply from kdb");
        }

    }

    public boolean isWritable() {
        return tcpEndpoint.isWritable();
    }

    public MutableDirectBuffer getWritBuffer() {
        return this.tcpEndpoint.getWritBuffer();
    }

    public int write(int offset, int length) {
        return this.tcpEndpoint.write(offset, length);
    }
}
