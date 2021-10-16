package net.kdb4j.io;

import org.agrona.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class TcpEndPoint implements TcpEndpointSender {
    private static final int SOCKET_CLOSED = -1;
    private final InetSocketAddress remote;
    final SocketChannel channel;
    private final TcpChannelHandler connectionHandler;
    private final TcpTransportPoller tcpPoller;

    private final MutableDirectBuffer readBuffer = new ExpandableArrayBuffer(4096);
    private int pendingBytes;
    private final int id;

    public TcpEndPoint(InetSocketAddress remote, SocketChannel channel, TcpTransportPoller tcpTransportPoller, TcpChannelHandler connectionHandler, int id) {

        this.remote = remote;
        this.channel = channel;
        this.connectionHandler = connectionHandler;
        this.tcpPoller = tcpTransportPoller;
        this.id = id;
    }

    public int onConnected() {
        if(!channel.isConnected()) {
            try {
                if(!channel.finishConnect()) return 0;
            } catch (IOException e) {
                LangUtil.rethrowUnchecked(e);
            }
        }
        tcpPoller.registerForRead(this);
        return connectionHandler.onConnected(this);

    }

    public int write(DirectBuffer buffer, int offset, int length)  {
        ByteBuffer byteBuff = buffer.byteBuffer();
        if(byteBuff == null) {
            byteBuff = ByteBuffer.wrap(buffer.byteArray());
        }
        final int startLimit = byteBuff.limit();
        final int startPosition = byteBuff.position();

        try {
            final int written = writeByteBuff(byteBuff, offset, length);
            final int pending = length - written;
            return written;
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;
    }

    private int writeByteBuff(ByteBuffer byteBuff, int offset, int length) throws IOException {
        ByteBufferUtil.limit(byteBuff, offset + length);
        ByteBufferUtil.position(byteBuff, offset);
        final int written = channel.write(byteBuff);

        if(written < length) {
            pendingWrite(byteBuff, offset, length, written);
            tcpPoller.registerForWrite(this);

        }
        return written;
    }

    private void pendingWrite(ByteBuffer buffer, int offset, int length, int written) {
        this.pendingBytes = length - written;
        throw new UnsupportedOperationException("not all bytes written");
    }


    public int onRead() {
        try {

            ByteBuffer byteBuffer = ByteBuffer.wrap(readBuffer.byteArray());
            final int read = channel.read(byteBuffer);
            if(read == SOCKET_CLOSED) {
                channel.close();
                return connectionHandler.onDisconnected(this);
            }
            else if (read > 0){
                return connectionHandler.onBytesReceived(this, readBuffer, 0, read);
            }
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;
    }


    @Override
    public boolean isWritable() {
        return pendingBytes == 0;
    }
}
