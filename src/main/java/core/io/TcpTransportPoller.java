package core.io;

import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.nio.TransportPoller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class TcpTransportPoller extends TransportPoller {
    private TcpEndPoint[] endPoints = new TcpEndPoint[0];

    public TcpTransportPoller() {

    }

    public void registerForOpen(TcpEndPoint channel) throws ClosedChannelException {
        channel.channel.register(selector, SelectionKey.OP_CONNECT, channel);

    }
    private void registerForWrite(TcpEndPoint tcpEndPoint) throws ClosedChannelException {
        tcpEndPoint.channel.register(selector, SelectionKey.OP_WRITE, tcpEndPoint);
    }
    private void registerForRead(TcpEndPoint tcpEndPoint) {

        try {
            tcpEndPoint.channel.register(selector, SelectionKey.OP_READ, tcpEndPoint);
        } catch (ClosedChannelException e) {
           LangUtil.rethrowUnchecked(e);
        }
    }

    public void addEndpoint(String host, int port, TcpChannelHandler connectionHandler) throws IOException {
        InetSocketAddress remote = new InetSocketAddress(host, port);
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);

        TcpTransportPoller.TcpEndPoint tcpEndPoint = new TcpTransportPoller.TcpEndPoint(remote, channel, this,  connectionHandler);
        endPoints = ArrayUtil.add(endPoints, tcpEndPoint);
        registerForOpen(tcpEndPoint);
        channel.connect(remote);
    }

    public int pollEndpoints() {
        int bytes = 0;
        try {
            selector.selectNow();
            SelectionKey[] keys = selectedKeySet.keys();
            for (int i = 0; i < selectedKeySet.size(); i++) {
                bytes += poll(keys[i]);
            }
            selectedKeySet.reset(); //all ready ops were dealt with so reset this set
        }
        catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return bytes;
    }

    private int poll(SelectionKey key) {
        Object attachment = key.attachment();
        TcpEndPoint endPoint = (TcpEndPoint) attachment;
        int bytes = 0;
        if(key.isConnectable()) {
            bytes += endPoint.onConnected();
        }
        if(key.isReadable()) {
            bytes += endPoint.onRead();
        }
        return bytes;
    }



    public static class TcpEndPoint implements TcpEndpointSender {
        private final InetSocketAddress remote;
        private final SocketChannel channel;
        private final TcpChannelHandler connectionHandler;
        private final TcpTransportPoller tcpPoller;
        private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
                4096, 64);
        private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        private final ByteBuffer writeByteBuff = BufferUtil.allocateDirectAligned(
                4096, 64);
        private final UnsafeBuffer writeBuffer = new UnsafeBuffer(writeByteBuff);

        public TcpEndPoint(InetSocketAddress remote, SocketChannel channel, TcpTransportPoller tcpTransportPoller, TcpChannelHandler connectionHandler) {

            this.remote = remote;
            this.channel = channel;
            this.connectionHandler = connectionHandler;
            this.tcpPoller = tcpTransportPoller;
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

        public int write(DirectBuffer buffer, int offset, int length) throws IOException {
            ByteBuffer byteBuff = buffer.byteBuffer();
            final int startLimit = byteBuff.limit();
            final int startPosition = byteBuff.position();
            ByteBufferUtil.limit(byteBuff, offset + length);
            ByteBufferUtil.position(byteBuff, offset);
            final int written = channel.write(byteBuff);
            byteBuff.limit(startLimit).position(startPosition);
            if(written < length) {
                pendingWrite(buffer, offset, length, written);
                tcpPoller.registerForWrite(this);

            }
            return written;
        }

        private void pendingWrite(DirectBuffer buffer, int offset, int length, int written) {

        }


        public int onRead() {
            try {
                byteBuffer.clear();
                int read = channel.read(byteBuffer);
                return connectionHandler.onBytesReceived(this, unsafeBuffer, 0, read);
            } catch (IOException e) {
                LangUtil.rethrowUnchecked(e);
            }
            return 0;
        }

        @Override
        public int write(int offset, int length) {
            try {
                return write(this.writeBuffer, offset, length);
            } catch (IOException e) {
                LangUtil.rethrowUnchecked(e);
            }
            return 0;
        }

        @Override
        public MutableDirectBuffer getWritBuffer() {
            return this.writeBuffer ;
        }

        @Override
        public boolean isWritable() {
            return true;
        }
    }




}
