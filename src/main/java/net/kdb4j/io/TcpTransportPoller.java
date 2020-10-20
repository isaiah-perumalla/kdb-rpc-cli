package net.kdb4j.io;

import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    void registerForWrite(TcpEndPoint tcpEndPoint) throws ClosedChannelException {
        tcpEndPoint.channel.register(selector, SelectionKey.OP_WRITE, tcpEndPoint);
    }
    void registerForRead(TcpEndPoint tcpEndPoint) {

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

        TcpEndPoint tcpEndPoint = new TcpEndPoint(remote, channel, this,  connectionHandler);
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


}
