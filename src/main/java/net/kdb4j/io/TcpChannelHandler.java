package net.kdb4j.io;

import org.agrona.DirectBuffer;

public interface TcpChannelHandler {
    int onConnected(TcpEndPoint tcpEndPoint);

    int onBytesReceived(TcpEndPoint tcpEndPoint, DirectBuffer directBuffer, int offset, int length);

    int onDisconnected(TcpEndPoint tcpEndPoint);
}
