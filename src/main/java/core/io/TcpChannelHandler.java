package core.io;

import org.agrona.DirectBuffer;

public interface TcpChannelHandler {
    int onConnected(TcpTransportPoller.TcpEndPoint tcpEndPoint);

    int onBytesReceived(TcpTransportPoller.TcpEndPoint tcpEndPoint, DirectBuffer directBuffer, int offset, int length);
}
