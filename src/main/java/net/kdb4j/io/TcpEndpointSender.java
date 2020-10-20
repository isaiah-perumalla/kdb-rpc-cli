package net.kdb4j.io;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface TcpEndpointSender {
    int write(DirectBuffer buffer, int offset, int length);
    boolean isWritable();


}
