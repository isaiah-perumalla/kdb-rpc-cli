package core.io;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface TcpEndpointSender {
    int write(int offset, int length);

    MutableDirectBuffer getWritBuffer();

    boolean isWritable();
}
