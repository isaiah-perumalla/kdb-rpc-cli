package net.kdb4j;

import org.agrona.DirectBuffer;

public interface KdbEventHandler {
    int onConnected(KdbConnection c);

    int onDisconnected(KdbConnection c);

    void onError(DirectBuffer directBuffer, int offset, int length);

    void onMessage(DirectBuffer directBuffer, int offset, int length);
}
