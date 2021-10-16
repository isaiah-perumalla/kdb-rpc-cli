package net.kdb4j;

public interface KdbEventHandler {
    int onConnected(KdbConnection c);

    int onDisconnected(KdbConnection c);
}
