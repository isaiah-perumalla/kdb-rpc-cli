package net.kdb4j.codecs;

public interface MessageReceiver {
    void receiveList(KdbListDecoder listDecoder);
}
