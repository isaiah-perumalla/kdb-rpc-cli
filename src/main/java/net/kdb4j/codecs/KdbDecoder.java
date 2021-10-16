package net.kdb4j.codecs;

import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

public class KdbDecoder {
    private static final int HEADER_SIZE = 8;
    private static final int NOT_ENOUGH_DATA = -1;
    private static final int MSG_SIZE_OFFSET = 4;
    private final KdbListDecoder listDecoder = new KdbListDecoder();

    public int decode(DirectBuffer directBuffer, int offset, int length, MessageReceiver messageReceiver) {
        if(length <= 8) {
            return NOT_ENOUGH_DATA;
        }
        ByteOrder byteOrder = directBuffer.getByte(offset) == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        final int messageSize = directBuffer.getInt(offset + MSG_SIZE_OFFSET, byteOrder);
        
        //list attr
        //00 – none, 01 – s, 02 – u, 03 – p, 04 – g
        //s -sorted, u- unique, - p grouped -g true index
       
        if(directBuffer.getByte(offset + HEADER_SIZE) == KdbType.List.atomTypeCode()) {
            listDecoder.wrap(directBuffer, offset + HEADER_SIZE, length - HEADER_SIZE, byteOrder);
            messageReceiver.receiveList(listDecoder);
        }
        return messageSize;
    }
}
