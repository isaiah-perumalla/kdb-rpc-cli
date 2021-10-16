package net.kdb4j.codecs;

import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

public class KdbListDecoder {
    private static final int LIST_SIZE_OFFSET = 2;
    private int listSize;

    public void wrap(DirectBuffer directBuffer, int offset, int length, ByteOrder byteOrder) {
        assert directBuffer.getByte(offset) == 0; //type is list
        listSize = directBuffer.getInt(offset + LIST_SIZE_OFFSET, byteOrder);
    }
    public void forEach(Consumer c) {

    }

    @FunctionalInterface
    public interface Consumer {

        void accept(int index, Item i);

    }

    public static class Item {

        public KdbType type() {
            return null;
        }
    }
}
