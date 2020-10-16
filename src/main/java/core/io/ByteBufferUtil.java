package core.io;

import java.nio.ByteBuffer;

public class ByteBufferUtil {
    public static void position(ByteBuffer byteBuff, int newPosition) {
        try {
            byteBuff.position(newPosition);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("byteBuffer limit = " + byteBuff.limit() + ", position = " + newPosition, e);
        }
    }

    public static void limit(ByteBuffer byteBuff, int newLimit) {
        try {
            byteBuff.limit(newLimit);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("byteBuffer newLimit = " + newLimit + ", capacity  = " + byteBuff.capacity(), e);
        }
    }
}
