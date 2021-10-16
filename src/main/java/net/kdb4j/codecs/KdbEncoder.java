package net.kdb4j.codecs;

import net.kdb4j.utils.TimeUtils;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class KdbEncoder {
    public static final int SizeOfType = 1;
    public static final int SIZE_OF_LENGTH = 4;
    private static final byte NULL_CHAR = (byte) 0;
    public static final int SIZE_OF_DOUBLE = 8;
    public static final int SIZE_OF_LONG = 8;
    private static final int SIZE_OF_TIMESPAN = 8;
    public static final String ISO_8859_1_ENCODING = "ISO-8859-1";
    public static final DirectBuffer SYNC_MSG = encodeSync(ByteOrder.LITTLE_ENDIAN);

    public static int encodeSymArray(MutableDirectBuffer buffer, int offset, String[] strings, ByteOrder byteOrder) {
        int position = offset;
        buffer.putByte(position, KdbType.Sym.type());
        position += SizeOfType + 1;
        buffer.putInt(position, strings.length, byteOrder);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < strings.length; i++) {
            String v = strings[i];
            for (int j = 0; j < v.length(); j++) {
                buffer.putByte(position++, (byte) v.charAt(j));
            }
            buffer.putByte(position++, NULL_CHAR);
        }
        return position - offset;
    }



    public static int encodeDoubleArray(MutableDirectBuffer buffer, int offset, double[] vector, ByteOrder BYTE_ORDER) {
        int position = offset;
        buffer.putByte(position, KdbType.Double.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length, BYTE_ORDER);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            double v = vector[i];
            buffer.putDouble(position, v, BYTE_ORDER);
            position += SIZE_OF_DOUBLE;
        }
        return position - offset;
    }

    public static int encodeLongArray(MutableDirectBuffer buffer, int offset, long[] vector, ByteOrder BYTE_ORDER) {
        int position = offset;
        buffer.putByte(position, KdbType.Long.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length, BYTE_ORDER);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            long v = vector[i];
            buffer.putLong(position, v, BYTE_ORDER);
            position += SIZE_OF_LONG;
        }
        return position - offset;
    }

    public static int encodeCharArray(MutableDirectBuffer buffer, int offset, char[] vector, ByteOrder byteOrder) {
        int position = offset;
        buffer.putByte(position, KdbType.Char.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length, byteOrder);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            char v = vector[i];
            buffer.putChar(position, v);
            position += 1;
        }
        return position - offset;
    }

    public static int encodeCharsequence(MutableDirectBuffer buffer, int offset, CharSequence seq, ByteOrder byteOrder) {
        int position = offset;
        buffer.putByte(position, KdbType.Char.type());
        position += SizeOfType + 1;
        buffer.putInt(position, seq.length(), byteOrder);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < seq.length(); i++) {
            char v = seq.charAt(i);
            buffer.putChar(position, v);
            position += 1;
        }
        return position - offset;
    }
    public static int encodeNanosToTimspan(MutableDirectBuffer buffer, int offset, long[] nanoTimes, ByteOrder order) {
        int position = offset;
        buffer.putByte(position, KdbType.Timespan.type());
        position += SizeOfType + 1;
        buffer.putInt(position, nanoTimes.length, order);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < nanoTimes.length; i++) {
            long t = nanoTimes[i];

            buffer.putLong(position, t, order);
            position += SIZE_OF_TIMESPAN;
        }
        return position - offset;
    }

    public static int encodeList(MutableDirectBuffer buffer, int offset, Object[] vector, ByteOrder byteOrder) {
        int position = offset;
        buffer.putByte(position, KdbType.List.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length, byteOrder);
        position += SIZE_OF_LENGTH;

        for (int i = 0; i < vector.length; i++) {
            Object v = vector[i];
            if(v instanceof String) {
                buffer.putByte(position, KdbType.Sym.atomTypeCode());
                position += SizeOfType;
                String s = (String) v;
                for (int j = 0; j < s.length(); j++) {
                    buffer.putByte(position, (byte) s.charAt(j));
                    position++;
                }
                buffer.putByte(position, NULL_CHAR);
                position++;
            }
            else if(v instanceof TimeUtils.TimespanVector) {
                TimeUtils.TimespanVector t = (TimeUtils.TimespanVector) v;
                int size = t.encode(buffer, position, byteOrder);
                position += size;
            }
            else if(v instanceof TimeUtils.TimestampVector) {
                TimeUtils.TimestampVector t = (TimeUtils.TimestampVector) v;
                int size = t.encode(buffer, position, byteOrder);
                position += size;
            }
            else if(v instanceof char[]) {
                char[] c = (char[]) v;
                position += encodeCharArray(buffer, position, c, byteOrder);
            }
            else if(v instanceof long[]) {
                long[] l = (long[]) v;
                position += encodeLongArray(buffer, position, l, byteOrder);
            }
            else if(v instanceof double[]) {
                double[] d = (double[]) v;
                position += encodeDoubleArray(buffer, position, d, byteOrder);
            }
            else if (v instanceof String[]) {
                String[] s = (String[]) v;
                position += encodeSymArray(buffer, position, s, byteOrder);
            }
            else if(v instanceof Object[]) {
                Object[] o = (Object[]) v;
                position += encodeList(buffer, position, o, byteOrder);
            }
            else {
                throw new UnsupportedOperationException("cannot encode type " + v.getClass());
            }

        }
        return position - offset;
    }

    public static int encodeRpcCall(MutableDirectBuffer buffer, int offset, ByteOrder byteOrder, Object... args) {
        int size = encodeList(buffer, offset + 8, args, byteOrder);
        encodeHeader(buffer, offset, size + 8, byteOrder);
        return size + 8;
    }

    public static void encodeHeader(MutableDirectBuffer buffer, int offset, int messageSize, ByteOrder byteOrder) {
        buffer.putByte(offset, encodeByteOrder(byteOrder)); //little endian
        buffer.putInt(offset + 4, messageSize, byteOrder);
    }

    private static byte encodeByteOrder(ByteOrder byteOrder) {
        return byteOrder == ByteOrder.LITTLE_ENDIAN ? (byte) 1 : 0;
    }

    public static DirectBuffer encodeSync(ByteOrder byteOrder) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(14));
        buffer.putByte(0, encodeByteOrder(byteOrder));
        buffer.putInt(4, 14, byteOrder);
                buffer.putByte(8, (byte) 10);
                buffer.putInt(10, 0, byteOrder);
        return buffer;

    }
    public static int encodeLogin(MutableDirectBuffer writeBuffer, int offset, String creds) {
        try {
            byte[] bytes = creds.getBytes(ISO_8859_1_ENCODING);
            writeBuffer.putBytes(offset, bytes);
            int written = bytes.length;
            writeBuffer.putByte(offset + written, (byte) 3);
            written += 1;
            writeBuffer.putByte(offset + written, NULL_CHAR);
            written += 1;
            return written;
        } catch (UnsupportedEncodingException e) {
            LangUtil.rethrowUnchecked(e);
        }
        return 0;
    }

    public static int encodeNanosToTimestamp(MutableDirectBuffer buffer, int offset, long[] nanoTimes, ByteOrder order) {
        int position = offset;
        buffer.putByte(position, KdbType.Timestamp.type());
        position += SizeOfType + 1;
        buffer.putInt(position, nanoTimes.length, order);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < nanoTimes.length; i++) {
            long t = nanoTimes[i];

            buffer.putLong(position, t, order);
            position += KdbType.Timestamp.primitiveSize();
        }
        return position - offset;
    }
}
