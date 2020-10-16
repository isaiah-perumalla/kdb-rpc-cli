package core.kdb;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;

import java.io.UnsupportedEncodingException;

public class KdbEncoder {
    public static final int SizeOfType = 1;
    public static final int SIZE_OF_LENGTH = 4;
    private static final byte NULL_CHAR = (byte) 0;
    public static final int SIZE_OF_DOUBLE = 8;
    public static final int SIZE_OF_LONG = 8;
    private static final int SIZE_OF_TIMESPAN = 8;
    private static final String ENCODING = "ISO-8859-1";

    public static int encodeSymArray(MutableDirectBuffer buffer, int offset, String[] strings) {
        int position = offset;
        buffer.putByte(position, KdbType.Sym.type());
        position += SizeOfType + 1;
        buffer.putInt(position, strings.length);
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

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes, int offset, int length) {
        char[] hexChars = new char[length * 2];
        for (int j = offset; j < offset+length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static int encodeDoubleArray(MutableDirectBuffer buffer, int offset, double[] vector) {
        int position = offset;
        buffer.putByte(position, KdbType.Double.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            double v = vector[i];
            buffer.putDouble(position, v);
            position += SIZE_OF_DOUBLE;
        }
        return position - offset;
    }

    public static int encodeLongArray(MutableDirectBuffer buffer, int offset, long[] vector) {
        int position = offset;
        buffer.putByte(position, KdbType.Long.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            long v = vector[i];
            buffer.putLong(position, v);
            position += SIZE_OF_LONG;
        }
        return position - offset;
    }

    public static int encodeCharArray(MutableDirectBuffer buffer, int offset, char[] vector) {
        int position = offset;
        buffer.putByte(position, KdbType.Char.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < vector.length; i++) {
            char v = vector[i];
            buffer.putChar(position, v);
            position += 1;
        }
        return position - offset;
    }

    public static int encodeNanosToTimspan(MutableDirectBuffer buffer, int offset, long[] nanoTimes) {
        int position = offset;
        buffer.putByte(position, KdbType.Timespan.type());
        position += SizeOfType + 1;
        buffer.putInt(position, nanoTimes.length);
        position += SIZE_OF_LENGTH;
        for (int i = 0; i < nanoTimes.length; i++) {
            long t = nanoTimes[i];

            buffer.putLong(position, t);
            position += SIZE_OF_TIMESPAN;
        }
        return position - offset;
    }

    public static int encodeList(MutableDirectBuffer buffer, int offset, Object[] vector) {
        int position = offset;
        buffer.putByte(position, KdbType.List.type());
        position += SizeOfType + 1;
        buffer.putInt(position, vector.length);
        position += SIZE_OF_LENGTH;

        for (int i = 0; i < vector.length; i++) {
            Object v = vector[i];
            if(v instanceof String) {
                buffer.putByte(position, KdbType.Sym.scalarTypeCode());
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
                int size = t.encode(buffer, position);
                position += size;
            }
            else if(v instanceof char[]) {
                char[] c = (char[]) v;
                position += encodeCharArray(buffer, position, c);
            }
            else if(v instanceof long[]) {
                long[] l = (long[]) v;
                position += encodeLongArray(buffer, position, l);
            }
            else if(v instanceof double[]) {
                double[] d = (double[]) v;
                position += encodeDoubleArray(buffer, position, d);
            }
            else if (v instanceof String[]) {
                String[] s = (String[]) v;
                position += encodeSymArray(buffer, position, s);
            }
            else if(v instanceof Object[]) {
                Object[] o = (Object[]) v;
                position += encodeList(buffer, position, o);
            }
            else {
                throw new UnsupportedOperationException("cannot encode type " + v.getClass());
            }

        }
        return position - offset;
    }

    public static int encodeRpcCall(MutableDirectBuffer buffer, int offset,   Object ... args) {
        int size = encodeList(buffer, offset + 8, args);
        encodeHeader(buffer, offset, size + 8);
        return size + 8;
    }

    public static void encodeHeader(MutableDirectBuffer buffer, int offset, int messageSize) {
        buffer.putByte(offset, (byte) 1); //little endian
        buffer.putInt(offset + 4, messageSize);
    }

    public static int encodeLogin(MutableDirectBuffer writeBuffer, int offset, String creds) {
        try {
            byte[] bytes = creds.getBytes(ENCODING);
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
}
