import org.agrona.DirectBuffer;

public class ByteUtils {
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

    public static String bytesToHex(DirectBuffer buffer, int offset, int length) {
        char[] hexChars = new char[length * 2];
        for (int j = offset; j < offset+length; j++) {
            int v = buffer.getByte(j) & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) * 16)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}
