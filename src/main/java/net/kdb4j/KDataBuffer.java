package net.kdb4j;

import net.kdb4j.codecs.KdbType;
import net.kdb4j.codecs.ListDecoder;
import net.kdb4j.utils.TimeUtils;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.Collection;

/**
 * wraps a subsequence of bytes in a DirectBuffer
 * to read common kdb types
 */
public class KDataBuffer {
    private final static KSymbol.MutableSym mutableSym = new KSymbol.MutableSym();
    private DirectBuffer directBuffer;
    private int offset;
    private ByteOrder byteOrder;
    private KdbType type;
    private static final ListDecoder listDecoder = new ListDecoder();

    public int readTimestampVector(TimeUtils.TimestampVector timestampVector) {
        checkKdbType(offset, directBuffer, KdbType.Timestamp);
        final int size = directBuffer.getInt(offset + 2, byteOrder);
        int pos = offset + 6;
        for (int i = 0; i < size; i++) {
            final long ts = directBuffer.getLong(pos, byteOrder);
            timestampVector.setKdbTsAt(i, ts);
            pos += KdbType.Timestamp.primitiveSize();
        }
        return size;
    }

    public void wrap(DirectBuffer directBuffer, int offset, ByteOrder byteOrder) {

        this.directBuffer = directBuffer;
        this.offset = offset;
        this.byteOrder = byteOrder;
        this.type = KdbType.fromByte(directBuffer.getByte(offset));
    }
    public String readSym() {
        return KSymbol.read(directBuffer, offset);
    }

    public KdbType type() {
        return type;
    }

    public void decodeTable(MutableTable mutableTable) {
        if(type() != KdbType.Table) {
            throw new IllegalArgumentException("not a table");
        }
        int i = offset + 3;
        checkKdbType(i, directBuffer, KdbType.Sym);

        int pos = readSymVector(directBuffer, i, mutableTable.mutableCols(), byteOrder);
        //list of col data vectors
        assert KdbType.List == KdbType.fromByte(directBuffer.getByte(pos)) : "table binary format error expected list of col data";
        listDecoder.wrap(directBuffer, pos, byteOrder);
        //data read col vectors
        final int size = listDecoder.size();
        for (int j = 0; j < size; j++) {
            KDataBuffer kdata = listDecoder.itemAt(j);
            mutableTable.addColVector(kdata);
        }
    }

    public static void checkKdbType(int i, DirectBuffer directBuffer, KdbType expectedType) {
        KdbType kdbType = KdbType.fromByte(directBuffer.getByte(i));
        if(kdbType != expectedType) {
            throw new IllegalStateException("expected type " + expectedType + " but was " + kdbType);
        }
    }

    public int readSymVector(Collection<String> symVector) {
        return readSymVector(this.directBuffer, offset, symVector, byteOrder);

    }
    public static int readSymVector(DirectBuffer directBuffer, int offset, Collection<String> vector, ByteOrder byteOrder) {
        checkKdbType(offset, directBuffer, KdbType.Sym);
        vector.clear();
        final int length = directBuffer.getInt(offset + 2, byteOrder);
        int pos = offset+2+4;
        for (int i = 0; i < length; i++) {
            mutableSym.clear();
            byte ch;
            while(( ch = directBuffer.getByte(pos++)) != 0) {
                mutableSym.append((char)ch);
            }
            vector.add(KSymbol.getSym(mutableSym));
        }
        return pos;

    }

    public int vectorLength() {
        assert this.type.type() > 0 ;
        return directBuffer.getInt(offset + 2, byteOrder);
    }

    public int readDoubleVector(double[] doubleVector) {
        checkKdbType(offset, directBuffer, KdbType.Double);
        return getDoubleVector(doubleVector, directBuffer, offset, byteOrder);
    }

    public static int getDoubleVector(double[] doubleVector, DirectBuffer directBuffer, int offset, ByteOrder byteOrder) {
        final int size = directBuffer.getInt(offset + 2, byteOrder);
        int pos = offset + 6;
        for (int i = 0; i < size; i++) {
            final double d = directBuffer.getDouble(pos, byteOrder);
            doubleVector[i] = d;
            pos += KdbType.Double.primitiveSize();
        }
        return size;
    }

    public int readLongVector(long[] longVector, int j) {
        int size = directBuffer.getInt(offset + 2, byteOrder);
        int pos = offset + 6;
        for (int i = 0; i < size; i++) {
            final long v = directBuffer.getLong(pos, byteOrder);
            longVector[j+i] = v;
            pos += KdbType.Long.primitiveSize();
        }
        return size;
    }

    public int readCharVector(char[] charVector, int j) {
        final int size = directBuffer.getInt(offset + 2, byteOrder);
        int pos = offset + 6;
        for (int i = 0; i < size; i++) {
            final char c = (char) directBuffer.getByte(pos);
            charVector[i+j] = c;
            pos += KdbType.Char.primitiveSize();
        }
        return size;
    }


}
