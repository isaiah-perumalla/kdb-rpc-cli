package net.kdb4j.codecs;

import org.agrona.DirectBuffer;

public enum KdbType {
    Dict(99, -1),
    Table(98, -1),
    List(0, -1),
    Sym(11, -1),
    SymAtom(-11, -1),
    Long(7, 8),
    Date(14, 4),
    Timestamp(12, 8),
    Time(19, 4),
    Timespan(16, 8),
    Bool(1, 1),
    Byte(4, 1),
    Short(5, 2),
    Int(6, 4),
    Char(10, 1),
    Float(8, 4),
    Double(9, 8);

    private final short typeCode;
    private final byte primitiveSize;
    private static final KdbType[] TYPES = KdbType.values();

    KdbType(int typeCode, int fixedSz) {
        this.typeCode = (short) typeCode;
        this.primitiveSize = (byte) fixedSz;
    }

    public static KdbType fromByte(byte aByte) {
        for (int i = 0; i < TYPES.length; i++) {
            KdbType type = TYPES[i];
            if(type.type() == aByte) return type;
        }
        throw new IllegalArgumentException("unknown kdb type " + aByte);
    }

    public static KdbType getTypeAt(DirectBuffer directBuffer, int offset) {
        return fromByte(directBuffer.getByte(offset));
    }

    public static int sizeOfSymAtom(DirectBuffer directBuffer, int offset) {
        int pos = offset + 1;
        int count = 1;
        while(directBuffer.getByte(pos++) != 0) count++;
        return count + 1; //add 1 for null terminator
    }

    public byte type() {
        return (byte) typeCode;
    }

    public byte atomTypeCode() {
        return (byte) (-1 * type());
    }

    public int primitiveSize() {
        return primitiveSize;
    }
}
