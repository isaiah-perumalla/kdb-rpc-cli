package core.kdb;

import org.agrona.MutableDirectBuffer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public enum KdbType {
    Dict(99, -1),
    Table(98, -1),
    List(0, -1),
    Sym(11, -1),
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


    KdbType(int typeCode, int fixedSz) {
        this.typeCode = (short) typeCode;
        this.primitiveSize = (byte) fixedSz;
    }

    public byte type() {
        return (byte) typeCode;
    }

    public byte scalarTypeCode() {
        return (byte) (-1 * type());
    }
}
