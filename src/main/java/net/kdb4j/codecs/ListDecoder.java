package net.kdb4j.codecs;

import net.kdb4j.KDataBuffer;
import net.kdb4j.KSymbol;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

public class ListDecoder {
    private static final int HEADER_SIZE = 8;
    private static final int NOT_ENOUGH_DATA = -1;
    private static final int MSG_SIZE_OFFSET = 4;
    private static final int LIST_DATA_OFFSET = 6;
    private int[] itemOffsets = new int[16];
    //    private final KdbListDecoder listDecoder = new KdbListDecoder();
    private DirectBuffer directBuffer;
    private int offset;
    private ByteOrder byteOrder;
    private final KDataBuffer kdata = new KDataBuffer();

    public int decode(DirectBuffer directBuffer, int offset, int length, MessageReceiver messageReceiver) {
        if(length <= 8) {
            return NOT_ENOUGH_DATA;
        }
        ByteOrder byteOrder = directBuffer.getByte(offset) == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        final int messageSize = directBuffer.getInt(offset + MSG_SIZE_OFFSET, byteOrder);
        
        //list attr
        //00 – none, 01 – s, 02 – u, 03 – p, 04 – g
        //s -sorted, u- unique, - p grouped -g true index
       
        if(directBuffer.getByte(offset + HEADER_SIZE) == KdbType.List.type()) {
//            listDecoder.wrap(directBuffer, offset + HEADER_SIZE, length - HEADER_SIZE, byteOrder);
//            messageReceiver.receiveList(listDecoder);
        }
        return messageSize;
    }

    public void wrap(DirectBuffer directBuffer, int offset, ByteOrder byteOrder) {
        this.directBuffer = directBuffer;
        this.offset = offset;
        this.byteOrder = byteOrder;
        KDataBuffer.checkKdbType(offset, directBuffer, KdbType.List);
        buildItemOffsets();
    }

    private void buildItemOffsets() {
        int size = this.size();
        if(this.itemOffsets.length < size) {
            this.itemOffsets = new int[BitUtil.findNextPositivePowerOfTwo(size)];
        }
        int itemOffset = LIST_DATA_OFFSET;
        for (int i = 0; i < size; i++) {
            this.itemOffsets[i] = itemOffset;
            itemOffset += sizeOfDataType(directBuffer, offset + itemOffset, byteOrder);

        }



    }

    private int sizeOfDataType(DirectBuffer directBuffer, int offset, ByteOrder byteOrder) {
        KdbType t = KdbType.getTypeAt(directBuffer, offset);
        switch (t) {
            case SymAtom:
                return KdbType.sizeOfSymAtom(directBuffer, offset);
            case Table:
                return tableSize(directBuffer, offset, byteOrder);
            case Timestamp:
            case Double:
            case Long:
            case Int:
            case Char:
            case Byte:
            case Bool:
                return sizeOfVector(directBuffer, offset, t);
            case Sym:
                return sizeOfSymVector(directBuffer, offset);
            default:
                return t.primitiveSize();
        }
    }

    private int sizeOfSymVector(DirectBuffer directBuffer, int offset) {
        KDataBuffer.checkKdbType( offset, directBuffer, KdbType.Sym);
        int pos = offset + 2;
        final int length = directBuffer.getInt(pos, byteOrder);
        pos += 4;
        int size = 6; //type byte + attrByte + 4 byte int
        for (int i = 0; i < length; i++) {
            byte ch;
            do {
                ch = directBuffer.getByte(pos);
                size++;
                pos++;
            }while(ch != 0);
        }
        return size;
    }

    private int sizeOfVector(DirectBuffer directBuffer, int offset, KdbType t) {
        final int itemSize = t.primitiveSize();
        assert itemSize > 0 : "primitive size not specified";
        int index = offset + 2;
        final int length = directBuffer.getInt(index, byteOrder);
        return (length * itemSize) + 6; //4 byte length + byte for type + byte for atrributes
    }

    private int tableSize(DirectBuffer directBuffer, int offset, ByteOrder byteOrder) {
        return 0;
    }

    public KdbType getDataType() {
        return KdbType.fromByte(directBuffer.getByte(offset));
    }

    /*
    size of list
     */
    public int size() {
        return directBuffer.getInt(offset+2, byteOrder);
    }

    public KdbType listType() {
        return KdbType.fromByte(directBuffer.getByte(offset+ LIST_DATA_OFFSET));
    }

    public KDataBuffer itemAt(int i) {
        kdata.wrap(directBuffer, itemOffsets[i] +offset, byteOrder);
        return kdata;
    }

    public String readSymAt(int i) {
        return KSymbol.read(directBuffer, offset + itemOffsets[i]);
    }
}
