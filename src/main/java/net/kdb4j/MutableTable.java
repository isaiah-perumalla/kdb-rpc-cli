package net.kdb4j;

import net.kdb4j.codecs.KdbType;
import net.kdb4j.utils.ArrayPool;
import net.kdb4j.utils.TimeUtils;

import java.util.ArrayList;
import java.util.Collection;

public class MutableTable {
    private final ArrayList<String> colsNames = new ArrayList<>(64);
    private final ArrayList<KdbType> types = new ArrayList<>(64);
    private int size = 0;
    private final ArrayList<Object> colsVector = new ArrayList();
    private final ArrayPool arrayPool = new ArrayPool();

    public Collection<String> mutableCols() {
        return this.colsNames;
    }

    public void addColVector(KDataBuffer kDataBuffer) {
        KdbType type = kDataBuffer.type();
        types.add(type);
        switch (type) {
            case Timestamp:
                TimeUtils.TimestampVector timestampVector = arrayPool.takeTimestamp(kDataBuffer.vectorLength());
                colsVector.add(timestampVector);
                kDataBuffer.readTimestampVector(timestampVector);
                break;
            case Sym:
                Collection<String> symVector = arrayPool.takeStringCollection();
                kDataBuffer.readSymVector(symVector);
                colsVector.add(symVector);
                break;
            case Double:
                double[] doubleArray = arrayPool.takeDouble(kDataBuffer.vectorLength());
                kDataBuffer.readDoubleVector(doubleArray);
                colsVector.add(doubleArray);
                break;
            case Long:
                long[] longArray = arrayPool.takeLong(kDataBuffer.vectorLength());
                kDataBuffer.readLongVector(longArray, 0);
                colsVector.add(longArray);
                break;
            case Char:
                char[] charArray = arrayPool.takeChar(kDataBuffer.vectorLength());
                kDataBuffer.readCharVector(charArray, 0);
                colsVector.add(charArray);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }



    public <T> T getColVector(String sym) {
        int i = colsNames.indexOf(sym);
        if(i < 0) return null;
        return (T) colsVector.get(i);
    }

    public KdbType getColType(String name) {
        return types.get(colsNames.indexOf(name));
    }

    public void reset() {
        for (int i = 0; i < colsVector.size(); i++) {
            arrayPool.putBack(colsVector.get(i));
        }
        types.clear();
        size = 0;
        colsNames.clear();
    }
}
