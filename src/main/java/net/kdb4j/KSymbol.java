package net.kdb4j;

import net.kdb4j.codecs.KdbType;
import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KSymbol  {
    private static final List<String> symbols = new ArrayList<>();
    private static final MutableSym mutableSym = new MutableSym();


    public static String read(DirectBuffer directBuffer, int offset) {
        KDataBuffer.checkKdbType(offset, directBuffer, KdbType.SymAtom);
        int i = offset + 1;
        byte ch;
        mutableSym.clear();
        while((ch = directBuffer.getByte(i++)) != 0) {
            mutableSym.append((char)ch);
        }
        return getSym(mutableSym);
    }

    public static String getSym(CharSequence mutableSym) {
        String sym = findSymbol(mutableSym);
        if(sym == null) {
            sym = mutableSym.toString();
            symbols.add(sym);
        }
        return sym;
    }

    private static String findSymbol(CharSequence charSequence) {
        for (int i = 0; i < symbols.size(); i++) {
            if(charSequenceEquals(charSequence, symbols.get(i))) return symbols.get(i);
        }
        return null;
    }

    public static boolean charSequenceEquals(CharSequence charSequence, String s) {
        if(charSequence.length() != s.length()) return false;
        for (int i = 0; i < charSequence.length(); i++) {
            if(charSequence.charAt(i) != s.charAt(i)) return false;
        }
        return true;
    }

    public static class MutableSym implements CharSequence {
        private byte[] bytes = new byte[32];
        private int size;

        @Override
        public int length() {
            return size;
        }

        @Override
        public char charAt(int index) {
            return (char) bytes[index];
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return new String(Arrays.copyOf(bytes,size));
        }

        public void append(char ch) {
            if(size == bytes.length) {
                bytes = Arrays.copyOf(bytes, (size/32 + 1) * 32);
            }
            bytes[size++] = (byte) ch;
        }

        public void clear() {
            size = 0;
        }
    }
}
