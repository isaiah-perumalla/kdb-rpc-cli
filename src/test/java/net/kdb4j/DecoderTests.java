package net.kdb4j;

import kx.c;
import net.kdb4j.codecs.KdbType;
import net.kdb4j.codecs.ListDecoder;
import net.kdb4j.utils.TimeUtils;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.*;

public class DecoderTests {

    private final ListDecoder decoder = new ListDecoder();
    private kx.c kx = new kx.c();
    @Test
    public void testDecodeTableData() throws UnsupportedEncodingException, c.KException {
        String hexData = "0100000054020000000003000000F575706400F571756F7465006200630B000800000074696D650073796D006269640061736B006273697A65006173697A65006D6F6465006578000000080000000C000A000000C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B09C0E01A9A06828B090B000A000000414D5A4E004E53524746004E5352474600474F4F4700424142410054534C4100574D540054534D004241424100414D5A4E0009000A000000A1D00A8AA457E83F9042451E248FBD3FF6CBBE3D1F70EB3F59325096F42107401C83BC528EDC11404E9291D4147016403C23ED458DBE1B408E33999162E51640B4FAA34E9FFF1240F6969961251A0F4009000A000000DAFAD7799F9DD53F7819643EEBD3D53F834670AF0551F73FB550E1CDC5210240D84636916831D13F947A337EC126164034CC4D21986D0440CA04040859760140BAE0E48800781E4093C360BA1113154007000A0000006400000000000000C8000000000000002C010000000000009001000000000000F4010000000000005802000000000000BC0200000000000020030000000000008403000000000000E80300000000000007000A000000C800000000000000900100000000000058020000000000002003000000000000E803000000000000B004000000000000780500000000000040060000000000000807000000000000D0070000000000000A000A000000585858585858585858580A000A0000005A5A5A5A5A5A5A5A5A5A";
        MutableDirectBuffer directBuffer = new ExpandableArrayBuffer();
        byte[] bytes = ByteUtils.hexStringToByteArray(hexData);
        Object d = kx.deserialize(bytes);
        System.out.println(ByteUtils.bytesToHex(bytes, 0, bytes.length));
        Object[] expecteObjects = (Object[]) d;
        c.Flip expectedTable = (c.Flip) expecteObjects[2];
        directBuffer.putBytes(0, bytes);
        decoder.wrap(directBuffer, 8, ByteOrder.LITTLE_ENDIAN);
        assertEquals(KdbType.List, decoder.getDataType());
        assertEquals(3, decoder.size());
        assertEquals(KdbType.SymAtom, decoder.itemAt(0).type());
        assertEquals(KdbType.SymAtom, decoder.itemAt(1).type());
        assertEquals(KdbType.Table, decoder.itemAt(2).type());
        assertEquals("upd", decoder.itemAt(0).readSym());
        assertEquals("quote", decoder.itemAt(1).readSym());
        KDataBuffer kdata = decoder.itemAt(2);
        MutableTable mutableTable = new MutableTable();
        kdata.decodeTable(mutableTable);

        String[] cols = {"time", "sym", "bid", "ask", "bsize", "asize", "mode", "ex"};
        KdbType[] types = {KdbType.Timestamp, KdbType.Sym, KdbType.Double, KdbType.Double, KdbType.Long, KdbType.Long, KdbType.Char, KdbType.Char};
        HashMap<String, Object> expectedData = new HashMap<>();

        assertArrayEquals(mutableTable.mutableCols().toArray(), cols);
        for (int i = 0; i < cols.length; i++) {
            String name = cols[i];
            KdbType type = mutableTable.getColType(name);
            assertNotNull(name, type);
            assertEquals(types[i], type);
        }
        assertArrayEquals( new long[] {687786332971000000L, 687786332971000000L, 687786332971000000L ,687786332971000000L, 687786332971000000L, 687786332971000000L, 687786332971000000L, 687786332971000000L, 687786332971000000L, 687786332971000000L},
                TimeUtils.TimestampVector.toArray(mutableTable.getColVector("time")));

            assertArrayEquals((Object[]) expectedTable.y[1],  mutableTable.<Collection>getColVector("sym").toArray());
            assertArrayEquals((double[]) expectedTable.y[2], mutableTable.getColVector("bid"), 0.00001);
            assertArrayEquals((double[]) expectedTable.y[3],  mutableTable.getColVector("ask"), 0.00001);
            assertArrayEquals((long[]) expectedTable.y[4],  mutableTable.getColVector("bsize"));
            assertArrayEquals((long[]) expectedTable.y[5],  mutableTable.getColVector("asize"));
            assertArrayEquals((char[]) expectedTable.y[6],  mutableTable.getColVector("mode"));
            assertArrayEquals((char[]) expectedTable.y[7],  mutableTable.getColVector("ex"));



    }
}
