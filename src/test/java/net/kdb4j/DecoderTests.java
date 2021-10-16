package net.kdb4j;

import kx.c;
import net.kdb4j.ByteUtils;
import net.kdb4j.codecs.KdbDecoder;
import net.kdb4j.codecs.KdbListDecoder;
import net.kdb4j.codecs.KdbType;
import net.kdb4j.codecs.MessageReceiver;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class DecoderTests {

    private final KdbDecoder decoder = new KdbDecoder();
    private kx.c kx = new kx.c();
    @Test
    public void testDecodeTableData() throws UnsupportedEncodingException, c.KException {
        String hexData = "0100000029020000000003000000F575706400F571756F7465006200630B000600000074696D650073796D006269640061736B006273697A65006173697A650000000600000010000A00000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D000080012A8D8B1D00000B000A0000004E464C580054534C41004E464C58004A504D0054534C41004E464C5800574D5400424142410054534C41004A504D0009000A00000098DC35E9F9B9B33FA8BE326C43D4D63FAAE506751A2A04402311EEB131480C40B0758B991A8ED63F8624E4BE6DDB0B407D3FD075EDA7FB3F656366EA8EB6134007FF3BCF94BA0540B44A3F4AAC5D104009000A0000001696B519DC33E03FD4632210DF0ED23F1A639CC08DC4E23F70D89F1C75FF0C40E2BF32B5FB0DFF3FE3A449BD58D617402A7E0F948733F13FDA7DC0BC68AA1340E0E2351DDCB9C73FAF7F4FC3033A224007000A0000006400000000000000C8000000000000002C010000000000009001000000000000F4010000000000005802000000000000BC0200000000000020030000000000008403000000000000E80300000000000007000A000000C800000000000000900100000000000058020000000000002003000000000000E803000000000000B004000000000000780500000000000040060000000000000807000000000000D007000000000000";
        MutableDirectBuffer directBuffer = new ExpandableArrayBuffer();
        byte[] bytes = ByteUtils.hexStringToByteArray(hexData);
        Object d = kx.deserialize(bytes);
        System.out.println(ByteUtils.bytesToHex(bytes, 0, bytes.length));
        directBuffer.putBytes(0, bytes);
        MessageReceiver messageReceiver = new MessageReceiver() {
            @Override
            public void receiveList(KdbListDecoder listDecoder) {
                listDecoder.forEach((i, item) -> {
                    if (i == 0) {
                        assertEquals(item.type(), KdbType.Sym);
                    }
                });
            }
        };
        int result = decoder.decode(directBuffer, 0 , bytes.length, messageReceiver);

    }
}
