package com.gllue.transport.protocol.payload;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.gllue.common.util.RandomUtils;
import com.gllue.transport.BaseTransportTest;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MySQLPayloadTest extends BaseTransportTest {
  @Test
  public void testIntReadWrite() {
    var payload = preparePayload();
    payload.writeInt1(1);
    payload.writeInt2(10);
    payload.writeInt3(100);
    payload.writeInt4(1000);
    payload.writeInt6(10000);
    payload.writeInt8(100000);
    payload.writeIntLenenc(1000000);
    assertEquals(1, payload.readInt1());
    assertEquals(10, payload.readInt2());
    assertEquals(100, payload.readInt3());
    assertEquals(1000, payload.readInt4());
    assertEquals(10000, payload.readInt6());
    assertEquals(100000, payload.readInt8());
    assertEquals(1000000, payload.readIntLenenc());
  }

  @Test
  public void testStringReadWrite() {
    final var payload = preparePayload();
    final var stringEof = "string eof";
    final var stringNul = "string nul";
    final var stringFixed = "string fixed";
    final var stringLenenc = "string lenenc";
    payload.writeStringNul(stringNul);
    payload.writeStringFix(stringFixed);
    payload.writeStringLenenc(stringLenenc);
    payload.writeStringEOF(stringEof);

    assertEquals(stringNul, payload.readStringNul());
    assertEquals(stringFixed, payload.readStringFix(stringFixed.length()));
    assertEquals(stringLenenc, payload.readStringLenenc());
    assertEquals(stringEof, payload.readStringEOF());
  }

  @Test
  public void testDoubleAndFloatReadWrite() {
    final var payload = preparePayload();
    payload.writeFloat(99.99f);
    payload.writeDouble(999.999d);

    float floatVal = payload.readFloat();
    double doubleVal = payload.readDouble();
    assertTrue(floatVal > 99.9 && floatVal < 100);
    assertTrue(doubleVal > 999.99 && doubleVal < 1000);
  }

  @Test
  public void testByteReadWrite() {
    final var payload = preparePayload();
    final var bytes = RandomUtils.generateRandomBytes(100);
    payload.writeBytes(bytes);
    payload.writeBytes(bytes, 50, 50);
    payload.writeBytesLenenc(bytes);

    assertArrayEquals(bytes, payload.readStringFixReturnBytes(bytes.length));
    final var subArray = new byte[50];
    System.arraycopy(bytes, 50, subArray, 0, 50);
    assertArrayEquals(subArray, payload.readStringFixReturnBytes(50));
    assertArrayEquals(bytes, payload.readStringLenencReturnBytes());
  }

  @Test
  public void testStringWithCharset() {
    final var payload = preparePayload();
    payload.setCharset(CharsetUtil.UTF_8);

    final var stringEof = "中文字符串EOF";
    final var stringNul = "中文字符串NUL";
    final var stringFixed = "中文字符串FIXED";
    final var stringLenenc = "中文字符串LENENC";
    payload.writeStringNul(stringNul);
    payload.writeStringFix(stringFixed);
    payload.writeStringLenenc(stringLenenc);
    payload.writeStringEOF(stringEof);

    assertEquals(stringNul, payload.readStringNul());
    assertEquals(stringFixed, payload.readStringFix(stringFixed.getBytes().length));
    assertEquals(stringLenenc, payload.readStringLenenc());
    assertEquals(stringEof, payload.readStringEOF());
  }

  private MySQLPayload preparePayload() {
    return createEmptyPayload();
  }
}
