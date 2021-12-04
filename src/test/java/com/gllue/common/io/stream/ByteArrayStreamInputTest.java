package com.gllue.common.io.stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.gllue.common.util.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ByteArrayStreamInputTest {
  ByteArrayStreamInput newStreamInput(byte[] bytes) {
    return new ByteArrayStreamInput(bytes);
  }

  ByteArrayStreamInput newStreamInput(byte[] bytes, int offset, int length) {
    return new ByteArrayStreamInput(bytes, offset, length);
  }

  @Test
  public void testReadByte() {
    var bytes = RandomUtils.generateRandomBytes(100);
    var input = newStreamInput(bytes);
    for (byte b : bytes) {
      assertEquals(b, input.readByte());
    }

    input = newStreamInput(bytes, 50, 50);
    for (int i = 50; i < 100; i++) {
      assertEquals(bytes[i], input.readByte());
    }
  }

  @Test
  public void testReadBytes() {
    var bytes = RandomUtils.generateRandomBytes(100);
    var input = newStreamInput(bytes);
    var readBytes = new byte[bytes.length];
    input.readBytes(readBytes);
    assertArrayEquals(bytes, readBytes);

    input = newStreamInput(bytes);
    input.readBytes(readBytes, 50, 50);
    for (int i = 0; i < 50; i++) {
      assertEquals(bytes[i], readBytes[i + 50]);
    }

    input = newStreamInput(bytes, 50, 50);
    input.readBytes(readBytes, 0, 50);
    for (int i = 0; i < 50; i++) {
      assertEquals(bytes[i + 50], readBytes[i]);
    }
  }

  @Test
  public void testBoolean() {
    var input = newStreamInput(new byte[] {0, 1, 2});
    assertFalse(input.readBoolean());
    assertTrue(input.readBoolean());
    try {
      input.readBoolean();
      fail();
    } catch (Exception ignore) {
    }
  }

  @Test
  public void testInt() {
    var input = newStreamInput(new byte[] {
        0, (byte)0xff, (byte)0xff, (byte)0xff,
        0, 0, 0, 0,
        (byte)0xff, 0, 0, 0,
        0, (byte)0xef, (byte)0x01, (byte)0x02
    });
    assertEquals(0x00ffffff, input.readInt());
    assertEquals(0, input.readInt());
    assertEquals(0xff000000, input.readInt());
    assertEquals(0x00ef0102, input.readInt());
  }

  @Test
  public void testLong() {
    var input = newStreamInput(new byte[] {
        0, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
        0, 0, 0, 0, 0, 0, 0, 0,
        (byte)0xff, 0, 0, 0, 0, 0, 0, 0,
        0, (byte)0xef, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04, (byte)0x05, (byte)0x06
    });
    assertEquals(0x00ffffffffffffffL, input.readLong());
    assertEquals(0L, input.readLong());
    assertEquals(0xff00000000000000L, input.readLong());
    assertEquals(0x00ef010203040506L, input.readLong());
  }

  @Test
  public void testFloat() {
    var input = newStreamInput(new byte[] {
        0, (byte)0xff, (byte)0xff, (byte)0xff,
        0, 0, 0, 0,
        (byte)0xff, 0, 0, 0,
        0, (byte)0xef, (byte)0x01, (byte)0x02
    });
    assertEquals(0x00ffffff, Float.floatToIntBits(input.readFloat()));
    assertEquals(0, Float.floatToIntBits(input.readFloat()));
    assertEquals(0xff000000, Float.floatToIntBits(input.readFloat()));
    assertEquals(0x00ef0102, Float.floatToIntBits(input.readFloat()));
  }

  @Test
  public void testDouble() {
    var input = newStreamInput(new byte[] {
        0, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
        0, 0, 0, 0, 0, 0, 0, 0,
        (byte)0xff, 0, 0, 0, 0, 0, 0, 0,
        0, (byte)0xef, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04, (byte)0x05, (byte)0x06
    });
    assertEquals(0x00ffffffffffffffL, Double.doubleToLongBits(input.readDouble()));
    assertEquals(0L, Double.doubleToLongBits(input.readDouble()));
    assertEquals(0xff00000000000000L, Double.doubleToLongBits(input.readDouble()));
    assertEquals(0x00ef010203040506L, Double.doubleToLongBits(input.readDouble()));
  }

  @Test
  public void testReadStringFix() {
    var testValue = "test string";
    var bytes = testValue.getBytes();
    var input = newStreamInput(bytes);
    assertEquals(testValue, input.readStringFix(bytes.length));
    assertEquals("", input.readStringFix(0));
  }

  @Test
  public void testReadStringNul() {
    var testValue = "test string";
    var bytes = testValue.getBytes();
    var copyBytes = new byte[bytes.length+2];
    System.arraycopy(bytes, 0, copyBytes, 0, bytes.length);

    var input = newStreamInput(copyBytes);
    assertEquals(testValue, input.readStringNul());
    assertEquals("", input.readStringNul());
  }

  @Test
  public void testReadNullableString() {
    var testValue = "test string";
    var bytes = testValue.getBytes();
    var copyBytes = new byte[bytes.length+3];
    System.arraycopy(bytes, 0, copyBytes, 1, bytes.length);
    copyBytes[copyBytes.length-1] = 1;

    var input = newStreamInput(copyBytes);
    assertEquals(testValue, input.readNullableString());
    assertNull(input.readNullableString());
  }

  @Test
  public void testReadStringArray() {
    int offset = 0;
    var bytes = new byte[1000];
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 5;
    var stringArray = new String[] {"a1", "b2", "c3", "d4", "e5"};
    for (var str: stringArray) {
      var strBytes = str.getBytes();
      System.arraycopy(strBytes, 0, bytes, offset, strBytes.length);
      offset += strBytes.length + 1;
    }

    var input = newStreamInput(bytes, 0, offset);
    var readStringArray = input.readStringArray();
    assertArrayEquals(stringArray, readStringArray);
  }

  @Test
  public void testSkipBytes() {
    var input = newStreamInput(new byte[] {
        0, 0, 0, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04
    });

    input.skipBytes(3);
    assertEquals(0x01020304, input.readInt());
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void readOutOfBounds() {
    newStreamInput(new byte[1]).readInt();
  }
}
