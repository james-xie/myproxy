package com.gllue.myproxy.common.io.stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.common.util.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ByteArrayStreamOutputTest {
  ByteArrayStreamOutput newStreamOutput() {
    return new ByteArrayStreamOutput();
  }

  @Test
  public void testWriteByte() {
    var bytes = RandomUtils.generateRandomBytes(100);
    var output = newStreamOutput();
    for (var b : bytes) {
      output.writeByte(b);
    }
    assertEquals(bytes.length, output.getWrittenBytes());
    assertArrayEquals(bytes, output.getTrimmedByteArray());
  }

  @Test
  public void testWriteBytes() {
    var bytes = RandomUtils.generateRandomBytes(100);
    var output = newStreamOutput();
    output.writeBytes(bytes);
    assertEquals(bytes.length, output.getWrittenBytes());
    assertArrayEquals(bytes, output.getTrimmedByteArray());

    output = newStreamOutput();
    output.writeBytes(bytes, 50, 50);
    assertEquals(50, output.getWrittenBytes());
    var subArray = new byte[50];
    System.arraycopy(bytes, 50, subArray, 0, 50);
    assertArrayEquals(subArray, output.getTrimmedByteArray());
  }

  @Test
  public void testWriteBoolean() {
    var output = newStreamOutput();
    output.writeBoolean(true);
    output.writeBoolean(false);
    assertEquals(2, output.getWrittenBytes());
    assertEquals(1, output.getByteArray()[0]);
    assertEquals(0, output.getByteArray()[1]);
  }

  @Test
  public void testWriteInt() {
    var output = newStreamOutput();
    output.writeInt(0x0000eeff);
    output.writeInt(0);
    output.writeInt(0xff00eeff);
    assertEquals(3 * 4, output.getWrittenBytes());
    assertArrayEquals(
        new byte[] {
          0, 0, (byte) 0xee, (byte) 0xff, 0, 0, 0, 0, (byte) 0xff, 0, (byte) 0xee, (byte) 0xff,
        },
        output.getTrimmedByteArray());
  }

  @Test
  public void testWriteLong() {
    var output = newStreamOutput();
    output.writeLong(0x0000eeff0000eeffL);
    output.writeLong(0L);
    output.writeLong(0xff00eeffff00eeffL);
    assertEquals(3 * 8, output.getWrittenBytes());
    assertArrayEquals(
        new byte[] {
          0,
          0,
          (byte) 0xee,
          (byte) 0xff,
          0,
          0,
          (byte) 0xee,
          (byte) 0xff,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          (byte) 0xff,
          0,
          (byte) 0xee,
          (byte) 0xff,
          (byte) 0xff,
          0,
          (byte) 0xee,
          (byte) 0xff,
        },
        output.getTrimmedByteArray());
  }

  @Test
  public void testWriteFloat() {
    var output = newStreamOutput();
    output.writeFloat(Float.intBitsToFloat(0x0000eeff));
    output.writeFloat(Float.intBitsToFloat(0));
    output.writeFloat(Float.intBitsToFloat(0xff00eeff));
    assertEquals(3 * 4, output.getWrittenBytes());
    assertArrayEquals(
        new byte[] {
          0, 0, (byte) 0xee, (byte) 0xff, 0, 0, 0, 0, (byte) 0xff, 0, (byte) 0xee, (byte) 0xff,
        },
        output.getTrimmedByteArray());
  }

  @Test
  public void testWriteDouble() {
    var output = newStreamOutput();
    output.writeDouble(Double.longBitsToDouble(0x0000eeff0000eeffL));
    output.writeDouble(Double.longBitsToDouble(0L));
    output.writeDouble(Double.longBitsToDouble(0xff00eeffff00eeffL));
    assertEquals(3 * 8, output.getWrittenBytes());
    assertArrayEquals(
        new byte[] {
          0,
          0,
          (byte) 0xee,
          (byte) 0xff,
          0,
          0,
          (byte) 0xee,
          (byte) 0xff,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          (byte) 0xff,
          0,
          (byte) 0xee,
          (byte) 0xff,
          (byte) 0xff,
          0,
          (byte) 0xee,
          (byte) 0xff,
        },
        output.getTrimmedByteArray());
  }

  byte[] trimmedByteArray(byte[] bytes, int length) {
    var res = new byte[length];
    System.arraycopy(bytes, 0, res, 0, length);
    return res;
  }

  @Test
  public void testWriteStringFix() {
    var output = newStreamOutput();
    var stringArray = new String[] {"a1", "a2", "a3", "a4", "a5"};
    int offset = 0;
    var stringBytes = new byte[1000];
    for (var str : stringArray) {
      output.writeStringFix(str);

      var bytes = str.getBytes();
      System.arraycopy(bytes, 0, stringBytes, offset, bytes.length);
      offset += bytes.length;
    }
    assertArrayEquals(trimmedByteArray(stringBytes, offset), output.getTrimmedByteArray());
  }

  @Test
  public void testWriteStringNul() {
    var output = newStreamOutput();
    var stringArray = new String[] {"a1", "a2", "a3", "a4", "a5"};
    int offset = 0;
    var stringBytes = new byte[1000];
    for (var str : stringArray) {
      output.writeStringNul(str);

      var bytes = str.getBytes();
      System.arraycopy(bytes, 0, stringBytes, offset, bytes.length);
      offset += bytes.length + 1;
    }
    assertArrayEquals(trimmedByteArray(stringBytes, offset), output.getTrimmedByteArray());
  }

  @Test
  public void testWriteNullableString() {
    var output = newStreamOutput();
    var stringArray = new String[] {"a1", null, "a2", null, "a3", "a4", null, "a5"};
    int offset = 0;
    var stringBytes = new byte[1000];
    for (var str : stringArray) {
      output.writeNullableString(str);

      if (str == null) {
        stringBytes[offset++] = 1;
        continue;
      }

      var bytes = str.getBytes();
      System.arraycopy(bytes, 0, stringBytes, ++offset, bytes.length);
      offset += bytes.length + 1;
    }
    assertArrayEquals(trimmedByteArray(stringBytes, offset), output.getTrimmedByteArray());
  }

  @Test
  public void testWriteStringArray() {
    var output = newStreamOutput();
    var stringArray = new String[] {"a1", "a2", "a3", "a4", "a5"};
    output.writeStringArray(stringArray);

    int offset = 0;
    var stringBytes = new byte[1000];
    stringBytes[offset++] = 0;
    stringBytes[offset++] = 0;
    stringBytes[offset++] = 0;
    stringBytes[offset++] = 5;
    for (var str : stringArray) {
      var bytes = str.getBytes();
      System.arraycopy(bytes, 0, stringBytes, offset, bytes.length);
      offset += bytes.length + 1;
    }
    assertArrayEquals(trimmedByteArray(stringBytes, offset), output.getTrimmedByteArray());
  }
}
