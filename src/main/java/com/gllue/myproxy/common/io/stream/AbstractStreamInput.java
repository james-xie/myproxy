package com.gllue.myproxy.common.io.stream;

import java.nio.charset.Charset;

public abstract class AbstractStreamInput implements StreamInput {

  private final Charset charset;

  protected AbstractStreamInput(final Charset charset) {
    this.charset = charset;
  }

  protected AbstractStreamInput() {
    this(Charset.defaultCharset());
  }

  @Override
  public void readBytes(byte[] buf) {
    readBytes(buf, 0, buf.length);
  }

  @Override
  public boolean readBoolean() {
    var value = readByte();
    if (value == 0) {
      return false;
    } else if (value == 1) {
      return true;
    } else {
      throw new IllegalStateException(String.format("unexpected byte [0x%02x]", value));
    }
  }

  @Override
  public int readInt() {
    return ((readByte() & 0xFF) << 24)
        | ((readByte() & 0xFF) << 16)
        | ((readByte() & 0xFF) << 8)
        | (readByte() & 0xFF);
  }

  @Override
  public long readLong() {
    return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
  }

  @Override
  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readStringFix(int len) {
    var buf = new byte[len];
    readBytes(buf);
    return new String(buf, charset);
  }

  abstract int bytesBefore(byte value);

  @Override
  public String readStringNul() {
    int length = bytesBefore((byte) NULL);
    if (length < 0) {
      throw new IllegalStateException("Cannot read terminator of the string.");
    }

    byte[] buf = new byte[length];
    readBytes(buf);
    skipBytes(1);
    return new String(buf, charset);
  }

  @Override
  public String readNullableString() {
    var isNull = readBoolean();
    if (isNull) {
      return null;
    }
    return readStringNul();
  }

  @Override
  public String[] readStringArray() {
    final int arraySize = readInt();
    var array = new String[arraySize];
    for (int i=0; i<arraySize; i++) {
      array[i] = readStringNul();
    }
    return array;
  }
}
