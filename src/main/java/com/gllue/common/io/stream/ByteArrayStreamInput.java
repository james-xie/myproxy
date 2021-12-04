package com.gllue.common.io.stream;

import com.google.common.base.Preconditions;

public class ByteArrayStreamInput extends AbstractStreamInput {
  private final byte[] array;
  private int index;
  private final int length;

  public ByteArrayStreamInput(final byte[] array) {
    this(array, 0, array.length);
  }

  public ByteArrayStreamInput(final byte[] array, final int offset, final int length) {
    Preconditions.checkArgument(offset >= 0 && offset < array.length, "Illegal argument: offset");
    Preconditions.checkArgument(
        offset + length <= array.length, "offset + length exceeds the range of the array.");

    this.array = array;
    this.index = offset;
    this.length = offset + length;
  }

  public static ByteArrayStreamInput wrap(final byte[] array) {
    return new ByteArrayStreamInput(array);
  }

  @Override
  int bytesBefore(byte value) {
    for (int i = index; i < length; i++) {
      if (array[i] == value) {
        return i - index;
      }
    }
    return -1;
  }

  @Override
  public byte readByte() {
    if (index >= length) {
      throw new IndexOutOfBoundsException();
    }
    return array[index++];
  }

  @Override
  public void readBytes(byte[] buf, int offset, int len) {
    if (len == 0) {
      return;
    }

    if (offset >= 0 && offset < buf.length && offset + len <= buf.length) {
      if (index + len - 1 >= length) {
        throw new IndexOutOfBoundsException();
      }

      System.arraycopy(array, index, buf, offset, len);
      index += len;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Illegal arguments. [bufSize:%s, offset:%s, len: %s]", buf.length, offset, len));
    }
  }

  @Override
  public void skipBytes(int len) {
    if (index + len - 1 >= length) {
      throw new IndexOutOfBoundsException();
    }
    index += len;
  }

  @Override
  public void close() throws Exception {}
}
