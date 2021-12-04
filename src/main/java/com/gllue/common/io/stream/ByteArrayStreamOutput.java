package com.gllue.common.io.stream;

import com.google.common.base.Preconditions;

public class ByteArrayStreamOutput extends AbstractStreamOutput {
  private static final int INIT_ARRAY_SIZE = 64;
  private static final int MAX_CAPACITY = 1 << 30;

  private int capacity;
  private int writeIndex = 0;
  private byte[] byteArray;

  public ByteArrayStreamOutput(final int initArraySize) {
    Preconditions.checkArgument(initArraySize > 0, "Initial array size must be greater than 0");
    capacity = initArraySize;
    byteArray = new byte[initArraySize];
  }

  public ByteArrayStreamOutput() {
    this(INIT_ARRAY_SIZE);
  }

  public int getWrittenBytes() {
    return writeIndex;
  }

  public byte[] getByteArray() {
    return byteArray;
  }

  public byte[] getTrimmedByteArray() {
    var trimmed = new byte[getWrittenBytes()];
    System.arraycopy(byteArray, 0, trimmed, 0, trimmed.length);
    return trimmed;
  }

  private int nextCapacity(final int minCapacity) {
    do {
      capacity <<= 1;
    } while (capacity <= minCapacity);
    return capacity;
  }

  private void ensureByteArrayCapacity(final int bytes) {
    int minCapacity = writeIndex + bytes - 1;
    if (minCapacity >= capacity) {
      capacity = nextCapacity(minCapacity);
      if (capacity >= MAX_CAPACITY) {
        throw new IndexOutOfBoundsException("Exceeds max capacity limit.");
      }
      var old = byteArray;
      byteArray = new byte[capacity];
      System.arraycopy(old, 0, byteArray, 0, old.length);
    }
  }

  @Override
  public void writeByte(byte b) {
    ensureByteArrayCapacity(1);
    byteArray[writeIndex++] = b;
  }

  @Override
  public void writeBytes(byte[] buf, int offset, int length) {
    if (length == 0) {
      return;
    }

    Preconditions.checkArgument(
        offset >= 0 && offset < buf.length && offset + length <= buf.length, "Illegal arguments.");

    ensureByteArrayCapacity(length);
    System.arraycopy(buf, offset, byteArray, writeIndex, length);
    writeIndex += length;
  }

  @Override
  public void close() throws Exception {}
}
