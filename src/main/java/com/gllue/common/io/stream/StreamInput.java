package com.gllue.common.io.stream;


public interface StreamInput extends AutoCloseable {
  int NULL = 0;

  /**
   * Reads and returns a single byte.
   */
  byte readByte();

  /**
   * Reads a specified number of bytes into an array.
   *
   * @param buf    the array to read bytes into
   */
  void readBytes(byte[] buf);

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param buf    the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len    the number of bytes to read
   */
  void readBytes(byte[] buf, int offset, int len);

  /**
   * Reads single byte and returns a boolean.
   */
  boolean readBoolean();

  /**
   * Reads 4 bytes and returns an integer.
   */
  int readInt();

  /**
   * Reads 8 bytes and returns a long.
   */
  long readLong();

  /**
   * Reads 4 bytes and returns a float.
   */
  float readFloat();

  /**
   * Reads 8 bytes and returns a double.
   */
  double readDouble();

  /**
   * Reads a string with fixed length.
   */
  String readStringFix(int len);

  /**
   * Reads a string ending in 0.
   */
  String readStringNul();

  /**
   * Reads a string or null.
   */
  String readNullableString();

  /**
   * Reads an array filled with string elements.
   */
  String[] readStringArray();

  /**
   * Skip some bytes.
   */
  void skipBytes(final int len);
}
