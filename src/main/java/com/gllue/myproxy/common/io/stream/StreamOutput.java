package com.gllue.myproxy.common.io.stream;

public interface StreamOutput extends AutoCloseable {
  int NULL = 0;

  /**
   * Writes a single byte into the stream.
   */
  void writeByte(byte b);

  /**
   * Writes byte array into the stream.
   */
  void writeBytes(byte[] buf);

  /**
   * Writes partial byte array into the stream.
   *
   * @param buf    the array to read bytes into
   * @param offset the offset in the array
   * @param len    the number of bytes to write
   */
  void writeBytes(byte[] buf, int offset, int len);

  /**
   * Writes a boolean value into the stream.
   */
  void writeBoolean(boolean val);

  /**
   * Writes a integer value into the stream.
   */
  void writeInt(int val);

  /**
   * Writes a long value into the stream.
   */
  void writeLong(long val);

  /**
   * Writes a float value into the stream.
   */
  void writeFloat(float val);

  /**
   * Writes a double value into the stream.
   */
  void writeDouble(double val);

  /**
   * Writes a string with fixed length into the stream.
   *
   * @param val nonnull string value.
   */
  void writeStringFix(String val);

  /**
   * Writes a string ending in 0 into the stream.
   *
   * @param val nonnull string value.
   */
  void writeStringNul(String val);

  /**
   * Writes a string or null.
   */
  void writeNullableString(String val);

  /**
   * Writes an string array into the stream.
   */
  void writeStringArray(String[] val);
}
