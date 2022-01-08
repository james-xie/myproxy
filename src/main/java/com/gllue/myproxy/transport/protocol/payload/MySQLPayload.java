package com.gllue.myproxy.transport.protocol.payload;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.Charset;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class MySQLPayload implements AutoCloseable {
  public static final int POWER_2_16 = 1 << 16;

  public static final int POWER_2_24 = 1 << 24;

  private Charset charset = Charset.defaultCharset();

  private final ByteBuf byteBuf;

  private boolean closed;

  public void setCharset(Charset charset) {
    Preconditions.checkNotNull(charset, "Argument charset cannot be null");
    this.charset = charset;
  }

  /**
   * Read 1 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 1 byte fixed length integer
   */
  public int readInt1() {
    return byteBuf.readUnsignedByte();
  }

  /**
   * Write 1 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 1 byte fixed length integer
   */
  public void writeInt1(final int value) {
    byteBuf.writeByte(value);
  }

  /**
   * Read 2 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 2 byte fixed length integer
   */
  public int readInt2() {
    return byteBuf.readUnsignedShortLE();
  }

  /**
   * Write 2 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 2 byte fixed length integer
   */
  public void writeInt2(final int value) {
    byteBuf.writeShortLE(value);
  }

  /**
   * Read 3 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 3 byte fixed length integer
   */
  public int readInt3() {
    return byteBuf.readUnsignedMediumLE();
  }

  /**
   * Write 3 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 3 byte fixed length integer
   */
  public void writeInt3(final int value) {
    byteBuf.writeMediumLE(value);
  }

  /**
   * Read 4 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 4 byte fixed length integer
   */
  public int readInt4() {
    return byteBuf.readIntLE();
  }

  /**
   * Write 4 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 4 byte fixed length integer
   */
  public void writeInt4(final int value) {
    byteBuf.writeIntLE(value);
  }

  /**
   * Read 6 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 6 byte fixed length integer
   */
  public long readInt6() {
    long result = 0;
    for (int i = 0; i < 6; i++) {
      result |= ((long) (0xff & byteBuf.readByte())) << (8 * i);
    }
    return result;
  }

  /**
   * Write 6 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 6 byte fixed length integer
   */
  public void writeInt6(long value) {
    for (int i = 0; i < 6; i++) {
      byteBuf.writeByte((byte) value & 0xff);
      value >>>= 8;
    }
  }

  /**
   * Read 8 byte fixed length integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @return 8 byte fixed length integer
   */
  public long readInt8() {
    return byteBuf.readLongLE();
  }

  /**
   * Write 8 byte fixed length integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::FixedLengthInteger">FixedLengthInteger</a>
   * @param value 8 byte fixed length integer
   */
  public void writeInt8(final long value) {
    byteBuf.writeLongLE(value);
  }

  /**
   * Read floating point in IEEE 754 double precision format from byte buffers.
   *
   * @return double precision floating number
   */
  public double readDouble() {
    return byteBuf.readDoubleLE();
  }

  /**
   * Write floating point in IEEE 754 double precision format to byte buffers.
   *
   * @param value double precision floating number
   */
  public void writeDouble(final double value) {
    byteBuf.writeDoubleLE(value);
  }

  /**
   * Read floating point in IEEE 754 single precision format from byte buffers.
   *
   * @return single precision floating number
   */
  public float readFloat() {
    return byteBuf.readFloatLE();
  }

  /**
   * Write floating point in IEEE 754 single precision format to byte buffers.
   *
   * @param value single precision floating number
   */
  public void writeFloat(final float value) {
    byteBuf.writeFloatLE(value);
  }

  /**
   * Read lenenc integer from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
   * @return lenenc integer
   */
  public long readIntLenenc() {
    int firstByte = readInt1();
    if (firstByte < 0xfb) {
      return firstByte;
    }
    if (0xfb == firstByte) {
      return 0;
    }
    if (0xfc == firstByte) {
      return readInt2();
    }
    if (0xfd == firstByte) {
      return readInt3();
    }
    return byteBuf.readLongLE();
  }

  /**
   * Return the occupied bytes of lenenc integer.
   *
   * @return number of occupied bytes
   */
  public static int getLenencIntOccupiedBytes(final long value) {
    if (value < 0xfb) {
      return 1;
    }
    if (value < POWER_2_16) {
      return 3;
    }
    if (value < POWER_2_24) {
      return 4;
    }
    return 9;
  }

  /**
   * Write lenenc integer to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
   * @param value lenenc integer
   */
  public void writeIntLenenc(final long value) {
    if (value < 0xfb) {
      byteBuf.writeByte((int) value);
      return;
    }
    if (value < POWER_2_16) {
      byteBuf.writeByte(0xfc);
      byteBuf.writeShortLE((int) value);
      return;
    }
    if (value < POWER_2_24) {
      byteBuf.writeByte(0xfd);
      byteBuf.writeMediumLE((int) value);
      return;
    }
    byteBuf.writeByte(0xfe);
    byteBuf.writeLongLE(value);
  }

  /**
   * Read fixed length long from byte buffers.
   *
   * @param length length read from byte buffers
   * @return fixed length long
   */
  public long readLong(final int length) {
    long result = 0;
    for (int i = 0; i < length; i++) {
      result = result << 8 | readInt1();
    }
    return result;
  }

  /**
   * Read lenenc string from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @return lenenc string
   */
  public String readStringLenenc() {
    int length = (int) readIntLenenc();
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return new String(result, charset);
  }

  /**
   * Read lenenc string from byte buffers for bytes.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @return lenenc bytes
   */
  public byte[] readStringLenencReturnBytes() {
    int length = (int) readIntLenenc();
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return result;
  }

  /**
   * Write lenenc string to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @param value fixed length string
   */
  public void writeStringLenenc(final String value) {
    if (Strings.isNullOrEmpty(value)) {
      byteBuf.writeByte(0);
      return;
    }

    var bytes = value.getBytes();
    writeIntLenenc(bytes.length);
    byteBuf.writeBytes(bytes);
  }

  /**
   * Write lenenc bytes to byte buffers.
   *
   * @param value fixed length bytes
   */
  public void writeBytesLenenc(final byte[] value) {
    if (0 == value.length) {
      byteBuf.writeByte(0);
      return;
    }
    writeIntLenenc(value.length);
    byteBuf.writeBytes(value);
  }

  /**
   * Read fixed length string from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @param length length of fixed string
   * @return fixed length string
   */
  public String readStringFix(final int length) {
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return new String(result, charset);
  }

  /**
   * Read fixed length string from byte buffers and return bytes.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @param length length of fixed string
   * @return fixed length bytes
   */
  public byte[] readStringFixReturnBytes(final int length) {
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return result;
  }

  /**
   * Write fixed length string to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
   * @param value fixed length string
   */
  public void writeStringFix(final String value) {
    byteBuf.writeBytes(value.getBytes());
  }

  /**
   * Write byte array to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41">Native41</a>
   * @param value byte array
   */
  public void writeBytes(final byte[] value) {
    byteBuf.writeBytes(value);
  }

  /**
   * Write partial byte array to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41">Native41</a>
   * @param value byte array
   * @param start start index of the value.
   * @param length length should be written to byte buffer.
   */
  public void writeBytes(final byte[] value, final int start, final int length) {
    byteBuf.writeBytes(value, start, length);
  }

  /**
   * Read null terminated string from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
   * @return null terminated string
   */
  public String readStringNul() {
    byte[] result = new byte[byteBuf.bytesBefore((byte) 0)];
    byteBuf.readBytes(result);
    byteBuf.skipBytes(1);
    return new String(result, charset);
  }

  /**
   * Read null terminated string from byte buffers and return bytes.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
   * @return null terminated bytes
   */
  public byte[] readStringNulReturnBytes() {
    byte[] result = new byte[byteBuf.bytesBefore((byte) 0)];
    byteBuf.readBytes(result);
    byteBuf.skipBytes(1);
    return result;
  }

  /**
   * Write null terminated string to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString">NulTerminatedString</a>
   * @param value null terminated string
   */
  public void writeStringNul(final String value) {
    byteBuf.writeBytes(value.getBytes());
    byteBuf.writeByte(0);
  }

  /**
   * Read rest of packet string from byte buffers and return bytes.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::RestOfPacketString">RestOfPacketString</a>
   * @return rest of packet string bytes
   */
  public byte[] readStringEOFReturnBytes() {
    byte[] result = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(result);
    return result;
  }

  /**
   * Read rest of packet string from byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::RestOfPacketString">RestOfPacketString</a>
   * @return rest of packet string
   */
  public String readStringEOF() {
    byte[] result = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(result);
    return new String(result, charset);
  }

  /**
   * Write rest of packet string to byte buffers.
   *
   * @see <a
   *     href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::RestOfPacketString">RestOfPacketString</a>
   * @param value rest of packet string
   */
  public void writeStringEOF(final String value) {
    byteBuf.writeBytes(value.getBytes());
  }

  /**
   * Skip reserved from byte buffers.
   *
   * @param length length of reserved
   */
  public void skipBytes(final int length) {
    byteBuf.skipBytes(length);
  }

  /**
   * Write zero to byte buffers.
   *
   * @param length length of reserved
   */
  public void writeZero(final int length) {
    for (int i = 0; i < length; i++) {
      byteBuf.writeByte(0);
    }
  }

  /**
   * Returns the number of readable bytes
   *
   * @return number of readable bytes
   */
  public int readableBytes() {
    return byteBuf.readableBytes();
  }

  /**
   * Returns the number of writable bytes
   *
   * @return number of writable bytes
   */
  public int writableBytes() {
    return byteBuf.writableBytes();
  }

  /**
   * Peek a byte at the reader index.
   *
   * @return byte at the reader index.
   */
  public int peek() {
    return byteBuf.getUnsignedByte(byteBuf.readerIndex());
  }

  public void retain() {
    byteBuf.retain();
  }

  public void release() {
    byteBuf.release();
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Release the payload holden resources.
   */
  @Override
  public void close() {
    if (closed) {
      throw new IllegalStateException("MySQLPayload has already closed.");
    }

    closed = true;
    byteBuf.release();
  }

  @Override
  public String toString() {
    return String.format("MySQLPayload [%s]", ByteBufUtil.hexDump(byteBuf));
  }
}
