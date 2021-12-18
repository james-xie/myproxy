package com.gllue.myproxy.transport.protocol.packet.query.binary.value;


import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.Set;

/**
 * Binary protocol value for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/binary-protocol-value.html">Binary Protocol Value</a>
 */
public interface BinaryProtocolValue<T, U> {

  /**
   * Read binary protocol value.
   *
   * @param payload payload operation for MySQL packet
   * @return binary value result
   */
  T read(final MySQLPayload payload);

  /**
   * Write binary protocol value.
   *
   * @param payload payload operation for MySQL packet
   * @param value value to be written
   */
  void write(final MySQLPayload payload, final T value);

  /**
   * This protocol value can support column types.
   *
   * @return column types
   */
  Set<MySQLColumnType> supportColumnTypes();

  /**
   * Get self instance.
   *
   * @return self instance
   */
  U getInstance();
}

