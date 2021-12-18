package com.gllue.myproxy.command.result.query;

import io.netty.buffer.ByteBuf;

public interface QueryResult {
  /**
   * Iterate next row.
   *
   * @return has next row
   */
  boolean next();

  /**
   * Get raw value.
   *
   * @param columnIndex column index
   * @return raw value
   */
  byte[] getValue(int columnIndex);

  /**
   * Get value as string.
   *
   * @param columnIndex column index
   * @return string value
   */
  String getStringValue(int columnIndex);


  /**
   * Discards the read rows.
   */
  void discardReadRows();

  /**
   * Similar to {@link ByteBuf#discardReadBytes()} except that this method might discard
   * some, all, or none of read rows depending on its internal implementation to reduce
   * overall memory bandwidth consumption at the cost of potentially additional memory
   * consumption.
   */
  void discardSomeReadRows();

  /**
   * Get the column definitions which is the meta data of the
   * columns in the query result.
   *
   * @return the meta data describes columns
   */
  QueryResultMetaData getMetaData();

  /**
   * Close the query result, release the relevant resources.
   */
  void close();
}
