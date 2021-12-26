package com.gllue.myproxy.command.result.query;

import io.netty.buffer.ByteBuf;

public interface BufferedQueryResult extends QueryResult {

  /**
   * Mark the completion of reading the query result.
   */
  void setDone();

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


}
