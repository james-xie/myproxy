package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.query.BufferedQueryResultImpl;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferedQueryResultReader extends DefaultQueryResultReader {
  private final int initCapacity;
  private final int maxCapacity;
  private final int bufferLowWaterMark;
  private final int bufferHighWaterMark;

  private BufferedQueryResultImpl queryResult;

  public BufferedQueryResultReader() {
    this(BufferedQueryResultImpl.INITIAL_BUFFER_CAPACITY);
  }

  public BufferedQueryResultReader(final int initCapacity) {
    this(
        initCapacity,
        BufferedQueryResultImpl.MAX_BUFFER_CAPACITY,
        BufferedQueryResultImpl.DEFAULT_BUFFER_LOW_WATER_MARK,
        BufferedQueryResultImpl.DEFAULT_BUFFER_HIGH_WATER_MARK);
  }

  public BufferedQueryResultReader(
      final int initCapacity,
      final int maxCapacity,
      final int bufferLowWaterMark,
      final int bufferHighWaterMark) {
    this.initCapacity = initCapacity;
    this.maxCapacity = maxCapacity;
    this.bufferLowWaterMark = bufferLowWaterMark;
    this.bufferHighWaterMark = bufferHighWaterMark;
  }

  @Override
  protected void beforeReadRows() {
    queryResult =
        new BufferedQueryResultImpl(
            queryResultMetaData,
            initCapacity,
            maxCapacity,
            bufferLowWaterMark,
            bufferHighWaterMark);
  }

  @Override
  protected QueryResult getQueryResult() {
    return queryResult;
  }

  @Override
  protected void onRowRead(TextResultSetRowPacket packet) {
    var overflow = queryResult.addRow(packet.getRowData());
    if (overflow) {
      if (log.isDebugEnabled()) {
        log.debug("Buffered query result is overflow.");
      }
      queryResult.setWritabilityChangedListener(() -> getConnection().enableAutoRead());
      getConnection().disableAutoRead();
    }
  }
}
