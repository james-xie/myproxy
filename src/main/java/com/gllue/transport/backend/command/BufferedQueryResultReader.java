package com.gllue.transport.backend.command;

import com.gllue.command.result.query.BufferedQueryResult;
import com.gllue.command.result.query.QueryResult;
import com.gllue.transport.protocol.packet.query.text.TextResultSetRowPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferedQueryResultReader extends DefaultQueryResultReader {
  private final int initCapacity;
  private final int maxCapacity;
  private final int bufferLowWaterMark;
  private final int bufferHighWaterMark;

  private BufferedQueryResult queryResult;

  public BufferedQueryResultReader() {
    this(BufferedQueryResult.INITIAL_BUFFER_CAPACITY);
  }

  public BufferedQueryResultReader(final int initCapacity) {
    this(
        initCapacity,
        BufferedQueryResult.MAX_BUFFER_CAPACITY,
        BufferedQueryResult.DEFAULT_BUFFER_LOW_WATER_MARK,
        BufferedQueryResult.DEFAULT_BUFFER_HIGH_WATER_MARK);
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
        new BufferedQueryResult(
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
