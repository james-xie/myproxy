package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.query.MemoryBufferedQueryResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferedQueryResultReader extends DefaultQueryResultReader {
  private final int maxCapacity;
  private final int bufferLowWaterMark;
  private final int bufferHighWaterMark;

  private MemoryBufferedQueryResult queryResult;

  public BufferedQueryResultReader() {
    this(
        MemoryBufferedQueryResult.DEFAULT_MAX_CAPACITY_IN_BYTES,
        MemoryBufferedQueryResult.DEFAULT_LOW_WATER_MARK_IN_BYTES,
        MemoryBufferedQueryResult.DEFAULT_HIGH_WATER_MARK_IN_BYTES);
  }

  public BufferedQueryResultReader(
      final int maxCapacity, final int bufferLowWaterMark, final int bufferHighWaterMark) {
    this.maxCapacity = maxCapacity;
    this.bufferLowWaterMark = bufferLowWaterMark;
    this.bufferHighWaterMark = bufferHighWaterMark;
  }

  @Override
  protected void beforeReadRows() {
    queryResult =
        new MemoryBufferedQueryResult(
            queryResultMetaData,
            maxCapacity,
            bufferLowWaterMark,
            bufferHighWaterMark,
            MemoryBufferedQueryResult.THRESHOLD_OF_DISCARD_BUFFERED_QUERY_RESULT);
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

  @Override
  protected void afterReadRows() {
    queryResult.setDone();
    super.afterReadRows();
  }
}
