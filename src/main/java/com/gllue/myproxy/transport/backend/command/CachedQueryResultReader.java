package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.MemoryBufferedQueryResult;
import com.gllue.myproxy.command.result.query.MemoryCachedQueryResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CachedQueryResultReader extends DefaultQueryResultReader {
  private final int maxCapacityInBytes;

  private MemoryCachedQueryResult queryResult;

  public CachedQueryResultReader() {
    this(MemoryCachedQueryResult.DEFAULT_MAX_CAPACITY_IN_BYTES);
  }

  public CachedQueryResultReader(final int maxCapacityInBytes) {
    this.maxCapacityInBytes = maxCapacityInBytes;
  }

  @Override
  protected void beforeReadRows() {
    queryResult = new MemoryCachedQueryResult(queryResultMetaData, maxCapacityInBytes);
  }

  @Override
  protected QueryResult getQueryResult() {
    return queryResult;
  }

  @Override
  protected void onRowRead(TextResultSetRowPacket packet) {
    queryResult.addRow(packet.getRowData());
  }

  public static CachedQueryResultReader newInstance(Callback<CommandResult> callback) {
    var reader = new CachedQueryResultReader();
    reader.addCallback(callback);
    return reader;
  }
}
