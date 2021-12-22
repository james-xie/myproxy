package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.command.result.CommandResult;

public interface QueryRowsConsumerPipeline {
  void addConsumer(QueryRowsConsumer consumer);

  void removeConsumer(QueryRowsConsumer consumer);

  void fireBeforeReadRows(QueryResultMetaData metaData);

  void fireReadRowData(byte[][] rowData);

  void fireAfterReadRows();

  void fireReadSuccess(CommandResult result);

  void fireReadFailed(Throwable e);
}
