package com.gllue.command.result.query;

import com.gllue.command.result.CommandResult;
import java.util.ArrayList;
import java.util.List;

public class DefaultQueryRowsConsumerPipeline implements QueryRowsConsumerPipeline {
  private final List<QueryRowsConsumer> consumers = new ArrayList<>();
  private ReusableRow reusableRow;

  @Override
  public void addConsumer(QueryRowsConsumer consumer) {
    consumers.add(consumer);
  }

  @Override
  public void removeConsumer(QueryRowsConsumer consumer) {
    consumers.remove(consumer);
  }

  @Override
  public void fireBeforeReadRows(QueryResultMetaData metaData) {
    assert metaData != null;

    for (var consumer : consumers) {
      consumer.begin(metaData);
    }
    reusableRow = new ReusableRow(metaData);
  }

  @Override
  public void fireReadRowData(byte[][] rowData) {
    reusableRow.setRowData(rowData);
    for (var consumer : consumers) {
      consumer.accept(reusableRow);
    }
  }

  @Override
  public void fireAfterReadRows() {
    for (var consumer : consumers) {
      consumer.end();
    }
  }

  @Override
  public void fireReadSuccess(CommandResult result) {
    for (var consumer : consumers) {
      consumer.onSuccess(result);
    }
  }

  @Override
  public void fireReadFailed(Throwable e) {
    for (var consumer : consumers) {
      consumer.onFailure(e);
    }
  }
}
