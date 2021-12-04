package com.gllue.transport.backend.command;

import com.gllue.command.result.CommandResult;
import com.gllue.command.result.query.QueryRowsConsumerPipeline;
import com.gllue.transport.protocol.packet.query.text.TextResultSetRowPacket;
import com.google.common.base.Preconditions;

public class PipelineSupportedQueryResultReader extends DefaultQueryResultReader {

  private final QueryRowsConsumerPipeline pipeline;

  public PipelineSupportedQueryResultReader(final QueryRowsConsumerPipeline pipeline) {
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null.");
    this.pipeline = pipeline;
  }

  @Override
  protected void beforeReadRows() {
    super.beforeReadRows();
    pipeline.fireBeforeReadRows(queryResultMetaData);
  }

  @Override
  protected void onRowRead(TextResultSetRowPacket packet) {
    pipeline.fireReadRowData(packet.getRowData());
  }

  @Override
  protected void afterReadRows() {
    super.afterReadRows();
    pipeline.fireAfterReadRows();
  }

  @Override
  public void onSuccess(CommandResult result) {
    try {
      super.onSuccess(result);
    } finally {
      pipeline.fireReadSuccess(result);
    }
  }

  @Override
  public void onFailure(Throwable e) {
    try {
      super.onFailure(e);
    } finally {
      pipeline.fireReadFailed(e);
    }
  }
}
