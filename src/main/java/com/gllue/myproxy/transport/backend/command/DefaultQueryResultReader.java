package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.command.result.query.QueryResultMetaDataImpl;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;
import java.util.ArrayList;
import java.util.List;

public abstract class DefaultQueryResultReader extends AbstractQueryResultReader {

  private List<ColumnDefinition41Packet> columnDefList;
  protected QueryResultMetaData queryResultMetaData;

  @Override
  protected void beforeReadColumnDefinitions() {
    columnDefList = new ArrayList<>();
  }

  @Override
  protected void onColumnRead(ColumnDefinition41Packet packet) {
    columnDefList.add(packet);
  }

  @Override
  protected void afterReadColumnDefinitions() {
    queryResultMetaData = new QueryResultMetaDataImpl(columnDefList);
    columnDefList = null;
  }

  @Override
  public QueryResultMetaData getQueryResultMetaData() {
    return queryResultMetaData;
  }
}
