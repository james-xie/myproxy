package com.gllue.myproxy.command.result.query;

import com.google.common.base.Preconditions;

public abstract class QueryResultDecorator implements QueryResult {
  private final QueryResult queryResult;

  protected QueryResultDecorator(QueryResult queryResult) {
    Preconditions.checkNotNull(queryResult);
    this.queryResult = queryResult;
  }

  @Override
  public boolean next() {
    return queryResult.next();
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return queryResult.getValue(columnIndex);
  }

  @Override
  public String getStringValue(int columnIndex) {
    return queryResult.getStringValue(columnIndex);
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return queryResult.getMetaData();
  }

  @Override
  public void close() {
    queryResult.close();
  }
}
