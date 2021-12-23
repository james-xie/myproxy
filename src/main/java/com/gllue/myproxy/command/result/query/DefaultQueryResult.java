package com.gllue.myproxy.command.result.query;

import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultQueryResult implements QueryResult {
  private final QueryResultMetaData queryResultMetaData;
  private final String[][] rows;
  private int rowIndex = 0;

  @Override
  public boolean next() {
    if (rowIndex >= rows.length) {
      return false;
    }
    return ++rowIndex < rows.length;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return rows[rowIndex][columnIndex].getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String getStringValue(int columnIndex) {
    return rows[rowIndex][columnIndex];
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return queryResultMetaData;
  }

  @Override
  public void close() {

  }
}
