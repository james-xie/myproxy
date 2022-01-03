package com.gllue.myproxy.command.result.query;

import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.RequiredArgsConstructor;

public class SimpleQueryResult implements QueryResult {
  private final QueryResultMetaData queryResultMetaData;
  private final String[][] rows;
  private final int maxIndex;
  private int rowIndex = -1;

  public SimpleQueryResult(final QueryResultMetaData queryResultMetaData, final String[][] rows) {
    this.queryResultMetaData = queryResultMetaData;
    this.rows = rows;
    this.maxIndex = rows.length - 1;
  }

  public SimpleQueryResult(
      final QueryResultMetaData queryResultMetaData, final List<String[]> rows) {
    this(queryResultMetaData, rows.toArray(new String[0][]));
  }

  @Override
  public boolean next() {
    if (rowIndex >= maxIndex) {
      return false;
    }

    rowIndex++;
    return true;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    var value = rows[rowIndex][columnIndex];
    if (value == null) {
      return null;
    }
    return value.getBytes(StandardCharsets.UTF_8);
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
  public void close() {}
}
