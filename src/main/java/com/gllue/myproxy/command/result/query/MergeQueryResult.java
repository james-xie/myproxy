package com.gllue.myproxy.command.result.query;

import com.google.common.base.Preconditions;
import java.util.List;

public class MergeQueryResult implements QueryResult {
  private final List<QueryResult> queryResults;
  private final QueryResultMetaData metaData;
  private QueryResult currentResult;
  private int nextIndex;

  public MergeQueryResult(List<QueryResult> queryResults) {
    Preconditions.checkArgument(queryResults != null && queryResults.size() > 0);
    this.queryResults = queryResults;
    this.currentResult = queryResults.get(0);
    this.metaData = currentResult.getMetaData();
    this.nextIndex = 1;
  }

  private void validateQueryResultMetaData(QueryResult[] queryResults) {
    if (queryResults.length == 1) {
      return;
    }

    var first = queryResults[0];
    for (int i = 1; i < queryResults.length; i++) {
      var current = queryResults[i];
      if (!compareQueryResultMetaData(current.getMetaData(), first.getMetaData())) {
        throw new MergeQueryResultException("Query result meta data is not consistency.");
      }
    }
  }

  private boolean compareQueryResultMetaData(QueryResultMetaData m1, QueryResultMetaData m2) {
    var columnCount = m1.getColumnCount();
    if (columnCount != m2.getColumnCount()) {
      return false;
    }
    for (int i = 0; i < columnCount; i++) {
      var res =
          (m1.getColumnType(i) == m2.getColumnType(i))
              && stringEquals(m1.getColumnLabel(i), m2.getColumnLabel(i));
      if (!res) {
        return false;
      }
    }
    return true;
  }

  private boolean stringEquals(String s1, String s2) {
    if (s1 == null && s2 == null) {
      return true;
    }
    return s1 != null && s1.equals(s2);
  }

  @Override
  public boolean next() {
    do {
      if (currentResult == null) {
        currentResult = queryResults.get(nextIndex++);
      }

      var hasNext = currentResult.next();
      if (hasNext) {
        return true;
      }
      currentResult = null;
    } while (nextIndex < queryResults.size());
    return false;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return currentResult.getValue(columnIndex);
  }

  @Override
  public String getStringValue(int columnIndex) {
    return currentResult.getStringValue(columnIndex);
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() {
    for (var item : queryResults) {
      item.close();
    }
  }
}
