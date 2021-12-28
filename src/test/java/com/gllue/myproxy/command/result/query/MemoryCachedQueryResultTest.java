package com.gllue.myproxy.command.result.query;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MemoryCachedQueryResultTest extends BaseQueryResultTest {
  @Test
  public void testNext() {
    var columnCount = 3;
    var rowCount = 100;
    var queryResult = new MemoryCachedQueryResult(prepareMetaData(columnCount));
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildRandomRow(columnCount);
      queryResult.addRow(rows[i]);
    }
    assertRowsEquals(rows, queryResult);
  }

  @Test
  public void testMaxCapacity() {
    var columnCount = 1;
    var rowCount = 10;
    var queryResult = new MemoryCachedQueryResult(prepareMetaData(columnCount), 10 * 1024);
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildEmptyRow(columnCount, 1024);
      queryResult.addRow(rows[i]);
    }
    assertRowsEquals(rows, queryResult);
  }

  @Test(expected = QueryResultOutOfMemoryException.class)
  public void testOutOfMemory() {
    var columnCount = 101;
    var rowCount = 1;
    var queryResult = new MemoryCachedQueryResult(prepareMetaData(columnCount), 100);
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildEmptyRow(columnCount, 1);
      queryResult.addRow(rows[i]);
    }
  }
}
