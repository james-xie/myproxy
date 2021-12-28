package com.gllue.myproxy.command.result.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData.Column;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.constant.MySQLColumnType;

public abstract class BaseQueryResultTest {
  QueryResultMetaData prepareMetaData(int columnCount) {
    var columns = new Column[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = new Column("col_" + i, MySQLColumnType.MYSQL_TYPE_VAR_STRING);
    }
    return new DefaultQueryResultMetaData(columns);
  }

  byte[][] buildEmptyRow(int columnCount, int rowBytes) {
    byte[][] row = new byte[columnCount][];
    for (int i = 0; i < columnCount; i++) {
      row[i] = new byte[rowBytes];
    }
    return row;
  }

  byte[][] buildRandomRow(int columnCount) {
    byte[][] row = new byte[columnCount][];
    for (int i = 0; i < columnCount; i++) {
      row[i] = RandomUtils.generateRandomBytes(10);
    }
    return row;
  }

  void assertRowsEquals(byte[][][] rows, QueryResult queryResult) {
    int i = 0;
    var columnCount = queryResult.getMetaData().getColumnCount();
    while (queryResult.next()) {
      for (int col = 0; col < columnCount; col++) {
        assertArrayEquals(rows[i][col], queryResult.getValue(col));
      }
      i++;
    }
    assertEquals(rows.length, i);
  }


}
