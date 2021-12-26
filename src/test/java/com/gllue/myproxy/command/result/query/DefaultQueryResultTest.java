package com.gllue.myproxy.command.result.query;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData.Column;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultQueryResultTest {

  QueryResultMetaData prepareMetaData(int columnCount) {
    var columns = new Column[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = new Column("col_" + i, MySQLColumnType.MYSQL_TYPE_VAR_STRING);
    }
    return new DefaultQueryResultMetaData(columns);
  }

  private String[] buildRandomRow(int columnCount) {
    var row = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      row[i] = RandomUtils.randomUUID();
    }
    return row;
  }

  void assertRowsEquals(String[][] rows, QueryResult queryResult) {
    int i = 0;
    var columnCount = queryResult.getMetaData().getColumnCount();
    while (queryResult.next()) {
      for (int col = 0; col < columnCount; col++) {
        assertEquals(rows[i][col], queryResult.getStringValue(col));
      }
      i++;
    }
    assertEquals(rows.length, i);
  }

  @Test
  public void testNext() {
    var columnCount = 5;
    var rowCount = 10;
    var rows = new String[rowCount][];
    var queryResult = new DefaultQueryResult(prepareMetaData(columnCount), rows);
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildRandomRow(columnCount);
    }
    assertRowsEquals(rows, queryResult);
  }
}
