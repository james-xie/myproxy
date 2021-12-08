package com.gllue.command.handler.query.dml.select;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.gllue.command.handler.query.BaseQueryHandlerTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableScopeFactoryTest extends BaseQueryHandlerTest {

  @Test
  public void testNewTableScope() {
    var table1 = prepareTable("table1", "col1");
    var table2 = prepareTable("table2", "col1");
    var datasource = DATASOURCE;
    var database = DATABASE;
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var query =
        "select * from table t "
            + "inner join table1 t1 on t.id = t1.id "
            + "inner join table2 t2 on t.id = t2.id "
            + "where t.id = 1";
    var selectStmt = parseSelectQuery(query);
    var tableSource = selectStmt.getSelect().getFirstQueryBlock().getFrom();
    var scope = factory.newTableScope(null, false, tableSource);
    assertNull(scope.getTable(database, "t"));
    assertNull(scope.getTable(database, "table1"));
    assertNull(scope.getTable(database, "table2"));
    assertEquals(table1, scope.getTable(database, "t1"));
    assertEquals(table2, scope.getTable(database, "t2"));
  }

  @Test
  public void testNewTableScope1() {
    var table1 = prepareTable("table1", "col1");
    var table2 = prepareTable("table2", "col1");
    var datasource = DATASOURCE;
    var database = DATABASE;
    var databasesMetaData = prepareMultiDatabasesMetaData(datasource, database, table1, table2);
    var factory = new TableScopeFactory(datasource, database, databasesMetaData);
    var query =
        "select * from newdb.table t "
            + "inner join newdb.table1 t1 on t.id = t1.id "
            + "inner join newdb.table2 t2 on t.id = t2.id "
            + "where t.id = 1";
    var selectStmt = parseSelectQuery(query);
    var tableSource = selectStmt.getSelect().getFirstQueryBlock().getFrom();
    var scope = factory.newTableScope(null, false, tableSource);
    assertNull(scope.getTable(database, "t1"));
    assertNull(scope.getTable(database, "t2"));
    assertFalse(scope.anyTablesInScope());
  }
}
