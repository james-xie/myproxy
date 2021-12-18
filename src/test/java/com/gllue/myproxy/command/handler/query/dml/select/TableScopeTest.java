package com.gllue.myproxy.command.handler.query.dml.select;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.metadata.model.TableMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableScopeTest {
  @Test
  public void testTableAddAndGet() {
    var scope = new TableScope(null, false);
    var table1 = Mockito.mock(TableMetaData.class);
    scope.addTable("db1", "ta", table1);
    var table2 = Mockito.mock(TableMetaData.class);
    scope.addTable("db2", "ta", table2);
    var table3 = Mockito.mock(TableMetaData.class);
    scope.addTable("db1", "ta1", table3);
    assertEquals(table1, scope.getTable("db1", "ta"));
    assertEquals(table2, scope.getTable("db2", "ta"));
    assertEquals(table3, scope.getTable("db1", "ta1"));
    assertNull(scope.getTable("db", "ta1"));
  }

  @Test
  public void testExtensionTableAliasAddAndGet() {
    var scope = new TableScope(null, false);
    var aliases1 = new String[] {"1"};
    scope.addExtensionTableAlias("db1", "ta", aliases1);
    var aliases2 = new String[] {"2"};
    scope.addExtensionTableAlias("db2", "ta", aliases2);
    var aliases3 = new String[] {"3"};
    scope.addExtensionTableAlias("db1", "ta1", aliases3);
    assertArrayEquals(aliases1, scope.getExtensionTableAliases("db1", "ta"));
    assertArrayEquals(aliases2, scope.getExtensionTableAliases("db2", "ta"));
    assertArrayEquals(aliases3, scope.getExtensionTableAliases("db1", "ta1"));
    assertNull(scope.getTable("db2", "ta1"));
  }


  @Test
  public void testInheritableGet() {
    var table1 = Mockito.mock(TableMetaData.class);
    var table2 = Mockito.mock(TableMetaData.class);
    var table3 = Mockito.mock(TableMetaData.class);

    var scope1 = new TableScope(null, false);
    scope1.addTable("db1", "ta", table1);
    scope1.addTable("db1", "ta1", table2);

    var scope2 = new TableScope(scope1, true);
    scope2.addTable("db2", "ta", table2);

    var scope3 = new TableScope(scope2, true);
    scope3.addTable("db3", "ta1", table3);
    scope3.addTable("db2", "ta", table3);

    assertEquals(table1, scope3.getTable("db1", "ta"));
    assertEquals(table3, scope3.getTable("db2", "ta"));
    assertEquals(table3, scope3.getTable("db3", "ta1"));
  }

  @Test
  public void testAnyTablesInScope() {
    var table1 = Mockito.mock(TableMetaData.class);

    var scope1 = new TableScope(null, false);
    scope1.addTable("db1", "ta", table1);
    var scope2 = new TableScope(scope1, true);
    var scope3 = new TableScope(scope2, true);

    assertEquals(table1, scope3.getTable("db1", "ta"));
    assertTrue(scope3.anyTablesInScope());
  }

}
