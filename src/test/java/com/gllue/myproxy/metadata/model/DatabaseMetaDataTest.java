package com.gllue.myproxy.metadata.model;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseMetaDataTest {
  static final String DATASOURCE = "ds";

  TableMetaData prepareTable(String name) {
    var builder = new TableMetaData.Builder();
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col").setType(ColumnType.VARCHAR).build());
    return builder.build();
  }

  DatabaseMetaData prepareDatabase() {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName("db1");
    builder.addTable(prepareTable("table1"));
    builder.addTable(prepareTable("table2"));
    builder.addTable(prepareTable("table3"));
    builder.addTable(prepareTable("table4"));
    return builder.build();
  }

  Set<String> getTableNames(DatabaseMetaData database) {
    var names = new HashSet<String>();
    for (var name : database.getTableNames()) {
      names.add(name);
    }
    return names;
  }

  @Test
  public void testBuild() {
    var database = prepareDatabase();
    assertEquals("db1", database.getName());
    assertEquals(Set.of("table1", "table2", "table3", "table4"), getTableNames(database));
  }

  @Test
  public void testCopyFrom() {
    var database = prepareDatabase();
    var builder = new DatabaseMetaData.Builder();
    builder.copyFrom(database, CopyOptions.COPY_CHILDREN);
    var newDatabase = builder.build();

    assertEquals(database.getName(), newDatabase.getName());
    assertEquals(database.getIdentity(), newDatabase.getIdentity());
    assertEquals(database.getVersion(), newDatabase.getVersion());
    assertEquals(getTableNames(database), getTableNames(newDatabase));
  }
}
