package com.gllue.myproxy.metadata.loader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.common.util.PathUtils;
import com.gllue.myproxy.metadata.MetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.metadata.model.ColumnMetaData.Builder;
import com.gllue.myproxy.repository.PersistRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiDatabasesMetaDataLoaderTest {
  static final String DATASOURCE = "ds";
  static final String BASE_PATH = "/root";

  @Mock PersistRepository repository;

  Map<String, List<String>> childrenKeysMap = new HashMap<>();
  Map<String, DatabaseMetaData> dbMap = new HashMap<>();
  Map<String, TableMetaData> tableMap = new HashMap<>();

  final String db1Path = concatPath(BASE_PATH, "db1");
  final String db2Path = concatPath(BASE_PATH, "db2");
  final String table1Path = concatPath(BASE_PATH, "db1", "table1");
  final String table2Path = concatPath(BASE_PATH, "db1", "table2");
  final String table3Path = concatPath(BASE_PATH, "db2", "table3");

  {
    childrenKeysMap.put(concatPath(BASE_PATH), List.of("db1", "db2"));
    childrenKeysMap.put(db1Path, List.of("table1", "table2"));
    childrenKeysMap.put(db2Path, List.of("table3"));
    dbMap.put(db1Path, prepareDatabase("db1"));
    dbMap.put(db2Path, prepareDatabase("db2"));
    tableMap.put(table1Path, prepareTable("table1"));
    tableMap.put(table2Path, prepareTable("table2"));
    tableMap.put(table3Path, prepareTable("table3"));
    dbMap.get(db1Path).addTable(tableMap.get(table1Path));
    dbMap.get(db1Path).addTable(tableMap.get(table2Path));
    dbMap.get(db2Path).addTable(tableMap.get(table3Path));
  }

  String concatPath(String... path) {
    return PathUtils.joinPaths(path);
  }

  TableMetaData prepareTable(String name) {
    var builder = new TableMetaData.Builder();
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(
        new Builder().setName("col").setType(ColumnType.VARCHAR).build());
    return builder.build();
  }

  DatabaseMetaData prepareDatabase(String name) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName(name);
    return builder.build();
  }

  byte[] metaDataToBytes(MetaData metaData) {
    var output = new ByteArrayStreamOutput();
    metaData.writeTo(output);
    return output.getTrimmedByteArray();
  }

  void mockRepository() {
    when(repository.exists(BASE_PATH)).thenReturn(true);
    for (var entry : childrenKeysMap.entrySet()) {
      when(repository.getChildrenKeys(entry.getKey())).thenReturn(entry.getValue());
    }
    for (var entry : dbMap.entrySet()) {
      when(repository.get(entry.getKey())).thenReturn(metaDataToBytes(entry.getValue()));
    }
    for (var entry : tableMap.entrySet()) {
      when(repository.get(entry.getKey())).thenReturn(metaDataToBytes(entry.getValue()));
    }
  }

  void assertDatabaseEquals(DatabaseMetaData db1, DatabaseMetaData db2) {
    assertEquals(db1.getName(), db2.getName());
    Set<String> tableNames1 = new HashSet<>();
    Set<String> tableNames2 = new HashSet<>();
    for (var name : db1.getTableNames()) {
      tableNames1.add(name);
    }
    for (var name : db2.getTableNames()) {
      tableNames2.add(name);
    }
    assertEquals(tableNames1, tableNames2);
  }

  void assertTableEquals(TableMetaData t1, TableMetaData t2) {
    assertEquals(t1.getName(), t2.getName());
    assertEquals(t1.getIdentity(), t2.getIdentity());
    assertEquals(t1.getType(), t2.getType());
    assertEquals(t1.getNumberOfColumns(), t2.getNumberOfColumns());
  }

  @Test
  public void testLoad() throws InterruptedException {
    var loader = new MultiDatabasesMetaDataLoader(repository);
    mockRepository();

    var multiDatabases = loader.load(BASE_PATH);
    var db1 = multiDatabases.getDatabase(DATASOURCE, "db1");
    var db2 = multiDatabases.getDatabase(DATASOURCE, "db2");
    assertDatabaseEquals(dbMap.get(db1Path), db1);
    assertDatabaseEquals(dbMap.get(db2Path), db2);
    assertTableEquals(tableMap.get(table1Path), db1.getTable("table1"));
    assertTableEquals(tableMap.get(table2Path), db1.getTable("table2"));
    assertTableEquals(tableMap.get(table3Path), db2.getTable("table3"));
  }
}
