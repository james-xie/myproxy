package com.gllue.myproxy.metadata.codec;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseMetaDataCodecTest {
  MetaDataCodec<DatabaseMetaData> getCodec() {
    return DatabaseMetaDataCodec.getInstance();
  }

  DatabaseMetaData prepareDatabase(String datasource, String name, TableMetaData... tables) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(datasource);
    builder.setName(name);
    for (var table : tables) {
      builder.addTable(table);
    }
    builder.setVersion(10);
    return builder.build();
  }

  static final ColumnType[] COLUMN_TYPES =
      new ColumnType[] {
        ColumnType.ENCRYPT, ColumnType.VARCHAR, ColumnType.BIGINT, ColumnType.TINYINT
      };

  TableMetaData prepareTableMetaData(String tableName, TableType tableType, String... columnNames) {
    var builder = new TableMetaData.Builder();
    var random = new Random();
    builder
        .setName(tableName)
        .setType(tableType)
        .setIdentity(tableName + random.nextInt())
        .setVersion(2);
    for (var columnName : columnNames) {
      var typeIndex = random.nextInt(COLUMN_TYPES.length);
      builder.addColumn(
          new ColumnMetaData.Builder()
              .setName(columnName)
              .setType(COLUMN_TYPES[typeIndex])
              .setBuiltin(false)
              .setDefaultValue(columnName)
              .build());
    }
    return builder.build();
  }

  PartitionTableMetaData preparePartitionTableMetaData(
      String tableName, String[] columnNames, String[] extColumnNames) {
    var builder = new PartitionTableMetaData.Builder();
    builder.setIdentity(tableName);
    builder.setVersion(2);
    builder.setName(tableName);
    builder.setPrimaryTable(prepareTableMetaData(tableName, TableType.PRIMARY, columnNames));
    builder.addExtensionTable(
        prepareTableMetaData(tableName + "-1", TableType.EXTENSION, extColumnNames));
    return builder.build();
  }

  @Test
  public void testEncodeAndDecodeForNoTableDatabase() {
    var metadata = prepareDatabase("datasource1", "testDb");
    var codec = getCodec();
    var output = new ByteArrayStreamOutput();
    codec.encode(output, metadata);
    var input = ByteArrayStreamInput.wrap(output.getByteArray());
    var newMetaData = codec.decode(input);
    assertEquals(metadata.getDatasource(), newMetaData.getDatasource());
    assertEquals(metadata.getName(), newMetaData.getName());
    assertEquals(metadata.getVersion(), newMetaData.getVersion());
    assertEquals(metadata.getIdentity(), newMetaData.getIdentity());
  }

  void assertColumnEquals(ColumnMetaData expect, ColumnMetaData actual) {
    assertEquals(expect.getName(), actual.getName());
    assertEquals(expect.getVersion(), actual.getVersion());
    assertEquals(expect.getType(), actual.getType());
    assertEquals(expect.getTable().getName(), actual.getTable().getName());
    assertEquals(expect.getDefaultValue(), actual.getDefaultValue());
    assertEquals(expect.isNullable(), actual.isNullable());
    assertEquals(expect.isBuiltin(), actual.isBuiltin());
  }

  void assertTableEquals(TableMetaData expect, TableMetaData actual) {
    assertEquals(expect.getName(), actual.getName());
    assertEquals(expect.getVersion(), actual.getVersion());
    assertEquals(expect.getIdentity(), actual.getIdentity());
    assertEquals(expect.getType(), actual.getType());
    assertEquals(expect.getColumnNames(), actual.getColumnNames());
    for (var name : expect.getColumnNames()) {
      assertColumnEquals(expect.getColumn(name), actual.getColumn(name));
    }
  }

  @Test
  public void testEncodeAndDecodeForMultipleTablesDatabase() {
    var table1 = prepareTableMetaData("table1", TableType.STANDARD, "col1", "col2", "col3");
    var table2 = prepareTableMetaData("table2", TableType.STANDARD, "col1");
    var table3 =
        preparePartitionTableMetaData(
            "table3", new String[] {"col1", "col2"}, new String[] {"col3", "col4"});
    var table4 =
        preparePartitionTableMetaData("table4", new String[] {"col1"}, new String[] {"col2"});
    var metadata = prepareDatabase("datasource2", "testDb1", table1, table2, table3, table4);
    var codec = getCodec();
    var output = new ByteArrayStreamOutput();
    codec.encode(output, metadata);
    var input = ByteArrayStreamInput.wrap(output.getByteArray());
    var newMetaData = codec.decode(input);
    assertEquals(metadata.getDatasource(), newMetaData.getDatasource());
    assertEquals(metadata.getName(), newMetaData.getName());
    assertEquals(metadata.getVersion(), newMetaData.getVersion());
    assertEquals(metadata.getIdentity(), newMetaData.getIdentity());
    assertEquals(metadata.getNumberOfTables(), newMetaData.getNumberOfTables());
    assertTableEquals(metadata.getTable("table1"), newMetaData.getTable("table1"));
    assertTableEquals(metadata.getTable("table2"), newMetaData.getTable("table2"));
    assertTableEquals(metadata.getTable("table3"), newMetaData.getTable("table3"));
  }
}
