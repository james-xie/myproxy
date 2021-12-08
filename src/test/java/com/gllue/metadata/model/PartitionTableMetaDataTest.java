package com.gllue.metadata.model;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.common.io.stream.ByteArrayStreamInput;
import com.gllue.common.io.stream.ByteArrayStreamOutput;
import com.gllue.metadata.MetaDataBuilder.CopyOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionTableMetaDataTest {
  TableMetaData prepareTable(String name, String[] columns, TableType tableType) {
    var builder = new TableMetaData.Builder();
    builder.setName(name).setType(tableType).setIdentity(name).setVersion(1);
    for (var column : columns) {
      builder.addColumn(
          new ColumnMetaData.Builder().setName(column).setType(ColumnType.INT).build());
    }
    return builder.build();
  }

  TableMetaData preparePrimaryTable(String name) {
    return prepareTable(name, new String[] {"col1", "col2", "col3"}, TableType.PRIMARY);
  }

  String[] generateExtensionColumns(int ordinalValue) {
    var columns = new String[3];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = String.format("ext-%s-%s", ordinalValue, i);
    }
    return columns;
  }

  TableMetaData prepareExtensionTable(String name, int ordinalValue) {
    return prepareTable(name, generateExtensionColumns(ordinalValue), TableType.EXTENSION);
  }

  PartitionTableMetaData prepareTable() {
    var builder = new PartitionTableMetaData.Builder();
    var name = "table";
    builder.setName(name).setIdentity(name).setVersion(1);
    builder.setPrimaryTable(preparePrimaryTable(name));
    builder.addExtensionTable(prepareExtensionTable(name + 1, 1));
    builder.addExtensionTable(prepareExtensionTable(name + 2, 2));
    builder.addExtensionTable(prepareExtensionTable(name + 3, 3));
    return builder.build();
  }

  List<String> getColumnNames(TableMetaData table) {
    var colNames = new ArrayList<String>();
    for (int i = 0; i < table.getNumberOfColumns(); i++) {
      colNames.add(table.getColumn(i).getName());
    }
    return colNames;
  }

  @Test
  public void testBuild() {
    var builder = new PartitionTableMetaData.Builder();
    var name = "table";
    var extensionTableName = "table-1";
    builder.setName(name).setIdentity(name).setVersion(1);
    builder.setPrimaryTable(preparePrimaryTable(name));
    builder.addExtensionTable(prepareExtensionTable(extensionTableName, 1));
    var table = builder.build();
    assertEquals(name, table.getName());
    assertEquals(name, table.getIdentity());
    assertEquals(TableType.PARTITION, table.getType());
    assertEquals(1, table.getVersion());
    assertEquals(6, table.getNumberOfColumns());
    assertTrue(table.hasColumn("col1"));
    assertFalse(table.hasColumn("col10"));
    assertEquals(10 - 3, table.freeExtensionColumns(10));
    assertArrayEquals(new String[] {name, extensionTableName}, table.getTableNames());
    assertEquals(0, table.getOrdinalValueByColumnName("col1"));
    assertEquals(1, table.getOrdinalValueByColumnName("ext-1-1"));
    assertEquals(-1, table.getOrdinalValueByColumnName("unknown column"));
    assertFalse(table.hasExtensionColumn("col1"));
    assertTrue(table.hasExtensionColumn("ext-1-1"));

    var columns = new ArrayList<>(List.of("col1", "col2", "col3"));
    columns.addAll(Arrays.asList(generateExtensionColumns(1)));
    assertEquals(columns, getColumnNames(table));
  }

  @Test
  public void testReadAndWriteStream() {
    var table = prepareTable();
    var output = new ByteArrayStreamOutput();
    table.writeTo(output);
    var input = new ByteArrayStreamInput(output.getTrimmedByteArray());
    var builder = new PartitionTableMetaData.Builder();
    builder.readStream(input);
    var newTable = builder.build();

    assertEquals(table.getName(), newTable.getName());
    assertEquals(table.getIdentity(), newTable.getIdentity());
    assertEquals(table.getType(), newTable.getType());
    assertEquals(table.getVersion(), newTable.getVersion());
    assertEquals(table.getNumberOfColumns(), newTable.getNumberOfColumns());
    assertEquals(getColumnNames(table), getColumnNames(newTable));
    assertEquals(
        getColumnNames(table.getPrimaryTable()), getColumnNames(newTable.getPrimaryTable()));
    assertEquals(
        getColumnNames(table.getTableByOrdinalValue(1)),
        getColumnNames(newTable.getTableByOrdinalValue(1)));
    assertEquals(
        getColumnNames(table.getTableByOrdinalValue(2)),
        getColumnNames(newTable.getTableByOrdinalValue(2)));
    assertEquals(
        getColumnNames(table.getTableByOrdinalValue(3)),
        getColumnNames(newTable.getTableByOrdinalValue(3)));
  }

  @Test
  public void testCopyFrom() {
    var table = prepareTable();
    var builder = new TableMetaData.Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    var newTable = builder.build();

    assertEquals(table.getName(), newTable.getName());
    assertEquals(table.getIdentity(), newTable.getIdentity());
    assertEquals(table.getType(), newTable.getType());
    assertEquals(table.getVersion(), newTable.getVersion());
    assertEquals(table.getNumberOfColumns(), newTable.getNumberOfColumns());
    assertEquals(getColumnNames(table), getColumnNames(newTable));
  }
}
