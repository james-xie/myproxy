package com.gllue.myproxy.metadata.model;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableMetaDataTest {
  TableMetaData prepareTable() {
    var builder = new TableMetaData.Builder();
    var name = "table1";
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("col1").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col3").setType(ColumnType.FLOAT).build());
    return builder.build();
  }

  List<String> getColumnNames(TableMetaData table) {
    var colNames = new ArrayList<String>();
    for (int i=0; i<table.getNumberOfColumns(); i++) {
      colNames.add(table.getColumn(i).getName());
    }
    return colNames;
  }

  @Test
  public void testBuild() {
    var builder = new TableMetaData.Builder();
    var name = "table1";
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("col1").setType(ColumnType.INT).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col2").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col3").setType(ColumnType.FLOAT).build());
    var table = builder.build();

    assertEquals(name, table.getName());
    assertEquals(name, table.getIdentity());
    assertEquals(TableType.PRIMARY, table.getType());
    assertEquals(1, table.getVersion());
    assertEquals(3, table.getNumberOfColumns());

    assertEquals(List.of("col1", "col2", "col3"), getColumnNames(table));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildWithDuplicateColumn() {
    var builder = new TableMetaData.Builder();
    var name = "table1";
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(new ColumnMetaData.Builder().setName("col1").setType(ColumnType.INT).build());
    builder.addColumn(new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.build();
  }

  @Test
  public void testReadAndWriteStream() {
    var table = prepareTable();
    var output = new ByteArrayStreamOutput();
    table.writeTo(output);
    var input = new ByteArrayStreamInput(output.getTrimmedByteArray());
    var builder = new TableMetaData.Builder();
    builder.readStream(input);
    var newTable = builder.build();

    assertEquals(table.getName(), newTable.getName());
    assertEquals(table.getIdentity(), newTable.getIdentity());
    assertEquals(table.getType(), newTable.getType());
    assertEquals(table.getVersion(), newTable.getVersion());
    assertEquals(table.getNumberOfColumns(), newTable.getNumberOfColumns());
    assertEquals(getColumnNames(table), getColumnNames(newTable));
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
