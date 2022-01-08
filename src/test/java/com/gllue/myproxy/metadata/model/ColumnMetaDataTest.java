package com.gllue.myproxy.metadata.model;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ColumnMetaDataTest {
  ColumnMetaData prepareColumn() {
    var builder = new ColumnMetaData.Builder();
    var name = "col1";
    var type = ColumnType.VARCHAR;
    var defaultValue = "";
    var nullable = false;
    builder.setName(name);
    builder.setType(type);
    builder.setDefaultValue(defaultValue);
    builder.setNullable(nullable);
    return builder.build();
  }

  @Test
  public void testBuild() {
    var builder = new ColumnMetaData.Builder();
    var name = "col1";
    var type = ColumnType.VARCHAR;
    var defaultValue = "";
    var nullable = false;
    builder.setName(name);
    builder.setType(type);
    builder.setDefaultValue(defaultValue);
    builder.setNullable(nullable);
    var column = builder.build();

    assertEquals(name, column.getName());
    assertEquals(type, column.getType());
    assertEquals(defaultValue, column.getDefaultValue());
    assertEquals(nullable, column.isNullable());
  }

  @Test
  public void testCopyFrom() {
    var column = prepareColumn();
    var builder = new ColumnMetaData.Builder();
    builder.copyFrom(column, CopyOptions.DEFAULT);
    var newColumn = builder.build();

    assertEquals(column.getIdentity(), newColumn.getIdentity());
    assertEquals(column.getVersion(), newColumn.getVersion());
    assertEquals(column.getName(), newColumn.getName());
    assertEquals(column.getType(), newColumn.getType());
    assertEquals(column.getDefaultValue(), newColumn.getDefaultValue());
    assertEquals(column.isNullable(), newColumn.isNullable());
  }
}
