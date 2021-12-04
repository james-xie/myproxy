package com.gllue.metadata.command;

import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.units.qual.C;

public abstract class AbstractTableUpdateCommand
    extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  @AllArgsConstructor
  public static class Column {
    public String name;
    public ColumnType type;
    public boolean nullable;
    public String defaultValue;
    public boolean builtin;

    public Column(
        final String name,
        final ColumnType type,
        final boolean nullable,
        final String defaultValue) {
      this(name, type, nullable, defaultValue, false);
    }

    public static Column newColumn(final ColumnMetaData column) {
      return new Column(
          column.getName(),
          column.getType(),
          column.isNullable(),
          column.getDefaultValue(),
          column.isBuiltin());
    }
  }

  protected void validateColumnNames(Column[] columns) {
    Preconditions.checkArgument(columns.length > 0, "Columns cannot be an empty array.");

    var nameSet = new HashSet<String>();
    for (var column : columns) {
      Preconditions.checkArgument(
          !nameSet.contains(column.name), "Has duplicate column name. [%s]", column.name);
      nameSet.add(column.name);
    }
  }
}
