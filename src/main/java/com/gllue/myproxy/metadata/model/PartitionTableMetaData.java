package com.gllue.myproxy.metadata.model;

import com.gllue.myproxy.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class PartitionTableMetaData extends TableMetaData {
  @Getter private final TableMetaData primaryTable;
  @Getter private final TableMetaData[] extensionTables;

  public PartitionTableMetaData(
      final String identity,
      final String name,
      final int version,
      final TableMetaData primaryTable,
      final TableMetaData[] extensionTables) {
    super(
        identity,
        name,
        TableType.PARTITION,
        mergeColumns(primaryTable, extensionTables),
        version,
        false);
    this.primaryTable = primaryTable;
    this.extensionTables = extensionTables;
  }

  private static ColumnMetaData[] mergeColumns(
      TableMetaData primaryTable, TableMetaData[] extensionTables) {
    List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 0; i < primaryTable.getNumberOfColumns(); i++) {
      var col = primaryTable.getColumn(i);
      if (col.isBuiltin()) {
        continue;
      }
      columns.add(col);
    }
    for (var extensionTable : extensionTables) {
      for (int i = 0; i < extensionTable.getNumberOfColumns(); i++) {
        var col = extensionTable.getColumn(i);
        if (col.isBuiltin()) {
          continue;
        }
        columns.add(col);
      }
    }
    return columns.toArray(ColumnMetaData[]::new);
  }

  public int getNumberOfTables() {
    return extensionTables.length + 1;
  }

  public int getNumberOfExtensionTables() {
    return extensionTables.length;
  }

  public int freeExtensionColumns(final int maxColumnsPerTable) {
    int freeColumns = 0;
    for (var table : extensionTables) {
      freeColumns += maxColumnsPerTable - table.getNumberOfColumns();
    }
    return freeColumns;
  }

  public boolean hasExtensionColumn(final String columnName) {
    for (var table : extensionTables) {
      if (table.hasColumn(columnName)) {
        return true;
      }
    }
    return false;
  }

  public TableMetaData getTableByOrdinalValue(final int ordinalValue) {
    Preconditions.checkArgument(
        ordinalValue >= 0 && ordinalValue <= extensionTables.length,
        "Illegal ordinal value. [%d]",
        ordinalValue);

    if (ordinalValue == 0) {
      return primaryTable;
    }
    return extensionTables[ordinalValue - 1];
  }

  public int nextOrdinalValue() {
    return extensionTables.length + 1;
  }

  public int getOrdinalValueByColumnName(final String columnName) {
    int ordinalValue = 0;
    if (primaryTable.hasColumn(columnName)) {
      return ordinalValue;
    }

    ordinalValue++;
    for (var table : extensionTables) {
      if (table.hasColumn(columnName)) {
        return ordinalValue;
      }
      ordinalValue++;
    }
    return -1;
  }

  public String[] getTableNames() {
    var tableNames = new String[extensionTables.length + 1];
    tableNames[0] = primaryTable.getName();
    for (int i = 1; i <= extensionTables.length; i++) {
      tableNames[i] = extensionTables[i - 1].getName();
    }
    return tableNames;
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<PartitionTableMetaData> {
    @Setter private String name;
    @Setter private TableMetaData primaryTable;
    private final List<TableMetaData> extensionTables = new ArrayList<>();

    public Builder setIdentity(final String identity) {
      this.identity = identity;
      return this;
    }

    public Builder setVersion(final int version) {
      this.version = version;
      return this;
    }

    public Builder setNextVersion(final int version) {
      this.version = version + 1;
      return this;
    }

    public Builder addExtensionTable(TableMetaData table) {
      assert table.getType() == TableType.EXTENSION;
      this.extensionTables.add(table);
      return this;
    }


    @Override
    public void copyFrom(PartitionTableMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.primaryTable = metadata.primaryTable;
      this.extensionTables.addAll(Arrays.asList(metadata.extensionTables));
    }

    @Override
    public PartitionTableMetaData build() {
      return new PartitionTableMetaData(
          identity, name, version, primaryTable, extensionTables.toArray(new TableMetaData[0]));
    }
  }
}
