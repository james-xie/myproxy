package com.gllue.myproxy.metadata.model;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.gllue.myproxy.metadata.AbstractMetaData;
import com.gllue.myproxy.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class DatabaseMetaData extends AbstractMetaData {
  @Getter private final String datasource;
  @Getter private final String name;
  private final Map<String, TableMetaData> tableNameMap;
  private final Map<String, TableMetaData> tableIdMap;

  public DatabaseMetaData(
      final String datasource, final String name, final TableMetaData[] tables, final int version) {
    super(joinDatasourceAndName(datasource, name), version);

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(datasource), "Datasource cannot be null or empty.");

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(name), "Database name cannot be null or empty.");

    this.datasource = datasource;
    this.name = name;
    this.tableNameMap = new ConcurrentHashMap<>(tables.length);
    this.tableIdMap = new ConcurrentHashMap<>(tables.length);

    for (TableMetaData table : tables) {
      var old = tableIdMap.put(table.getIdentity(), table);
      if (old != null) {
        throw new IllegalArgumentException(
            String.format("Table identity [%s] in tables must be unique.", table.getIdentity()));
      }

      old = tableNameMap.put(table.getName(), table);
      if (old != null) {
        throw new IllegalArgumentException(
            String.format("Table name [%s] in tables must be unique.", table.getName()));
      }
    }
  }

  public static String joinDatasourceAndName(final String datasource, final String name) {
    return String.format("%s#%s", datasource, name);
  }

  public static String[] splitJoinedDatasourceAndName(final String value) {
    var items = value.split("#", 2);
    if (items.length == 2) {
      return items;
    }
    throw new IllegalArgumentException(String.format("Invalid argument. [%s]", value));
  }

  public boolean hasTable(final String name) {
    return tableNameMap.containsKey(name);
  }

  public TableMetaData getTable(final String name) {
    return tableNameMap.get(name);
  }

  public TableMetaData getTableById(final String id) {
    return tableIdMap.get(id);
  }

  public Iterable<String> getTableNames() {
    return tableNameMap.keySet();
  }

  public Iterable<TableMetaData> getTables() {
    return tableIdMap.values();
  }

  public int getNumberOfTables() {
    return tableIdMap.size();
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<DatabaseMetaData> {
    @Setter private String datasource;
    @Setter private String name;
    private final Map<String, TableMetaData> tableMap = new HashMap<>();

    public Builder setVersion(final int version) {
      this.version = version;
      return this;
    }

    public Builder setNextVersion(final int version) {
      this.version = version + 1;
      return this;
    }

    public Builder addTable(final TableMetaData table) {
      var old = tableMap.put(table.getIdentity(), table);
      if (old != null) {
        throw new IllegalArgumentException(
            String.format("TableMetaData [%s] has already exists.", table.getName()));
      }
      return this;
    }

    public Builder removeTable(final String tableIdentity) {
      tableMap.remove(tableIdentity);
      return this;
    }

    public Builder copyTables(DatabaseMetaData metadata) {
      for (var table : metadata.getTables()) {
        this.addTable(table);
      }
      return this;
    }

    @Override
    public void copyFrom(DatabaseMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.datasource = metadata.getDatasource();
      if (options == CopyOptions.COPY_CHILDREN) {
        copyTables(metadata);
      }
    }

    @Override
    public DatabaseMetaData build() {
      var tables = new TableMetaData[tableMap.size()];
      int index = 0;
      for (var table : tableMap.values()) {
        tables[index++] = table;
      }
      return new DatabaseMetaData(datasource, name, tables, version);
    }
  }
}
