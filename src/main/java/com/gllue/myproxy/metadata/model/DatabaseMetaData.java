package com.gllue.myproxy.metadata.model;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.gllue.myproxy.metadata.AbstractMetaData;
import com.gllue.myproxy.metadata.AbstractMetaDataBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
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

  public int getNumberOfTables() {
    return tableIdMap.size();
  }

  public Iterable<String> getTableNames() {
    return tableNameMap.keySet();
  }

  public synchronized boolean addTable(final TableMetaData table, final boolean autoUpdate) {
    var previous = tableIdMap.putIfAbsent(table.getIdentity(), table);
    if (previous != null) {
      if (!autoUpdate || previous.getVersion() >= table.getVersion()) {
        return false;
      }
      tableIdMap.put(table.getIdentity(), table);
    }
    tableNameMap.put(table.getName(), table);
    return true;
  }

  public boolean addTable(final TableMetaData table) {
    return addTable(table, false);
  }

  public synchronized TableMetaData removeTable(final String identity) {
    var table = tableIdMap.remove(identity);
    if (table == null) {
      return null;
    }
    tableNameMap.remove(table.getName());
    return table;
  }

  @Override
  public void writeTo(StreamOutput output) {
    super.writeTo(output);

    output.writeStringNul(datasource);
    output.writeStringNul(name);
  }

  @Accessors(chain = true)
  public static class Builder extends AbstractMetaDataBuilder<DatabaseMetaData> {
    @Setter private String datasource;
    @Setter private String name;
    private List<TableMetaData> tables = new ArrayList<>();

    public Builder addTable(final TableMetaData table) {
      this.tables.add(table);
      return this;
    }

    public Builder copyTables(DatabaseMetaData metadata) {
      for (var tableName : metadata.getTableNames()) {
        this.tables.add(metadata.getTable(tableName));
      }
      return this;
    }

    @Override
    public void readStream(StreamInput input) {
      super.readStream(input);
      datasource = input.readStringNul();
      name = input.readStringNul();
    }

    @Override
    public void copyFrom(DatabaseMetaData metadata, CopyOptions options) {
      super.copyFrom(metadata, options);
      this.name = metadata.getName();
      this.datasource = metadata.getDatasource();
      if (options == CopyOptions.COPY_CHILDREN) {
        this.tables = new ArrayList<>(metadata.getNumberOfTables());
        for (var tableName : metadata.getTableNames()) {
          this.tables.add(metadata.getTable(tableName));
        }
      }
    }

    @Override
    public DatabaseMetaData build() {
      return new DatabaseMetaData(datasource, name, tables.toArray(new TableMetaData[0]), version);
    }
  }
}
