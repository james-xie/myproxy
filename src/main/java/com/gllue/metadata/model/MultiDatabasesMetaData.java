package com.gllue.metadata.model;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.common.io.stream.StreamOutput;
import com.gllue.metadata.MetaData;
import com.gllue.metadata.MetaDataBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.experimental.Accessors;

public class MultiDatabasesMetaData implements MetaData {
  private static final String IDENTITY = "ID";
  private static final int VERSION = -1;

  private final Map<String, Map<String, DatabaseMetaData>> datasourceMap;

  public MultiDatabasesMetaData(final DatabaseMetaData[] databases) {
    this.datasourceMap = new ConcurrentHashMap<>(databases.length);

    for (DatabaseMetaData database : databases) {
      var dbMap =
          datasourceMap.computeIfAbsent(database.getDatasource(), k -> new ConcurrentHashMap<>());
      var old = dbMap.put(database.getName(), database);
      if (old != null) {
        throw new IllegalArgumentException(
            String.format("Database name [%s] in databases must be unique.", database.getName()));
      }
    }
  }

  public int getNumberOfDatabases() {
    return datasourceMap.size();
  }

  public boolean hasDatabase(final String datasource, final String name) {
    if (datasourceMap.containsKey(datasource)) {
      return datasourceMap.containsKey(name);
    }
    return false;
  }

  public DatabaseMetaData getDatabase(final String datasource, final String name) {
    var dbMap = datasourceMap.get(datasource);
    if (dbMap == null) {
      return null;
    }
    return dbMap.get(name);
  }

  public synchronized boolean addDatabase(
      final DatabaseMetaData database, final boolean autoUpdate) {
    var dbMap =
        datasourceMap.computeIfAbsent(database.getDatasource(), k -> new ConcurrentHashMap<>());
    var previous = dbMap.putIfAbsent(database.getName(), database);
    if (previous != null) {
      if (!autoUpdate || previous.getVersion() >= database.getVersion()) {
        return false;
      }
      dbMap.put(database.getName(), database);
    }
    return true;
  }

  public synchronized DatabaseMetaData removeDatabase(final String datasource, final String name) {
    var dbMap = datasourceMap.get(datasource);
    if (dbMap == null) {
      return null;
    }
    return dbMap.remove(name);
  }

  public Iterable<String> getDatabaseNames(final String datasource) {
    return datasourceMap.getOrDefault(datasource, Collections.emptyMap()).keySet();
  }

  public Iterable<DatabaseMetaData> getAllDatabases() {
    List<DatabaseMetaData> databases = new ArrayList<>();
    for (var dbMap : datasourceMap.values()) {
      databases.addAll(dbMap.values());
    }
    return databases;
  }

  @Override
  public String getIdentity() {
    return IDENTITY;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public void writeTo(StreamOutput output) {}

  @Accessors(chain = true)
  public static class Builder implements MetaDataBuilder<MultiDatabasesMetaData> {
    private List<DatabaseMetaData> databases = new ArrayList<>();

    public Builder addDatabase(final DatabaseMetaData database) {
      this.databases.add(database);
      return this;
    }

    @Override
    public void readStream(StreamInput input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copyFrom(MultiDatabasesMetaData metadata, CopyOptions options) {
      if (options == CopyOptions.COPY_CHILDREN) {
        this.databases = new ArrayList<>(metadata.getNumberOfDatabases());
        for (var database : metadata.getAllDatabases()) {
          this.databases.add(database);
        }
      }
    }

    @Override
    public MultiDatabasesMetaData build() {
      return new MultiDatabasesMetaData(databases.toArray(new DatabaseMetaData[0]));
    }
  }
}
