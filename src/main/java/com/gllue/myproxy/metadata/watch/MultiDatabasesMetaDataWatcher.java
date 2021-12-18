package com.gllue.myproxy.metadata.watch;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.repository.ClusterPersistRepository;
import com.gllue.myproxy.repository.DataChangedEvent;
import com.gllue.myproxy.repository.DataChangedEvent.Type;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MultiDatabasesMetaDataWatcher {
  private final String basePath;
  private final MultiDatabasesMetaData metaData;
  private final ClusterPersistRepository repository;

  public void watch() {
    repository.watch(basePath, this::dispatch);
  }

  private void dispatch(final DataChangedEvent event) {
    var path = event.getKey();
    var items = splitEventPath(path);
    if (items.size() == 1) {
      watchForDatabaseMetaDataChanged(items.get(0), event.getType(), (byte[]) event.getValue());
    } else if (items.size() == 2) {
      watchForTableMetaDataChanged(
          items.get(0), items.get(1), event.getType(), (byte[]) event.getValue());
    } else {
      throw new IllegalStateException(
          String.format("Got an invalid data change event. [path:%s]", path));
    }
  }

  private List<String> splitEventPath(final String path) {
    if (!path.startsWith(basePath)) {
      throw new IllegalStateException(
          String.format("Got an invalid data change event. [path:%s]", path));
    }

    var subPath = path.substring(basePath.length());
    return Arrays.stream(subPath.split(ServerConstants.PATH_SEPARATOR))
        .filter((x) -> !x.isEmpty())
        .collect(Collectors.toList());
  }

  private void watchForDatabaseMetaDataChanged(
      final String dbKey, final Type eventType, final byte[] data) {
    var items = DatabaseMetaData.splitJoinedDatasourceAndName(dbKey);
    var datasource = items[0];
    var dbName = items[1];
    switch (eventType) {
      case CREATED:
        {
          var builder = new DatabaseMetaData.Builder();
          builder.readStream(ByteArrayStreamInput.wrap(data));
          metaData.addDatabase(builder.build(), false);
          break;
        }
      case UPDATED:
        {
          var builder = new DatabaseMetaData.Builder();
          builder.readStream(ByteArrayStreamInput.wrap(data));
          var previous = metaData.getDatabase(datasource, dbName);
          if (previous != null) {
            builder.copyTables(previous);
          }
          metaData.addDatabase(builder.build(), true);
          break;
        }
      case DELETED:
        {
          metaData.removeDatabase(datasource, dbName);
          break;
        }
    }
  }

  private void watchForTableMetaDataChanged(
      final String dbKey, final String tableId, final Type eventType, final byte[] data) {
    var items = DatabaseMetaData.splitJoinedDatasourceAndName(dbKey);
    var datasource = items[0];
    var dbName = items[1];
    var database = metaData.getDatabase(datasource, dbName);
    if (database == null) {
      throw new IllegalStateException(
          String.format("Cannot find the database by name. [%s]", dbName));
    }

    switch (eventType) {
      case CREATED:
        {
          var builder = new TableMetaData.Builder();
          builder.readStream(ByteArrayStreamInput.wrap(data));
          database.addTable(builder.build(), false);
          break;
        }
      case UPDATED:
        {
          var builder = new TableMetaData.Builder();
          builder.readStream(ByteArrayStreamInput.wrap(data));
          database.addTable(builder.build(), true);
          break;
        }
      case DELETED:
        {
          database.removeTable(tableId);
          break;
        }
    }
  }
}
