package com.gllue.myproxy.metadata.watch;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.codec.DatabaseMetaDataCodec;
import com.gllue.myproxy.metadata.codec.MetaDataCodec;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
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
    } else if (items.size() > 1) {
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

  private MetaDataCodec<DatabaseMetaData> getCodec() {
    return DatabaseMetaDataCodec.getInstance();
  }

  private void watchForDatabaseMetaDataChanged(
      final String dbKey, final Type eventType, final byte[] data) {
    var items = DatabaseMetaData.splitJoinedDatasourceAndName(dbKey);
    var datasource = items[0];
    var dbName = items[1];
    var codec = getCodec();
    var stream = ByteArrayStreamInput.wrap(data);
    switch (eventType) {
      case CREATED:
        metaData.addDatabase(codec.decode(stream), false);
        break;
      case UPDATED:
        metaData.addDatabase(codec.decode(stream), true);
        break;
      case DELETED:
        metaData.removeDatabase(datasource, dbName);
        break;
    }
  }
}
