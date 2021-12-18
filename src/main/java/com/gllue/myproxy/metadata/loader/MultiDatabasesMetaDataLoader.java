package com.gllue.myproxy.metadata.loader;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.repository.PersistRepository;

public class MultiDatabasesMetaDataLoader {
  private static final String PATH_SEPARATOR = ServerConstants.PATH_SEPARATOR;
  private final PersistRepository repository;

  public MultiDatabasesMetaDataLoader(final PersistRepository repository) {
    this.repository = repository;
  }

  public MultiDatabasesMetaData load(final String basePath) {
    var builder = new MultiDatabasesMetaData.Builder();
    var children = repository.getChildrenKeys(basePath);
    for (var child : children) {
      builder.addDatabase(buildDatabaseMetaData(basePath, child));
    }
    return builder.build();
  }

  private String concatPath(final String path1, final String path2) {
    return String.join(PATH_SEPARATOR, path1, path2);
  }

  private DatabaseMetaData buildDatabaseMetaData(String path, final String dbName) {
    path = concatPath(path, dbName);
    var data = repository.get(path);
    var builder = new DatabaseMetaData.Builder();
    builder.readStream(ByteArrayStreamInput.wrap(data));
    var children = repository.getChildrenKeys(path);
    for (var child : children) {
      builder.addTable(buildTableMetaData(path, child));
    }
    return builder.build();
  }

  private TableMetaData buildTableMetaData(String path, final String tableName) {
    path = concatPath(path, tableName);
    var data = repository.get(path);
    var builder = new TableMetaData.Builder();
    builder.readStream(ByteArrayStreamInput.wrap(data));
    return builder.build();
  }
}
