package com.gllue.myproxy.metadata.command;

import static com.gllue.myproxy.common.util.PathUtils.joinPaths;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.TestHelper;
import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.command.context.MultiDatabasesCommandContext;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import org.mockito.Mock;

public abstract class BaseCommandTest {
  static final String DATASOURCE = "ds";
  static final String ROOT_PATH = "/root";

  @Mock MultiDatabasesMetaData rootMetaData;

  @Mock PersistRepository repository;

  @Mock Configurations configurations;

  CommandExecutionContext<MultiDatabasesMetaData> buildContext() {
    return new MultiDatabasesCommandContext(rootMetaData, repository, configurations);
  }

  void mockConfigurations() {
    when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn(ROOT_PATH);
  }

  TableMetaData prepareTable(String name) {
    var builder = new TableMetaData.Builder();
    builder
        .setName(name)
        .setType(TableType.PRIMARY)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);
    builder.addColumn(
        new ColumnMetaData.Builder().setName("col1").setType(ColumnType.VARCHAR).build());
    builder.addColumn(new ColumnMetaData.Builder().setName("col2").setType(ColumnType.INT).build());
    return builder.build();
  }

  DatabaseMetaData prepareDatabase(String name) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName(name);
    return builder.build();
  }

  DatabaseMetaData bytesToDatabaseMetaData(byte[] bytes) {
    var input = new ByteArrayStreamInput(bytes);
    var builder = new DatabaseMetaData.Builder();
    builder.readStream(input);
    return builder.build();
  }

  TableMetaData bytesToTableMetaData(byte[] bytes) {
    return TestHelper.bytesToTableMetaData(bytes);
  }

  PartitionTableMetaData bytesToPartitionTableMetaData(byte[] bytes) {
    return TestHelper.bytesToPartitionTableMetaData(bytes);
  }

  String getPersistPath(String... paths) {
    var newPaths = new String[paths.length + 2];
    newPaths[0] = ROOT_PATH;
    newPaths[1] = ServerConstants.DATABASES_ROOT_PATH;
    System.arraycopy(paths, 0, newPaths, 2, paths.length);
    return joinPaths(newPaths);
  }

  String dbKey(String dbName) {
    return DatabaseMetaData.joinDatasourceAndName(DATASOURCE, dbName);
  }
}