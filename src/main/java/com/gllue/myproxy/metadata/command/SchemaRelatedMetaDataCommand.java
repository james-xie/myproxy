package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.myproxy.metadata.codec.DatabaseMetaDataCodec;
import com.gllue.myproxy.metadata.codec.MetaDataCodec;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;

public abstract class SchemaRelatedMetaDataCommand
    extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  DatabaseMetaData.Builder newDatabaseBuilder(DatabaseMetaData database) {
    var builder = new DatabaseMetaData.Builder();
    builder.copyFrom(database, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(database.getVersion());
    return builder;
  }

  DatabaseMetaData createTable(DatabaseMetaData database, TableMetaData table) {
    var builder = newDatabaseBuilder(database);
    builder.addTable(table);
    return builder.build();
  }

  DatabaseMetaData updateTable(DatabaseMetaData database, TableMetaData table) {
    var builder = newDatabaseBuilder(database);
    builder.removeTable(table.getIdentity());
    builder.addTable(table);
    return builder.build();
  }

  DatabaseMetaData dropTable(DatabaseMetaData database, String tableIdentity) {
    var builder = newDatabaseBuilder(database);
    builder.removeTable(tableIdentity);
    return builder.build();
  }

  void refreshDatabase(
      CommandExecutionContext<MultiDatabasesMetaData> context, DatabaseMetaData database) {
    var metadata = context.getRootMetaData();
    var refreshed = metadata.addDatabase(database, true);
    if (!refreshed) {
      throw new IllegalStateException(
          String.format("Failed to refresh database metadata. [%s]", database.getName()));
    }
  }

  MetaDataCodec<DatabaseMetaData> getCodec() {
    return DatabaseMetaDataCodec.getInstance();
  }

  void saveDatabase(
      CommandExecutionContext<MultiDatabasesMetaData> context, DatabaseMetaData database) {
    var path = getPersistPathForMetaData(context, database);
    var stream = new ByteArrayStreamOutput();
    getCodec().encode(stream, database);
    saveMetaData(context, path, stream.getTrimmedByteArray());
  }

  void refreshAndSaveDatabase(
      CommandExecutionContext<MultiDatabasesMetaData> context, DatabaseMetaData database) {
    refreshDatabase(context, database);
    saveDatabase(context, database);
  }

  void dropDatabase(
      CommandExecutionContext<MultiDatabasesMetaData> context, DatabaseMetaData database) {
    var path = getPersistPathForMetaData(context, database);
    deleteMetaData(context, path);
  }

  void refreshAndDropDatabase(
      CommandExecutionContext<MultiDatabasesMetaData> context, DatabaseMetaData database) {
    var metadata = context.getRootMetaData();
    metadata.removeDatabase(database.getDatasource(), database.getName());
    dropDatabase(context, database);
  }
}
