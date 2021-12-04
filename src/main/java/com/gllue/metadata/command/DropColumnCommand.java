package com.gllue.metadata.command;

import com.gllue.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DropColumnCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String databaseName;
  private final String tableName;
  private final String name;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTable(tableName);
    Preconditions.checkArgument(table != null, "Unknown table name. [%s]", tableName);
    Preconditions.checkArgument(table.hasColumn(name), "Unknown column name. [%s]", name);

    var builder = new TableMetaData.Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(table.getVersion());
    builder.removeColumn(name);

    var path = getPersistPathForMetaData(context, database, table);
    saveMetaData(context, path, builder.build());
  }
}
