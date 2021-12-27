package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData.Builder;
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

    var builder = new Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(table.getVersion());
    builder.removeColumn(name);

    var newTable = builder.build();
    database.addTable(newTable, true);

    var path = getPersistPathForMetaData(context, database, table);
    saveMetaData(context, path, newTable);
  }
}