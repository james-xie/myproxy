package com.gllue.metadata.command;

import com.gllue.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RenameTableCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String databaseName;
  private final String oldName;
  private final String newName;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    Preconditions.checkArgument(
        oldName != null && newName != null && !oldName.equals(newName),
        "Bad table name. [old:%s, new:%s]",
        oldName,
        newName);

    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTable(oldName);
    Preconditions.checkArgument(table != null, "Unknown table name. [%s]", oldName);
    Preconditions.checkArgument(!database.hasTable(newName), "Table already exists. [%s]", newName);

    var builder = new TableMetaData.Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(table.getVersion());
    builder.setName(newName);

    var newTable = builder.build();
    database.addTable(newTable, true);

    var path = getPersistPathForMetaData(context, database, table);
    saveMetaData(context, path, newTable);
  }
}
