package com.gllue.metadata.command;

import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DropDatabaseCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String name;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, name);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", name);

    var path = getPersistPathForMetaData(context, database);
    deleteMetaData(context, path);
  }
}
