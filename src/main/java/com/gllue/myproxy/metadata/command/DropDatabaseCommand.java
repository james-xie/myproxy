package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
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

    metadata.removeDatabase(datasource, name);

    var path = getPersistPathForMetaData(context, database);
    deleteMetaData(context, path);
  }
}
