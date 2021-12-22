package com.gllue.metadata.command;

import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.DatabaseMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateDatabaseCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String name;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    Preconditions.checkArgument(
        !metadata.hasDatabase(datasource, name), "Database already exists. [%s]", name);

    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(datasource);
    builder.setName(name);
    var database = builder.build();
    var path = getPersistPathForMetaData(context, database);
    metadata.addDatabase(database, false);
    saveMetaData(context, path, database);

    log.info("Create database. [{}]", name);
  }
}
