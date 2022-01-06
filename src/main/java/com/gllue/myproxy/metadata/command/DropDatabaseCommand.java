package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DropDatabaseCommand extends SchemaRelatedMetaDataCommand {
  private final String datasource;
  private final String name;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, name);
    if (database == null) {
      if (log.isWarnEnabled()) {
        log.warn("Database [{}] does not exists.", name);
      }
      return;
    }

    refreshAndDropDatabase(context, database);
  }
}
