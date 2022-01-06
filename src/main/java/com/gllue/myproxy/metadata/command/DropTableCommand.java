package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DropTableCommand extends SchemaRelatedMetaDataCommand {
  private final String datasource;
  private final String databaseName;
  private final String name;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();

    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTable(name);
    if (table == null) {
      if (log.isWarnEnabled()) {
        log.warn("Table [{}.{}] does not exists.", databaseName, name);
      }
      return;
    }

    var newDatabase = dropTable(database, table.getIdentity());
    refreshAndSaveDatabase(context, newDatabase);
  }
}
