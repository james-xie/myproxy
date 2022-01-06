package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.metadata.model.TableMetaData.Builder;
import com.google.common.base.Preconditions;

public class UpdateTableCommand extends AbstractTableUpdateCommand {

  private final String datasource;
  private final String databaseName;
  private final String identity;
  private final String name;
  private final Column[] columns;

  public UpdateTableCommand(
      String datasource, String databaseName, String identity, String name, Column[] columns) {
    this.datasource = datasource;
    this.databaseName = databaseName;
    this.identity = identity;
    this.name = name;
    this.columns = columns;
    validateColumnNames(columns);
  }

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();

    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTableById(identity);
    Preconditions.checkArgument(table != null, "Table does not exists. [id: %s]", identity);

    var builder = new Builder();
    builder
        .setName(name)
        .setIdentity(identity)
        .setType(TableType.STANDARD)
        .setNextVersion(table.getVersion());

    for (var column : columns) {
      var colBuilder = new ColumnMetaData.Builder();
      colBuilder
          .setName(column.name)
          .setType(column.type)
          .setNullable(column.nullable)
          .setDefaultValue(column.defaultValue);
      builder.addColumn(colBuilder.build());
    }

    var newTable = builder.build();
    var newDatabase = updateTable(database, newTable);
    refreshAndSaveDatabase(context, newDatabase);
  }
}
