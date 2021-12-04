package com.gllue.metadata.command;

import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
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

    var builder = new TableMetaData.Builder();
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
    var path = getPersistPathForMetaData(context, database, newTable);
    saveMetaData(context, path, newTable);
  }
}
