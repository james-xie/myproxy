package com.gllue.metadata.command;

import com.gllue.common.util.RandomUtils;
import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import com.google.common.base.Preconditions;

public class CreateTableCommand extends AbstractTableUpdateCommand {

  private final String datasource;
  private final String databaseName;
  private final String name;
  private final TableType type;
  private final Column[] columns;

  public CreateTableCommand(
      String datasource, String databaseName, String name, TableType type, Column[] columns) {
    this.datasource = datasource;
    this.databaseName = databaseName;
    this.name = name;
    this.type = type;
    this.columns = columns;
    validateColumnNames(columns);
  }

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();

    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);
    Preconditions.checkArgument(!database.hasTable(name), "Table already exists. [%s]", name);

    var builder = new TableMetaData.Builder();
    builder.setName(name).setType(type);

    for (var column : columns) {
      var colBuilder = new ColumnMetaData.Builder();
      colBuilder
          .setName(column.name)
          .setType(column.type)
          .setNullable(column.nullable)
          .setDefaultValue(column.defaultValue);
      builder.addColumn(colBuilder.build());
    }

    do {
      builder.setIdentity(RandomUtils.randomShortUUID());
      var table = builder.build();
      var path = getPersistPathForMetaData(context, database, table);
      if (context.getRepository().exists(path)) {
        continue;
      }

      saveMetaData(context, path, table);
    } while (false);
  }
}
