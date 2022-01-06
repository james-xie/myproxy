package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.metadata.model.TableMetaData.Builder;
import com.google.common.base.Preconditions;

public class CreateTableCommand extends AbstractTableUpdateCommand {

  private final String datasource;
  private final String databaseName;
  private final String name;
  private final Column[] columns;

  public CreateTableCommand(
      String datasource, String databaseName, String name, Column[] columns) {
    this.datasource = datasource;
    this.databaseName = databaseName;
    this.name = name;
    this.columns = columns;
    validateColumnNames(columns);
  }

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();

    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);
    Preconditions.checkArgument(!database.hasTable(name), "Table already exists. [%s]", name);

    var builder = new Builder();
    builder.setName(name).setType(TableType.STANDARD);

    for (var column : columns) {
      var colBuilder = new ColumnMetaData.Builder();
      colBuilder
          .setName(column.name)
          .setType(column.type)
          .setNullable(column.nullable)
          .setDefaultValue(column.defaultValue);
      builder.addColumn(colBuilder.build());
    }

    builder.setIdentity(RandomUtils.randomShortUUID());
    var newDatabase = createTable(database, builder.build());
    refreshAndSaveDatabase(context, newDatabase);
  }
}
