package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

public class CreatePartitionTableCommand extends AbstractTableUpdateCommand {

  private final String datasource;
  private final String databaseName;
  private final String name;
  private final Table primaryTable;
  private final Table[] extensionTables;

  @RequiredArgsConstructor
  public static class Table {
    private final String name;
    private final Column[] columns;
  }

  public CreatePartitionTableCommand(
      String datasource,
      String databaseName,
      String name,
      Table primaryTable,
      Table[] extensionTables) {
    this.datasource = datasource;
    this.databaseName = databaseName;
    this.name = name;
    this.primaryTable = primaryTable;
    this.extensionTables = extensionTables;

    validateColumnNames(primaryTable.columns);
    for (var extTable : extensionTables) {
      validateColumnNames(extTable.columns);
    }
  }

  private TableMetaData buildTableMetaData(Table table, TableType tableType) {
    var builder = new TableMetaData.Builder();
    builder.setName(table.name).setType(tableType).setIdentity(table.name);

    for (var column : table.columns) {
      var colBuilder = new ColumnMetaData.Builder();
      colBuilder
          .setName(column.name)
          .setType(column.type)
          .setNullable(column.nullable)
          .setDefaultValue(column.defaultValue)
          .setBuiltin(column.builtin);
      builder.addColumn(colBuilder.build());
    }
    return builder.build();
  }

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();

    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);
    Preconditions.checkArgument(!database.hasTable(name), "Table already exists. [%s]", name);

    var builder = new PartitionTableMetaData.Builder();
    builder.setName(name).setPrimaryTable(buildTableMetaData(primaryTable, TableType.PRIMARY));
    for (var extensionTable : extensionTables) {
      builder.addExtensionTable(buildTableMetaData(extensionTable, TableType.EXTENSION));
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
