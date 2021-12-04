package com.gllue.metadata.command;

import com.gllue.common.util.RandomUtils;
import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
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
    Preconditions.checkArgument(
        extensionTables.length > 0, "Extension table array cannot be empty.");
    for (var extTable : extensionTables) {
      validateColumnNames(extTable.columns);
    }
  }

  private TableMetaData buildTableMetaData(Table table) {
    var builder = new TableMetaData.Builder();
    builder.setName(table.name).setType(TableType.PRIMARY).setIdentity(table.name);

    for (var column : table.columns) {
      var colBuilder = new ColumnMetaData.Builder();
      colBuilder
          .setName(column.name)
          .setType(column.type)
          .setNullable(column.nullable)
          .setDefaultValue(column.defaultValue);
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
    builder.setName(name).setPrimaryTable(buildTableMetaData(primaryTable));
    for (var extensionTable : extensionTables) {
      builder.addExtensionTable(buildTableMetaData(extensionTable));
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
