package com.gllue.metadata.command;

import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

public class UpdatePartitionTableCommand extends AbstractTableUpdateCommand {

  private final String datasource;
  private final String databaseName;
  private final String identity;
  private final String name;
  private final Table primaryTable;
  private final Table[] extensionTables;

  @RequiredArgsConstructor
  public static class Table {
    public final String name;
    public final Column[] columns;
  }

  public UpdatePartitionTableCommand(
      final String datasource,
      final String databaseName,
      final String identity,
      final String name,
      final Table primaryTable) {
    this(datasource, databaseName, identity, name, primaryTable, null);
  }

  public UpdatePartitionTableCommand(
      final String datasource,
      final String databaseName,
      final String identity,
      final String name,
      final Table primaryTable,
      final Table[] extensionTables) {
    this.datasource = datasource;
    this.databaseName = databaseName;
    this.identity = identity;
    this.name = name;
    this.primaryTable = primaryTable;
    this.extensionTables = extensionTables != null ? extensionTables : new Table[0];

    validateColumnNames(primaryTable.columns);
    for (var extTable : this.extensionTables) {
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

    var table = database.getTableById(identity);
    Preconditions.checkArgument(table != null, "Table does not exists. [id: %s]", identity);

    var builder = new PartitionTableMetaData.Builder();
    builder
        .setName(name)
        .setIdentity(identity)
        .setNextVersion(table.getVersion())
        .setPrimaryTable(buildTableMetaData(primaryTable));

    for (var extensionTable : extensionTables) {
      builder.addExtensionTable(buildTableMetaData(extensionTable));
    }

    var newTable = builder.build();
    var path = getPersistPathForMetaData(context, database, newTable);
    saveMetaData(context, path, newTable);
  }
}
