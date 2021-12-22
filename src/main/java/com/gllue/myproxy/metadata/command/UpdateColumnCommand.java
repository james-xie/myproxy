package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData.Builder;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UpdateColumnCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String databaseName;
  private final String tableName;
  private final String oldName;
  private final String newName;
  private final ColumnType type;
  private final boolean nullable;
  private final String defaultValue;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTable(tableName);
    Preconditions.checkArgument(table != null, "Unknown table name. [%s]", tableName);
    Preconditions.checkArgument(
        table.hasColumn(oldName), "Unknown column to be updated. [%s]", oldName);
    Preconditions.checkArgument(
        oldName.equals(newName) || !table.hasColumn(newName),
        "New columns name already exists. [%s]",
        newName);

    var builder = new Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(table.getVersion());

    var colBuilder = new ColumnMetaData.Builder();
    colBuilder.setName(newName).setType(type).setNullable(nullable).setDefaultValue(defaultValue);
    builder.addColumn(colBuilder.build());
    builder.removeColumn(oldName);

    var newTable = builder.build();
    database.addTable(newTable, true);

    var path = getPersistPathForMetaData(context, database, table);
    saveMetaData(context, path, newTable);
  }
}
