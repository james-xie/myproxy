package com.gllue.metadata.command;

import com.gllue.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.metadata.command.context.CommandExecutionContext;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AddColumnCommand extends AbstractMetaDataCommand<MultiDatabasesMetaData> {
  private final String datasource;
  private final String databaseName;
  private final String tableName;
  private final String name;
  private final ColumnType type;
  private final boolean nullable;
  private final String defaultValue;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    var metadata = context.getRootMetaData();
    var database = metadata.getDatabase(datasource, databaseName);
    Preconditions.checkArgument(database != null, "Unknown database name. [%s]", databaseName);

    var table = database.getTable(this.tableName);
    Preconditions.checkArgument(table != null, "Unknown table name. [%s]", this.tableName);
    Preconditions.checkArgument(!table.hasColumn(name), "Column already exists. [%s]", name);

    var builder = new TableMetaData.Builder();
    builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
    builder.setNextVersion(table.getVersion());

    var colBuilder = new ColumnMetaData.Builder();
    colBuilder.setName(name).setType(type).setNullable(nullable).setDefaultValue(defaultValue);
    builder.addColumn(colBuilder.build());

    var path = getPersistPathForMetaData(context, database, table);
    var newTable = builder.build();
    database.addTable(builder.build(), true);
    saveMetaData(context, path, newTable);
  }
}
